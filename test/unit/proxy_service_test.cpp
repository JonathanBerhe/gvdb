#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <atomic>
#include <memory>

#include "network/proxy_service.h"
#include "vectordb.grpc.pb.h"
#include "internal.grpc.pb.h"

using namespace gvdb;

// ============================================================================
// Mock Backend Services
// ============================================================================

// Mock Coordinator Service
class MockCoordinatorService : public proto::VectorDBService::Service {
 public:
  std::atomic<int> create_collection_calls{0};
  std::atomic<int> drop_collection_calls{0};
  std::atomic<int> list_collections_calls{0};
  std::atomic<int> get_stats_calls{0};

  grpc::Status CreateCollection(
      grpc::ServerContext* context,
      const proto::CreateCollectionRequest* request,
      proto::CreateCollectionResponse* response) override {
    create_collection_calls++;
    response->set_collection_id(42);
    response->set_message("Collection created");
    return grpc::Status::OK;
  }

  grpc::Status DropCollection(
      grpc::ServerContext* context,
      const proto::DropCollectionRequest* request,
      proto::DropCollectionResponse* response) override {
    drop_collection_calls++;
    response->set_message("Collection dropped");
    return grpc::Status::OK;
  }

  grpc::Status ListCollections(
      grpc::ServerContext* context,
      const proto::ListCollectionsRequest* request,
      proto::ListCollectionsResponse* response) override {
    list_collections_calls++;
    auto* collection = response->add_collections();
    collection->set_collection_id(1);
    collection->set_collection_name("test_collection");
    collection->set_dimension(128);
    collection->set_metric_type("L2");
    return grpc::Status::OK;
  }

  grpc::Status GetStats(
      grpc::ServerContext* context,
      const proto::GetStatsRequest* request,
      proto::GetStatsResponse* response) override {
    get_stats_calls++;
    response->set_total_collections(1);
    response->set_total_vectors(100);
    return grpc::Status::OK;
  }
};

// Mock internal service on the coordinator. The proxy calls RouteQuery
// before dispatching writes/reads to a data node; we answer with a single
// pre-configured data-node address so the test verifies routing actually
// lands on the right backend. Pre-1.7 the tests relied on the --data-nodes
// static-list fallback when RouteQuery was absent; that fallback is gone,
// so the mock must now implement it.
class MockCoordinatorInternalService : public proto::internal::InternalService::Service {
 public:
  std::atomic<int> route_query_calls{0};
  std::string data_node_address;

  grpc::Status RouteQuery(
      grpc::ServerContext* context,
      const proto::internal::RouteQueryRequest* request,
      proto::internal::RouteQueryResponse* response) override {
    route_query_calls++;
    response->add_target_node_addresses(data_node_address);
    return grpc::Status::OK;
  }
};

// Mock Query Node Service
class MockQueryNodeService : public proto::VectorDBService::Service {
 public:
  int node_id;
  std::atomic<int> search_calls{0};

  explicit MockQueryNodeService(int id) : node_id(id) {}

  grpc::Status Search(
      grpc::ServerContext* context,
      const proto::SearchRequest* request,
      proto::SearchResponse* response) override {
    search_calls++;

    // Add a result entry with node ID encoded in the vector ID
    auto* result = response->add_results();
    result->set_id(node_id * 1000);  // Encode node ID
    result->set_distance(0.5f);

    return grpc::Status::OK;
  }
};

// Mock Data Node Service
class MockDataNodeService : public proto::VectorDBService::Service {
 public:
  std::atomic<int> insert_calls{0};
  std::atomic<int> get_calls{0};
  std::atomic<int> delete_calls{0};
  std::atomic<int> update_metadata_calls{0};

  grpc::Status Insert(
      grpc::ServerContext* context,
      const proto::InsertRequest* request,
      proto::InsertResponse* response) override {
    insert_calls++;
    response->set_inserted_count(request->vectors_size());
    return grpc::Status::OK;
  }

  grpc::Status Get(
      grpc::ServerContext* context,
      const proto::GetRequest* request,
      proto::GetResponse* response) override {
    get_calls++;
    auto* vec = response->add_vectors();
    vec->set_id(123);
    vec->mutable_vector()->set_dimension(128);
    return grpc::Status::OK;
  }

  grpc::Status Delete(
      grpc::ServerContext* context,
      const proto::DeleteRequest* request,
      proto::DeleteResponse* response) override {
    delete_calls++;
    response->set_deleted_count(request->ids_size());
    return grpc::Status::OK;
  }

  grpc::Status UpdateMetadata(
      grpc::ServerContext* context,
      const proto::UpdateMetadataRequest* request,
      proto::UpdateMetadataResponse* response) override {
    update_metadata_calls++;
    response->set_message("Metadata updated");
    return grpc::Status::OK;
  }
};

// ============================================================================
// Test Fixture
// ============================================================================

struct ProxyServiceTest {
  ProxyServiceTest() {
    // Create mock services
    mock_coordinator_ = std::make_unique<MockCoordinatorService>();
    mock_coordinator_internal_ = std::make_unique<MockCoordinatorInternalService>();
    mock_query_node_1_ = std::make_unique<MockQueryNodeService>(1);
    mock_query_node_2_ = std::make_unique<MockQueryNodeService>(2);
    mock_data_node_ = std::make_unique<MockDataNodeService>();

    // Data-node first so we can tell the coordinator where to route.
    data_node_address_ = "localhost:50061";
    StartServer(data_node_address_, mock_data_node_.get(), data_node_server_);
    mock_coordinator_internal_->data_node_address = data_node_address_;

    // Coordinator server registers BOTH the public VectorDBService (for
    // metadata ops) AND the internal InternalService (for RouteQuery).
    coordinator_address_ = "localhost:50051";
    {
      grpc::ServerBuilder builder;
      builder.AddListeningPort(coordinator_address_, grpc::InsecureServerCredentials());
      builder.RegisterService(mock_coordinator_.get());
      builder.RegisterService(mock_coordinator_internal_.get());
      coordinator_server_ = builder.BuildAndStart();
    }

    // Start query node servers
    query_node_1_address_ = "localhost:50071";
    query_node_2_address_ = "localhost:50072";
    StartServer(query_node_1_address_, mock_query_node_1_.get(), query_node_1_server_);
    StartServer(query_node_2_address_, mock_query_node_2_.get(), query_node_2_server_);

    // Give servers time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create ProxyService. Post-1.7 the proxy takes a single query-node
    // URI (not a list); gRPC's dns:/// resolver does round_robin over DNS
    // A records at the channel layer. We point at query_node_1 here; the
    // "2 query-nodes" fan-out is now tested in the DNS integration tests,
    // which fake a headless service with multiple A records.
    proxy_service_ = std::make_unique<network::ProxyService>(
        std::vector<std::string>{coordinator_address_},
        query_node_1_address_);
  }

  ~ProxyServiceTest() {
    proxy_service_.reset();

    if (coordinator_server_) coordinator_server_->Shutdown();
    if (query_node_1_server_) query_node_1_server_->Shutdown();
    if (query_node_2_server_) query_node_2_server_->Shutdown();
    if (data_node_server_) data_node_server_->Shutdown();

    coordinator_server_.reset();
    query_node_1_server_.reset();
    query_node_2_server_.reset();
    data_node_server_.reset();

    mock_coordinator_.reset();
    mock_query_node_1_.reset();
    mock_query_node_2_.reset();
    mock_data_node_.reset();
  }

  void StartServer(const std::string& address,
                  grpc::Service* service,
                  std::unique_ptr<grpc::Server>& server) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);
    server = builder.BuildAndStart();
  }

  // Mock services
  std::unique_ptr<MockCoordinatorService> mock_coordinator_;
  std::unique_ptr<MockCoordinatorInternalService> mock_coordinator_internal_;
  std::unique_ptr<MockQueryNodeService> mock_query_node_1_;
  std::unique_ptr<MockQueryNodeService> mock_query_node_2_;
  std::unique_ptr<MockDataNodeService> mock_data_node_;

  // Servers
  std::unique_ptr<grpc::Server> coordinator_server_;
  std::unique_ptr<grpc::Server> query_node_1_server_;
  std::unique_ptr<grpc::Server> query_node_2_server_;
  std::unique_ptr<grpc::Server> data_node_server_;

  // Addresses
  std::string coordinator_address_;
  std::string query_node_1_address_;
  std::string query_node_2_address_;
  std::string data_node_address_;

  // ProxyService under test
  std::unique_ptr<network::ProxyService> proxy_service_;
};

// ============================================================================
// Health Check Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "HealthCheck") {
  grpc::ServerContext context;
  proto::HealthCheckRequest request;
  proto::HealthCheckResponse response;

  auto status = proxy_service_->HealthCheck(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.status(), proto::HealthCheckResponse::SERVING);
  CHECK_EQ(response.message(), "Proxy is healthy");
}

// ============================================================================
// Coordinator Routing Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "CreateCollectionRoutesToCoordinator") {
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test_collection");
  request.set_dimension(128);
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse response;

  auto status = proxy_service_->CreateCollection(&context, &request, &response);

  CHECK(status.ok());
  CHECK_GT(response.collection_id(), 0);
  CHECK_EQ(response.collection_id(), 42);
  CHECK_EQ(mock_coordinator_->create_collection_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "DropCollectionRoutesToCoordinator") {
  grpc::ServerContext context;
  proto::DropCollectionRequest request;
  request.set_collection_name("test_collection");
  proto::DropCollectionResponse response;

  auto status = proxy_service_->DropCollection(&context, &request, &response);

  CHECK(status.ok());
  CHECK_FALSE(response.message().empty());
  CHECK_EQ(mock_coordinator_->drop_collection_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "ListCollectionsRoutesToCoordinator") {
  grpc::ServerContext context;
  proto::ListCollectionsRequest request;
  proto::ListCollectionsResponse response;

  auto status = proxy_service_->ListCollections(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.collections_size(), 1);
  CHECK_EQ(response.collections(0).collection_name(), "test_collection");
  CHECK_EQ(mock_coordinator_->list_collections_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "GetStatsRoutesToCoordinator") {
  grpc::ServerContext context;
  proto::GetStatsRequest request;
  proto::GetStatsResponse response;

  auto status = proxy_service_->GetStats(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.total_collections(), 1);
  CHECK_EQ(response.total_vectors(), 100);
  CHECK_EQ(mock_coordinator_->get_stats_calls.load(), 1);
}

// ============================================================================
// Query Node Routing Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "SearchRoutesToQueryNode") {
  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    query->add_values(1.0f);
  }
  request.set_top_k(10);
  proto::SearchResponse response;

  auto status = proxy_service_->Search(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.results().size(), 1);

  // Post-1.7: proxy uses a single query-node channel (fixture points at
  // mock_query_node_1_), so this call must land there.
  CHECK_EQ(mock_query_node_1_->search_calls.load(), 1);
}

// Post-1.7 the proxy dials a single query-node channel (dns:///<headless>
// with round_robin LB policy) rather than a vector of addresses —
// application-level round-robin across 2+ backends is no longer exercised
// at this layer. The dns:/// fan-out is covered in the DNS integration
// test (test/integration/proxy_dns_test.cpp, new in 1.7), which fakes a
// headless service with multiple A records and asserts round_robin
// distribution + failover. Leaving that responsibility to gRPC's built-in
// resolver keeps the proxy's in-process code minimal.

// ============================================================================
// Data Node Routing Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "InsertRoutesToDataNode") {
  grpc::ServerContext context;
  proto::InsertRequest request;
  request.set_collection_name("test_collection");
  auto* vec = request.add_vectors();
  vec->set_id(1);
  vec->mutable_vector()->set_dimension(128);
  for (int i = 0; i < 128; ++i) {
    vec->mutable_vector()->add_values(1.0f);
  }
  proto::InsertResponse response;

  auto status = proxy_service_->Insert(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.inserted_count(), 1);
  CHECK_EQ(mock_data_node_->insert_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "GetRoutesToDataNode") {
  grpc::ServerContext context;
  proto::GetRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(123);
  proto::GetResponse response;

  auto status = proxy_service_->Get(&context, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.vectors().size(), 1);
  CHECK_EQ(mock_data_node_->get_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "DeleteRoutesToDataNode") {
  grpc::ServerContext context;
  proto::DeleteRequest request;
  request.set_collection_name("test_collection");
  request.add_ids(123);
  proto::DeleteResponse response;

  auto status = proxy_service_->Delete(&context, &request, &response);

  CHECK(status.ok());
  CHECK_GE(response.deleted_count(), 0);
  CHECK_EQ(mock_data_node_->delete_calls.load(), 1);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "UpdateMetadataRoutesToDataNode") {
  grpc::ServerContext context;
  proto::UpdateMetadataRequest request;
  request.set_collection_name("test_collection");
  request.set_id(123);
  request.set_merge(false);  // false = replace, true = merge
  proto::UpdateMetadataResponse response;

  auto status = proxy_service_->UpdateMetadata(&context, &request, &response);

  CHECK(status.ok());
  CHECK_FALSE(response.message().empty());
  CHECK_EQ(mock_data_node_->update_metadata_calls.load(), 1);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "NoCoordinatorAvailable") {
  // Shutdown coordinator
  coordinator_server_->Shutdown();
  coordinator_server_.reset();

  // Wait for shutdown
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create new proxy with invalid coordinator address
  auto proxy = std::make_unique<network::ProxyService>(
      std::vector<std::string>{"localhost:9999"},  // Invalid address
      query_node_1_address_);

  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test");
  request.set_dimension(128);
  proto::CreateCollectionResponse response;

  // Should fail because coordinator is unavailable
  auto status = proxy->CreateCollection(&context, &request, &response);

  // The status might be OK initially due to lazy connection
  // but the response will indicate failure
  // Or it might return UNAVAILABLE immediately
  CHECK((!status.ok() || response.collection_id() == 0));
}

TEST_CASE_FIXTURE(ProxyServiceTest, "NoQueryNodeReturnsUnavailable") {
  // Post-1.7: the pre-existing --data-nodes static fallback was removed in
  // favor of DNS-based discovery. A proxy configured with an empty
  // query-node URI fails Search cleanly rather than silently fanning out
  // to a random data-node. Users deploying without query-nodes are an
  // unsupported configuration; failure here is the correct surface.
  auto proxy = std::make_unique<network::ProxyService>(
      std::vector<std::string>{coordinator_address_},
      std::string{});  // empty URI = no query-node target

  grpc::ServerContext context;
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  request.set_top_k(10);
  proto::SearchResponse response;

  auto status = proxy->Search(&context, &request, &response);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
}

// Previously this slot held a test named "NoDataNodeAvailable" which
// exercised the --data-nodes static-list fallback when coordinator routing
// returned nothing. Post-1.7 that fallback is removed entirely; UNAVAILABLE
// coverage now lives in NoCoordinatorAvailable (which simulates the broader
// "coordinator can't route" case). No dedicated test needed.

// ============================================================================
// Lazy Client Initialization Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "LazyClientInitialization") {
  // Create new proxy (clients not initialized yet)
  auto proxy = std::make_unique<network::ProxyService>(
      std::vector<std::string>{coordinator_address_},
      query_node_1_address_);

  // Clients should be lazily initialized on first use
  grpc::ServerContext context;
  proto::CreateCollectionRequest request;
  request.set_collection_name("test");
  request.set_dimension(128);
  request.set_metric(proto::CreateCollectionRequest::L2);
  request.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse response;

  auto status = proxy->CreateCollection(&context, &request, &response);

  CHECK(status.ok());
  CHECK_GT(response.collection_id(), 0);
}

TEST_CASE_FIXTURE(ProxyServiceTest, "ConcurrentRequests") {
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  // Launch 10 concurrent search requests
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&]() {
      grpc::ServerContext context;
      proto::SearchRequest request;
      request.set_collection_name("test_collection");
      auto* query = request.mutable_query_vector();
      query->set_dimension(128);
      for (int j = 0; j < 128; ++j) {
        query->add_values(1.0f);
      }
      request.set_top_k(10);
      proto::SearchResponse response;

      auto status = proxy_service_->Search(&context, &request, &response);
      if (status.ok()) {
        success_count++;
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  CHECK_EQ(success_count.load(), 10);

  // All 10 requests land on the single configured query-node (the proxy's
  // channel points at query_node_1_address_ in the fixture). Multi-backend
  // distribution is covered in the DNS integration test.
  CHECK_EQ(mock_query_node_1_->search_calls.load(), 10);
}
