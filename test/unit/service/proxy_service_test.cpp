// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <initializer_list>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "auth/auth_context.h"
#include "auth/rbac.h"
#include "internal.grpc.pb.h"
#include "service/proxy/proxy_service.h"
#include "support/grpc_server.h"
#include "vectordb.grpc.pb.h"

namespace {

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

  grpc::Status CreateCollection(grpc::ServerContext* /*context*/,
                                const proto::CreateCollectionRequest* /*request*/,
                                proto::CreateCollectionResponse* response) override {
    create_collection_calls++;
    response->set_collection_id(42);
    response->set_message("Collection created");
    return grpc::Status::OK;
  }

  grpc::Status DropCollection(grpc::ServerContext* /*context*/,
                              const proto::DropCollectionRequest* /*request*/,
                              proto::DropCollectionResponse* response) override {
    drop_collection_calls++;
    response->set_message("Collection dropped");
    return grpc::Status::OK;
  }

  grpc::Status ListCollections(grpc::ServerContext* /*context*/,
                               const proto::ListCollectionsRequest* /*request*/,
                               proto::ListCollectionsResponse* response) override {
    list_collections_calls++;
    auto* collection = response->add_collections();
    collection->set_collection_id(1);
    collection->set_collection_name("test_collection");
    collection->set_dimension(128);
    collection->set_metric_type("L2");
    return grpc::Status::OK;
  }

  grpc::Status GetStats(grpc::ServerContext* /*context*/, const proto::GetStatsRequest* /*request*/,
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

  grpc::Status RouteQuery(grpc::ServerContext* /*context*/,
                          const proto::internal::RouteQueryRequest* /*request*/,
                          proto::internal::RouteQueryResponse* response) override {
    route_query_calls++;
    response->add_target_node_addresses(data_node_address);
    auto* shard_opts = response->add_per_shard_options();
    shard_opts->set_shard_id(0);
    auto* opt = shard_opts->add_options();
    opt->set_node_address(data_node_address);
    opt->set_is_primary(true);
    return grpc::Status::OK;
  }
};

// Mock Query Node Service
class MockQueryNodeService : public proto::VectorDBService::Service {
 public:
  int node_id;
  std::atomic<int> search_calls{0};

  explicit MockQueryNodeService(int id) : node_id(id) {}

  grpc::Status Search(grpc::ServerContext* /*context*/, const proto::SearchRequest* /*request*/,
                      proto::SearchResponse* response) override {
    search_calls++;

    // Add a result entry with node ID encoded in the vector ID
    auto* result = response->add_results();
    result->set_id(static_cast<uint64_t>(node_id) * 1000);  // Encode node ID
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

  grpc::Status Insert(grpc::ServerContext* /*context*/, const proto::InsertRequest* request,
                      proto::InsertResponse* response) override {
    insert_calls++;
    response->set_inserted_count(request->vectors_size());
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext* /*context*/, const proto::GetRequest* /*request*/,
                   proto::GetResponse* response) override {
    get_calls++;
    auto* vec = response->add_vectors();
    vec->set_id(123);
    vec->mutable_vector()->set_dimension(128);
    return grpc::Status::OK;
  }

  grpc::Status Delete(grpc::ServerContext* /*context*/, const proto::DeleteRequest* request,
                      proto::DeleteResponse* response) override {
    delete_calls++;
    response->set_deleted_count(request->ids_size());
    return grpc::Status::OK;
  }

  grpc::Status UpdateMetadata(grpc::ServerContext* /*context*/,
                              const proto::UpdateMetadataRequest* /*request*/,
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
    // Create mock services. Post-1.7 the proxy dials a SINGLE query-node
    // channel (dns:/// + round_robin is gRPC's responsibility), so the
    // fixture only needs one query-node mock. Multi-backend fan-out is
    // covered by the DNS integration tests.
    mock_coordinator_ = std::make_unique<MockCoordinatorService>();
    mock_coordinator_internal_ = std::make_unique<MockCoordinatorInternalService>();
    mock_query_node_ = std::make_unique<MockQueryNodeService>(1);
    mock_data_node_ = std::make_unique<MockDataNodeService>();

    // Each server listens on a port that the operating system picks, and
    // accepts connections as soon as it is constructed.
    // Data-node first so we can tell the coordinator where to route.
    data_node_server_ = StartServer({mock_data_node_.get()});
    data_node_address_ = data_node_server_->address();
    mock_coordinator_internal_->data_node_address = data_node_address_;

    // Coordinator server registers BOTH the public VectorDBService (for
    // metadata ops) AND the internal InternalService (for RouteQuery).
    coordinator_server_ = StartServer({mock_coordinator_.get(), mock_coordinator_internal_.get()});
    coordinator_address_ = coordinator_server_->address();

    // Query node
    query_node_server_ = StartServer({mock_query_node_.get()});
    query_node_address_ = query_node_server_->address();

    proxy_service_ = std::make_unique<service::ProxyService>(
        std::vector<std::string>{coordinator_address_}, query_node_address_);
  }

  ~ProxyServiceTest() {
    proxy_service_.reset();

    // TestGrpcServer shuts the server down in its destructor.
    coordinator_server_.reset();
    query_node_server_.reset();
    data_node_server_.reset();

    mock_coordinator_.reset();
    mock_query_node_.reset();
    mock_data_node_.reset();
  }

  static std::unique_ptr<test::TestGrpcServer> StartServer(
      std::initializer_list<grpc::Service*> services) {
    auto server = std::make_unique<test::TestGrpcServer>(services);
    REQUIRE(server->ok());
    return server;
  }

  // Mock services
  std::unique_ptr<MockCoordinatorService> mock_coordinator_;
  std::unique_ptr<MockCoordinatorInternalService> mock_coordinator_internal_;
  std::unique_ptr<MockQueryNodeService> mock_query_node_;
  std::unique_ptr<MockDataNodeService> mock_data_node_;

  // Servers
  std::unique_ptr<test::TestGrpcServer> coordinator_server_;
  std::unique_ptr<test::TestGrpcServer> query_node_server_;
  std::unique_ptr<test::TestGrpcServer> data_node_server_;

  // Addresses
  std::string coordinator_address_;
  std::string query_node_address_;
  std::string data_node_address_;

  // ProxyService under test
  std::unique_ptr<service::ProxyService> proxy_service_;
};

// ============================================================================
// Health Check Tests
// ============================================================================

TEST_CASE_FIXTURE(ProxyServiceTest, "Proxy HealthCheck") {
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
  // mock_query_node_), so this call must land there.
  CHECK_EQ(mock_query_node_->search_calls.load(), 1);
}

// Post-1.7 the proxy dials a single query-node channel (dns:///<headless>
// with round_robin LB policy) rather than a vector of addresses,
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
  // Stop the coordinator. Shutdown returns after the listening socket is
  // closed. The port came from the operating system, so the proxy below
  // finds nothing listening on coordinator_address_, unless another process
  // binds that exact port in the moment between.
  coordinator_server_->Shutdown();
  coordinator_server_.reset();

  // Create new proxy whose coordinator address has nothing listening
  auto proxy = std::make_unique<service::ProxyService>(
      std::vector<std::string>{coordinator_address_}, query_node_address_);

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
  auto proxy =
      std::make_unique<service::ProxyService>(std::vector<std::string>{coordinator_address_},
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

TEST_CASE_FIXTURE(ProxyServiceTest, "ConcurrentRequests") {
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  // Launch 10 concurrent search requests
  threads.reserve(10);
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
  // channel points at query_node_address_ in the fixture). Multi-backend
  // distribution is covered in the DNS integration test.
  CHECK_EQ(mock_query_node_->search_calls.load(), 10);
}

// ============================================================================
// Role checks
// ============================================================================

// The nodes behind the proxy run without RBAC, so the proxy must check the
// role of each request before it forwards it. A refused request never
// reaches a backend.
struct ProxyRbacTest : ProxyServiceTest {
  ProxyRbacTest() {
    auth::AuthConfig config;
    config.enabled = true;
    config.roles = {{"ro-key", "readonly", {"test_collection"}},
                    {"rw-key", "readwrite", {"test_collection"}},
                    {"admin-key", "admin", {}}};
    auto rbac_store = auth::RbacStore::Create(config);
    REQUIRE(rbac_store.ok());
    proxy_service_ =
        std::make_unique<service::ProxyService>(std::vector<std::string>{coordinator_address_},
                                                query_node_address_, std::move(*rbac_store));
  }

  ~ProxyRbacTest() { auth::AuthContext::Clear(); }

  enum class Rpc { kCreateCollection, kDropCollection, kInsert, kDelete, kSearch };

  grpc::Status Call(Rpc rpc, const std::string& collection) {
    grpc::ServerContext context;
    switch (rpc) {
      case Rpc::kCreateCollection: {
        proto::CreateCollectionRequest request;
        request.set_collection_name(collection);
        request.set_dimension(128);
        proto::CreateCollectionResponse response;
        return proxy_service_->CreateCollection(&context, &request, &response);
      }
      case Rpc::kDropCollection: {
        proto::DropCollectionRequest request;
        request.set_collection_name(collection);
        proto::DropCollectionResponse response;
        return proxy_service_->DropCollection(&context, &request, &response);
      }
      case Rpc::kInsert: {
        proto::InsertRequest request;
        request.set_collection_name(collection);
        auto* vec = request.add_vectors();
        vec->set_id(1);
        vec->mutable_vector()->set_dimension(128);
        for (int i = 0; i < 128; ++i) vec->mutable_vector()->add_values(1.0F);
        proto::InsertResponse response;
        return proxy_service_->Insert(&context, &request, &response);
      }
      case Rpc::kDelete: {
        proto::DeleteRequest request;
        request.set_collection_name(collection);
        request.add_ids(1);
        proto::DeleteResponse response;
        return proxy_service_->Delete(&context, &request, &response);
      }
      case Rpc::kSearch: {
        proto::SearchRequest request;
        request.set_collection_name(collection);
        request.set_top_k(5);
        auto* query = request.mutable_query_vector();
        query->set_dimension(128);
        for (int i = 0; i < 128; ++i) query->add_values(1.0F);
        proto::SearchResponse response;
        return proxy_service_->Search(&context, &request, &response);
      }
    }
    return grpc::Status(grpc::StatusCode::INTERNAL, "unknown rpc");
  }

  // The calls that reached any backend so far.
  int BackendCalls() const {
    return mock_coordinator_->create_collection_calls + mock_coordinator_->drop_collection_calls +
           mock_data_node_->insert_calls + mock_data_node_->delete_calls +
           mock_query_node_->search_calls;
  }
};

TEST_CASE_FIXTURE(ProxyRbacTest, "Proxy checks the role before it forwards") {
  using Code = grpc::StatusCode;
  struct Row {
    const char* key;
    Rpc rpc;
    const char* collection;
    Code expected;
  };
  const Row rows[] = {
      {"ro-key", Rpc::kSearch, "test_collection", Code::OK},
      {"ro-key", Rpc::kInsert, "test_collection", Code::PERMISSION_DENIED},
      {"ro-key", Rpc::kDelete, "test_collection", Code::PERMISSION_DENIED},
      {"ro-key", Rpc::kCreateCollection, "new_coll", Code::PERMISSION_DENIED},
      {"rw-key", Rpc::kInsert, "test_collection", Code::OK},
      {"rw-key", Rpc::kInsert, "other_coll", Code::PERMISSION_DENIED},
      {"rw-key", Rpc::kSearch, "other_coll", Code::PERMISSION_DENIED},
      {"rw-key", Rpc::kDropCollection, "test_collection", Code::PERMISSION_DENIED},
      {"admin-key", Rpc::kCreateCollection, "new_coll", Code::OK},
      {"admin-key", Rpc::kDropCollection, "test_collection", Code::OK},
      {"unknown-key", Rpc::kSearch, "test_collection", Code::UNAUTHENTICATED},
      {"", Rpc::kSearch, "test_collection", Code::UNAUTHENTICATED},
  };
  for (const Row& row : rows) {
    CAPTURE(row.key);
    CAPTURE(static_cast<int>(row.rpc));
    CAPTURE(row.collection);
    auth::AuthContext::SetCurrentKey(row.key);
    const int before = BackendCalls();
    auto status = Call(row.rpc, row.collection);
    CHECK_EQ(status.error_code(), row.expected);
    // An allowed call reaches one backend, a refused one reaches none.
    CHECK_EQ(BackendCalls() - before, row.expected == Code::OK ? 1 : 0);
  }
}

}  // namespace
