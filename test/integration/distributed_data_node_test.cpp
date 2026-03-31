// test/integration/distributed_data_node_test.cpp
// Integration test: coordinator creates collection -> data node serves Insert/Search

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <memory>
#include <thread>
#include <chrono>

#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "cluster/internal_client.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "internal.grpc.pb.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

class DistributedDataNodeTest {
 public:
  DistributedDataNodeTest() {
    // Clean up test directories
    std::filesystem::remove_all("/tmp/gvdb-distributed-dn-test");
    std::filesystem::create_directories("/tmp/gvdb-distributed-dn-test/coordinator");
    std::filesystem::create_directories("/tmp/gvdb-distributed-dn-test/data_node");
    std::filesystem::create_directories("/tmp/gvdb-distributed-dn-test/data_node_2");

    index_factory_ = std::make_unique<index::IndexFactory>();

    // --- Coordinator setup ---
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));

    coord_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-distributed-dn-test/coordinator", index_factory_.get());
    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_manager_.get());

    // --- Data node setup (separate storage) ---
    dn_segment_manager_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-distributed-dn-test/data_node", index_factory_.get());
    dn_query_executor_ = std::make_shared<compute::QueryExecutor>(
        dn_segment_manager_.get());

    // Start coordinator gRPC server (InternalService + VectorDBService)
    StartCoordinator();

    // Register data node in NodeRegistry (simulates heartbeat)
    // Use the coordinator server address so coordinator can call CreateSegment on data node
    RegisterDataNode(dn_server_address_);

    // Create coordinator with client factory that can reach data node
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);

    // Create coordinator's InternalService (serves GetCollectionMetadata)
    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_manager_, coord_query_executor_,
        node_registry_, nullptr, coordinator_);

    // Create coordinator's VectorDBService
    auto coord_resolver = network::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<network::VectorDBService>(
        coord_segment_manager_, coord_query_executor_, std::move(coord_resolver));

    // Start data node gRPC server (InternalService + VectorDBService)
    StartDataNode();

    // Re-register data node with actual data node address
    // (the data node's InternalService will receive CreateSegment RPCs)
    RegisterDataNode(dn_server_address_);

    // Now restart coordinator with the real data node address
    coord_server_->Shutdown();
    coord_server_->Wait();

    // Rebuild coordinator services with fresh state
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);
    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_manager_, coord_query_executor_,
        node_registry_, nullptr, coordinator_);
    auto coord_resolver2 = network::MakeCoordinatorResolver(coordinator_);
    coord_vectordb_service_ = std::make_unique<network::VectorDBService>(
        coord_segment_manager_, coord_query_executor_, std::move(coord_resolver2));

    // Restart coordinator server
    grpc::ServerBuilder coord_builder;
    coord_builder.AddListeningPort(coord_server_address_,
                                    grpc::InsecureServerCredentials());
    coord_builder.RegisterService(coord_internal_service_.get());
    coord_builder.RegisterService(coord_vectordb_service_.get());
    coord_builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    coord_builder.SetMaxSendMessageSize(256 * 1024 * 1024);
    coord_server_ = coord_builder.BuildAndStart();
    REQUIRE_NE(coord_server_, nullptr);

    // Create data node VectorDBService pointing at coordinator
    auto dn_resolver = network::MakeCachedCoordinatorResolver(coord_server_address_);
    dn_vectordb_service_ = std::make_unique<network::VectorDBService>(
        dn_segment_manager_, dn_query_executor_, std::move(dn_resolver));
  }

  ~DistributedDataNodeTest() {
    dn_vectordb_service_.reset();
    coord_vectordb_service_.reset();
    coord_internal_service_.reset();
    dn_internal_service_.reset();

    if (dn_server_) {
      dn_server_->Shutdown();
      dn_server_->Wait();
    }
    if (coord_server_) {
      coord_server_->Shutdown();
      coord_server_->Wait();
    }

    coordinator_.reset();
    node_registry_.reset();
    shard_manager_.reset();
    dn_query_executor_.reset();
    dn_segment_manager_.reset();
    coord_query_executor_.reset();
    coord_segment_manager_.reset();

    std::filesystem::remove_all("/tmp/gvdb-distributed-dn-test");
  }

 private:
  void StartCoordinator() {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    // Register a dummy service temporarily -- we'll restart with real services
    coord_internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_, coord_segment_manager_, coord_query_executor_);
    builder.RegisterService(coord_internal_service_.get());
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);
    coord_server_ = builder.BuildAndStart();
    REQUIRE_NE(coord_server_, nullptr);
    coord_server_address_ = "localhost:" + std::to_string(port);
  }

  void StartDataNode() {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
    auto dn_shard_manager = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    dn_internal_service_ = std::make_unique<network::InternalService>(
        dn_shard_manager, dn_segment_manager_, dn_query_executor_);
    builder.RegisterService(dn_internal_service_.get());
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);
    dn_server_ = builder.BuildAndStart();
    REQUIRE_NE(dn_server_, nullptr);
    dn_server_address_ = "localhost:" + std::to_string(port);
  }

  void RegisterDataNode(const std::string& address) {
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(100);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address(address);
    proto_node.set_memory_total_bytes(8ULL * 1024 * 1024 * 1024);
    proto_node.set_disk_total_bytes(100ULL * 1024 * 1024 * 1024);
    node_registry_->UpdateNode(proto_node);
  }

 public:
  std::unique_ptr<index::IndexFactory> index_factory_;

  // Coordinator components
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
  std::shared_ptr<storage::SegmentManager> coord_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<network::InternalService> coord_internal_service_;
  std::unique_ptr<network::VectorDBService> coord_vectordb_service_;
  std::unique_ptr<grpc::Server> coord_server_;
  std::string coord_server_address_;

  // Data node components
  std::shared_ptr<storage::SegmentManager> dn_segment_manager_;
  std::shared_ptr<compute::QueryExecutor> dn_query_executor_;
  std::unique_ptr<network::InternalService> dn_internal_service_;
  std::unique_ptr<network::VectorDBService> dn_vectordb_service_;
  std::unique_ptr<grpc::Server> dn_server_;
  std::string dn_server_address_;
};

// Test: Coordinator creates collection -> segment appears on data node
TEST_CASE_FIXTURE(DistributedDataNodeTest, "CoordinatorCreatesSegmentOnDataNode") {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_distributed");
  create_req.set_dimension(128);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx;
  auto status = coord_vectordb_service_->CreateCollection(&ctx, &create_req, &create_resp);
  INFO("CreateCollection failed: " << status.error_message());
  REQUIRE(status.ok());
  CHECK_GT(create_resp.collection_id(), 0);

  uint32_t collection_id = create_resp.collection_id();

  // Verify segment was created on data node (shard 0)
  auto segment_id = cluster::ShardSegmentId(core::CollectionId(collection_id), 0);
  auto* segment = dn_segment_manager_->GetSegment(segment_id);
  INFO("Segment not found on data node after CreateCollection");
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetDimension(), 128);
}

// Test: Insert vectors on data node via VectorDBService
TEST_CASE_FIXTURE(DistributedDataNodeTest, "InsertVectorsOnDataNode") {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_insert");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  auto create_status = coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp);
  INFO(create_status.error_message());
  REQUIRE(create_status.ok());

  // Insert vectors via data node's VectorDBService
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_insert");

  for (int i = 1; i <= 5; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(static_cast<float>(i * 2));
    v->add_values(static_cast<float>(i * 3));
    v->add_values(static_cast<float>(i * 4));
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  auto insert_status = dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp);
  INFO("Insert failed: " << insert_status.error_message());
  REQUIRE(insert_status.ok());
  CHECK_EQ(insert_resp.inserted_count(), 5);
}

// Test: Search on data node returns inserted vectors
TEST_CASE_FIXTURE(DistributedDataNodeTest, "SearchOnDataNode") {
  // Create collection
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_search");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  // Insert vectors
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_search");

  for (int i = 1; i <= 10; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Search for vector closest to [1, 0, 0, 0]
  proto::SearchRequest search_req;
  search_req.set_collection_name("test_search");
  search_req.set_top_k(3);
  auto* query = search_req.mutable_query_vector();
  query->set_dimension(4);
  query->add_values(1.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);

  proto::SearchResponse search_resp;
  grpc::ServerContext ctx3;
  auto search_status = dn_vectordb_service_->Search(&ctx3, &search_req, &search_resp);
  INFO("Search failed: " << search_status.error_message());
  REQUIRE(search_status.ok());
  CHECK_EQ(search_resp.results_size(), 3);

  // First result should be vector 1 (exact match)
  CHECK_EQ(search_resp.results(0).id(), 1);
  CHECK(search_resp.results(0).distance() == doctest::Approx(0.0f).epsilon(0.001f));
}

// Test: Get vectors by ID on data node
TEST_CASE_FIXTURE(DistributedDataNodeTest, "GetVectorsOnDataNode") {
  // Create collection + insert
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_get");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_get");
  auto* vec = insert_req.add_vectors();
  vec->set_id(42);
  auto* v = vec->mutable_vector();
  v->set_dimension(4);
  v->add_values(1.0f);
  v->add_values(2.0f);
  v->add_values(3.0f);
  v->add_values(4.0f);

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Get vector by ID
  proto::GetRequest get_req;
  get_req.set_collection_name("test_get");
  get_req.add_ids(42);

  proto::GetResponse get_resp;
  grpc::ServerContext ctx3;
  auto get_status = dn_vectordb_service_->Get(&ctx3, &get_req, &get_resp);
  INFO("Get failed: " << get_status.error_message());
  REQUIRE(get_status.ok());
  CHECK_EQ(get_resp.vectors_size(), 1);
  CHECK_EQ(get_resp.vectors(0).id(), 42);
}

// TODO: Rewrite for multi-shard -- needs separate fixture with 2 data nodes
TEST_CASE_FIXTURE(DistributedDataNodeTest, "DistributedSearchAcrossDataNodes" * doctest::skip(true)) {
  // Create collection via coordinator
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_distributed_search");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());

  // Insert vectors on data node (directly via dn_vectordb_service_)
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_distributed_search");

  // Vectors: [1,0,0,0], [2,0,0,0], ..., [5,0,0,0]
  for (int i = 1; i <= 5; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());
  CHECK_EQ(insert_resp.inserted_count(), 5);

  // Now create a "query node" VectorDBService that has NO local data
  // It should fan out to data nodes via distributed search
  auto qn_segment_manager = std::make_shared<storage::SegmentManager>(
      "/tmp/gvdb-distributed-dn-test/query_node", index_factory_.get());
  auto qn_query_executor = std::make_shared<compute::QueryExecutor>(
      qn_segment_manager.get());
  auto qn_resolver = network::MakeCachedCoordinatorResolver(coord_server_address_);
  auto qn_service = std::make_unique<network::VectorDBService>(
      qn_segment_manager, qn_query_executor, std::move(qn_resolver));

  // Search from query node -- should trigger distributed fan-out to data node
  proto::SearchRequest search_req;
  search_req.set_collection_name("test_distributed_search");
  search_req.set_top_k(3);
  auto* query = search_req.mutable_query_vector();
  query->set_dimension(4);
  query->add_values(1.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);
  query->add_values(0.0f);

  proto::SearchResponse search_resp;
  grpc::ServerContext ctx3;
  auto search_status = qn_service->Search(&ctx3, &search_req, &search_resp);
  INFO("Distributed search failed: " << search_status.error_message());
  REQUIRE(search_status.ok());

  // Should get results from the data node
  CHECK_GT(search_resp.results_size(), 0);
  CHECK_LE(search_resp.results_size(), 3);

  // First result should be vector 1 (closest to query [1,0,0,0])
  CHECK_EQ(search_resp.results(0).id(), 1);
  CHECK(search_resp.results(0).distance() == doctest::Approx(0.0f).epsilon(0.001f));
}

// Test: Coordinator replicates segment data between nodes
TEST_CASE_FIXTURE(DistributedDataNodeTest, "ReplicateSegmentBetweenNodes") {
  // Create collection (segment on data node)
  proto::CreateCollectionRequest create_req;
  create_req.set_collection_name("test_replication");
  create_req.set_dimension(4);
  create_req.set_metric(proto::CreateCollectionRequest::L2);
  create_req.set_index_type(proto::CreateCollectionRequest::FLAT);

  proto::CreateCollectionResponse create_resp;
  grpc::ServerContext ctx1;
  REQUIRE(coord_vectordb_service_->CreateCollection(&ctx1, &create_req, &create_resp).ok());
  uint32_t collection_id = create_resp.collection_id();

  // Insert vectors on data node
  proto::InsertRequest insert_req;
  insert_req.set_collection_name("test_replication");
  for (int i = 1; i <= 3; ++i) {
    auto* vec = insert_req.add_vectors();
    vec->set_id(i);
    auto* v = vec->mutable_vector();
    v->set_dimension(4);
    v->add_values(static_cast<float>(i));
    v->add_values(0.0f);
    v->add_values(0.0f);
    v->add_values(0.0f);
  }

  proto::InsertResponse insert_resp;
  grpc::ServerContext ctx2;
  REQUIRE(dn_vectordb_service_->Insert(&ctx2, &insert_req, &insert_resp).ok());

  // Start a second data node
  auto dn2_segment_manager = std::make_shared<storage::SegmentManager>(
      "/tmp/gvdb-distributed-dn-test/data_node_2", index_factory_.get());
  auto dn2_query_executor = std::make_shared<compute::QueryExecutor>(
      dn2_segment_manager.get());
  auto dn2_shard_manager = std::make_shared<cluster::ShardManager>(
      8, cluster::ShardingStrategy::HASH);
  auto dn2_internal_service = std::make_unique<network::InternalService>(
      dn2_shard_manager, dn2_segment_manager, dn2_query_executor);

  grpc::ServerBuilder dn2_builder;
  int dn2_port = 0;
  dn2_builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &dn2_port);
  dn2_builder.RegisterService(dn2_internal_service.get());
  dn2_builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
  dn2_builder.SetMaxSendMessageSize(256 * 1024 * 1024);
  auto dn2_server = dn2_builder.BuildAndStart();
  REQUIRE_NE(dn2_server, nullptr);
  std::string dn2_address = "localhost:" + std::to_string(dn2_port);

  // Register second data node
  proto::internal::NodeInfo proto_node2;
  proto_node2.set_node_id(200);
  proto_node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node2.set_grpc_address(dn2_address);
  node_registry_->UpdateNode(proto_node2);

  // Replicate segment from data node 1 to data node 2
  core::SegmentId seg_id = cluster::ShardSegmentId(core::CollectionId(collection_id), 0);
  core::NodeId source = core::MakeNodeId(100);
  core::NodeId target = core::MakeNodeId(200);

  auto status = coordinator_->ReplicateSegmentData(seg_id, source, target);
  INFO("Replication failed: " << status.message());
  REQUIRE(status.ok());

  // Verify data node 2 now has the segment with vectors
  auto* replicated_segment = dn2_segment_manager->GetSegment(seg_id);
  INFO("Replicated segment not found on node 2");
  REQUIRE_NE(replicated_segment, nullptr);
  CHECK_EQ(replicated_segment->GetVectorCount(), 3);

  // Cleanup
  dn2_server->Shutdown();
  dn2_server->Wait();
}

// Test: Coordinator handles failed node by promoting replica
TEST_CASE_FIXTURE(DistributedDataNodeTest, "FailoverPromotion") {
  // Register a second data node
  proto::internal::NodeInfo proto_node2;
  proto_node2.set_node_id(200);
  proto_node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node2.set_grpc_address("localhost:59999");
  node_registry_->UpdateNode(proto_node2);

  // Assign shard with node 100 as primary, node 200 as replica
  core::ShardId shard = core::MakeShardId(1);
  core::NodeId primary = core::MakeNodeId(100);
  core::NodeId replica = core::MakeNodeId(200);

  REQUIRE(shard_manager_->SetPrimaryNode(shard, primary).ok());
  REQUIRE(shard_manager_->AddReplica(shard, replica).ok());

  // Verify primary is node 100
  auto primary_before = shard_manager_->GetPrimaryNode(shard);
  REQUIRE(primary_before.ok());
  CHECK_EQ(*primary_before, primary);

  // Simulate node 100 failure -- remove from registry
  node_registry_->RemoveNode(100);

  // Trigger failover
  coordinator_->HandleFailedNode(primary);

  // Verify replica (node 200) was promoted to primary
  auto primary_after = shard_manager_->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, replica);
}

}  // namespace test
}  // namespace gvdb
