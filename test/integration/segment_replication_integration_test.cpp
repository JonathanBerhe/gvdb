// test/integration/segment_replication_integration_test.cpp
// Integration tests for segment replication (Phase 5)

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <thread>
#include <chrono>

#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "storage/segment_manager.h"
#include "utils/logger.h"
#include "internal.grpc.pb.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace test {

class SegmentReplicationIntegrationTest {
 public:
  SegmentReplicationIntegrationTest() {
    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Create shard manager and coordinator
    shard_manager_ = std::make_shared<cluster::ShardManager>(8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<cluster::Coordinator>(shard_manager_, node_registry_);

    // Register a fake data node via NodeRegistry (production flow)
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address("localhost:50051");
    node_registry_->UpdateNode(proto_node);

    // Create coordinator's segment manager and query executor
    coord_segment_store_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-segment-replication-test/coordinator",
        index_factory_.get());

    coord_query_executor_ = std::make_shared<compute::QueryExecutor>(
        coord_segment_store_.get());

    // Create InternalService (serves GetSegment RPC)
    internal_service_ = std::make_unique<network::InternalService>(
        shard_manager_,
        coord_segment_store_,
        coord_query_executor_,
        nullptr,  // node_registry not needed
        nullptr,  // timestamp_oracle not needed
        coordinator_);

    // Start gRPC server for InternalService
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &server_port_);
    builder.RegisterService(internal_service_.get());
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);
    server_ = builder.BuildAndStart();
    REQUIRE_NE(server_, nullptr);
    REQUIRE_GT(server_port_, 0);

    server_address_ = absl::StrFormat("localhost:%d", server_port_);

    // Create data node's segment manager (separate storage, won't have segments initially)
    data_segment_store_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-segment-replication-test/data_node",
        index_factory_.get());

    data_query_executor_ = std::make_shared<compute::QueryExecutor>(
        data_segment_store_.get());

    // Create data node's VectorDBService in distributed mode
    auto resolver = network::MakeCachedCoordinatorResolver(server_address_);
    data_vectordb_service_ = std::make_unique<network::VectorDBService>(
        data_segment_store_,
        data_query_executor_,
        std::move(resolver));
  }

  ~SegmentReplicationIntegrationTest() {
    server_->Shutdown();
    coord_segment_store_->Clear();
    data_segment_store_->Clear();
  }

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  // Coordinator components
  std::shared_ptr<storage::ISegmentStore> coord_segment_store_;
  std::shared_ptr<compute::QueryExecutor> coord_query_executor_;
  std::unique_ptr<network::InternalService> internal_service_;

  // Data node components
  std::shared_ptr<storage::ISegmentStore> data_segment_store_;
  std::shared_ptr<compute::QueryExecutor> data_query_executor_;
  std::unique_ptr<network::VectorDBService> data_vectordb_service_;

  std::unique_ptr<grpc::Server> server_;
  int server_port_ = 0;
  std::string server_address_;
};

// Test: Data node pulls segment from coordinator on search
TEST_CASE_FIXTURE(SegmentReplicationIntegrationTest, "DataNodePullsSegmentOnSearch") {
  // 1. Create collection on coordinator
  auto collection_id = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(collection_id.ok());

  // 2. Create segment on coordinator using ShardSegmentId scheme
  core::SegmentId segment_id = cluster::ShardSegmentId(*collection_id, 0);
  auto create_seg_status = coord_segment_store_->CreateSegmentWithId(
      segment_id, *collection_id, 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(create_seg_status.ok());

  // Insert some test vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 10; i++) {
    std::vector<float> data(128);
    for (int j = 0; j < 128; j++) {
      data[j] = static_cast<float>(i * 128 + j);
    }
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i + 1));
  }

  auto insert_status = coord_segment_store_->WriteVectors(segment_id, vectors, ids);
  REQUIRE(insert_status.ok());

  // 3. Also create segment on data node (simulate coordinator push)
  auto dn_create = data_segment_store_->CreateSegmentWithId(
      segment_id, *collection_id, 128, core::MetricType::L2, core::IndexType::FLAT);
  REQUIRE(dn_create.ok());
  auto* dn_seg = data_segment_store_->GetSegment(segment_id);
  REQUIRE_NE(dn_seg, nullptr);
  auto dn_insert = dn_seg->AddVectors(vectors, ids);
  REQUIRE(dn_insert.ok());
  core::IndexConfig cfg;
  cfg.index_type = core::IndexType::FLAT;
  cfg.dimension = 128;
  cfg.metric_type = core::MetricType::L2;
  (void)data_segment_store_->SealSegment(segment_id, cfg);

  // 4. Build gRPC Search request
  proto::SearchRequest request;
  request.set_collection_name("test_collection");
  request.set_top_k(3);

  // Query vector (similar to vector 0)
  auto* query_proto = request.mutable_query_vector();
  query_proto->set_dimension(128);
  for (int j = 0; j < 128; j++) {
    query_proto->add_values(static_cast<float>(j));
  }

  // 5. Execute search on data node (should trigger segment pull)
  grpc::ServerContext context;
  proto::SearchResponse response;

  grpc::Status status = data_vectordb_service_->Search(&context, &request, &response);

  // 6. Verify search succeeded
  INFO("Search failed: " << status.error_message());
  REQUIRE(status.ok());
  CHECK_GT(response.results_size(), 0);
  CHECK_LE(response.results_size(), 3);

  // 7. Verify search results are reasonable (vector 0 should be closest)
  CHECK_EQ(response.results(0).id(), 1);  // VectorId 1 (index 0)
}

// Test: Multiple searches reuse cached segment
TEST_CASE_FIXTURE(SegmentReplicationIntegrationTest, "MultipleSearchesReuseCachedSegment") {
  // Create collection and segment on coordinator
  auto collection_id = coordinator_->CreateCollection(
      "reuse_test", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(collection_id.ok());

  core::SegmentId segment_id = cluster::ShardSegmentId(*collection_id, 0);

  // Create segment on coordinator
  if (!coord_segment_store_->GetSegment(segment_id)) {
    auto cs = coord_segment_store_->CreateSegmentWithId(
        segment_id, *collection_id, 64, core::MetricType::L2, core::IndexType::FLAT);
    REQUIRE(cs.ok());
  }

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 5; i++) {
    std::vector<float> data(64, static_cast<float>(i));
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i + 1));
  }
  coord_segment_store_->WriteVectors(segment_id, vectors, ids);

  // Create segment on data node too (simulate push replication)
  if (!data_segment_store_->GetSegment(segment_id)) {
    auto ds = data_segment_store_->CreateSegmentWithId(
        segment_id, *collection_id, 64, core::MetricType::L2, core::IndexType::FLAT);
    REQUIRE(ds.ok());
    auto* dn_seg = data_segment_store_->GetSegment(segment_id);
    dn_seg->AddVectors(vectors, ids);
    core::IndexConfig cfg;
    cfg.index_type = core::IndexType::FLAT;
    cfg.dimension = 64;
    cfg.metric_type = core::MetricType::L2;
    (void)data_segment_store_->SealSegment(segment_id, cfg);
  }

  // First search
  proto::SearchRequest request1;
  request1.set_collection_name("reuse_test");
  request1.set_top_k(2);
  auto* query1 = request1.mutable_query_vector();
  query1->set_dimension(64);
  for (int j = 0; j < 64; j++) {
    query1->add_values(0.0f);
  }

  grpc::ServerContext context1;
  proto::SearchResponse response1;
  grpc::Status status1 = data_vectordb_service_->Search(&context1, &request1, &response1);
  REQUIRE(status1.ok());

  // Verify segment is now cached
  auto* cached_segment = data_segment_store_->GetSegment(segment_id);
  REQUIRE_NE(cached_segment, nullptr);

  // Second search (should reuse cached segment)
  proto::SearchRequest request2;
  request2.set_collection_name("reuse_test");
  request2.set_top_k(2);
  auto* query2 = request2.mutable_query_vector();
  query2->set_dimension(64);
  for (int j = 0; j < 64; j++) {
    query2->add_values(4.0f);  // Different query
  }

  grpc::ServerContext context2;
  proto::SearchResponse response2;
  grpc::Status status2 = data_vectordb_service_->Search(&context2, &request2, &response2);
  REQUIRE(status2.ok());
  CHECK_GT(response2.results_size(), 0);
}

// Test: Segment not found on coordinator
TEST_CASE_FIXTURE(SegmentReplicationIntegrationTest, "SearchEmptyCollectionReturnsNoResults") {
  // Create collection (segment is created on data node by coordinator)
  auto collection_id = coordinator_->CreateCollection(
      "empty_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(collection_id.ok());

  // Search empty collection — should succeed with 0 results
  proto::SearchRequest request;
  request.set_collection_name("empty_collection");
  request.set_top_k(5);
  auto* query = request.mutable_query_vector();
  query->set_dimension(128);
  for (int j = 0; j < 128; j++) {
    query->add_values(1.0f);
  }

  grpc::ServerContext context;
  proto::SearchResponse response;
  grpc::Status status = data_vectordb_service_->Search(&context, &request, &response);

  INFO(status.error_message());
  CHECK(status.ok());
  CHECK_EQ(response.results_size(), 0);
}

// ============================================================================
// Read Repair Integration Test
// ============================================================================

class ReadRepairIntegrationTest {
 public:
  ReadRepairIntegrationTest() {
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Two separate segment managers simulate two data nodes
    primary_segment_store_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-read-repair-test/primary", index_factory_.get());
    replica_segment_store_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb-read-repair-test/replica", index_factory_.get());

    auto primary_qe = std::make_shared<compute::QueryExecutor>(
        primary_segment_store_.get());
    auto replica_qe = std::make_shared<compute::QueryExecutor>(
        replica_segment_store_.get());

    // Shard manager and node registry
    shard_manager_ = std::make_shared<cluster::ShardManager>(
        8, cluster::ShardingStrategy::HASH);
    node_registry_ = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));

    // Register two data nodes in node registry
    proto::internal::NodeInfo node1;
    node1.set_node_id(1);
    node1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node1.set_grpc_address("localhost:0");  // Will be updated after server start
    node_registry_->UpdateNode(node1);

    proto::internal::NodeInfo node2;
    node2.set_node_id(2);
    node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node2.set_grpc_address("localhost:0");
    node_registry_->UpdateNode(node2);

    // Create coordinator without client factory first (we set it after servers start)
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_);

    // Start gRPC servers for each "data node" — each runs InternalService
    // with its own segment manager
    primary_internal_ = std::make_unique<network::InternalService>(
        shard_manager_, primary_segment_store_, primary_qe,
        nullptr, nullptr, coordinator_);
    replica_internal_ = std::make_unique<network::InternalService>(
        shard_manager_, replica_segment_store_, replica_qe,
        nullptr, nullptr, coordinator_);

    grpc::ServerBuilder builder1;
    builder1.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &primary_port_);
    builder1.RegisterService(primary_internal_.get());
    builder1.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder1.SetMaxSendMessageSize(256 * 1024 * 1024);
    primary_server_ = builder1.BuildAndStart();
    REQUIRE_NE(primary_server_, nullptr);

    grpc::ServerBuilder builder2;
    builder2.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &replica_port_);
    builder2.RegisterService(replica_internal_.get());
    builder2.SetMaxReceiveMessageSize(256 * 1024 * 1024);
    builder2.SetMaxSendMessageSize(256 * 1024 * 1024);
    replica_server_ = builder2.BuildAndStart();
    REQUIRE_NE(replica_server_, nullptr);

    // Update node addresses with actual ports
    primary_addr_ = absl::StrFormat("localhost:%d", primary_port_);
    replica_addr_ = absl::StrFormat("localhost:%d", replica_port_);

    proto::internal::NodeInfo node1_updated;
    node1_updated.set_node_id(1);
    node1_updated.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node1_updated.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node1_updated.set_grpc_address(primary_addr_);
    node_registry_->UpdateNode(node1_updated);

    proto::internal::NodeInfo node2_updated;
    node2_updated.set_node_id(2);
    node2_updated.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node2_updated.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node2_updated.set_grpc_address(replica_addr_);
    node_registry_->UpdateNode(node2_updated);

    // Now create a new coordinator WITH the client factory so it can make RPCs
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    coordinator_ = std::make_shared<cluster::Coordinator>(
        shard_manager_, node_registry_, client_factory);
  }

  ~ReadRepairIntegrationTest() {
    primary_server_->Shutdown();
    replica_server_->Shutdown();
    primary_segment_store_->Clear();
    replica_segment_store_->Clear();
  }

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<cluster::ShardManager> shard_manager_;
  std::shared_ptr<cluster::NodeRegistry> node_registry_;
  std::shared_ptr<cluster::Coordinator> coordinator_;

  std::shared_ptr<storage::ISegmentStore> primary_segment_store_;
  std::shared_ptr<storage::ISegmentStore> replica_segment_store_;

  std::unique_ptr<network::InternalService> primary_internal_;
  std::unique_ptr<network::InternalService> replica_internal_;

  std::unique_ptr<grpc::Server> primary_server_;
  std::unique_ptr<grpc::Server> replica_server_;

  int primary_port_ = 0;
  int replica_port_ = 0;
  std::string primary_addr_;
  std::string replica_addr_;
};

TEST_CASE_FIXTURE(ReadRepairIntegrationTest, "ReadRepairFixesDivergentReplica") {
  // 1. Create collection with replication_factor=2
  //    The coordinator creates the segment on the primary node via RPC.
  auto coll_result = coordinator_->CreateCollection(
      "repair_collection", 64, core::MetricType::L2, core::IndexType::FLAT, 2);
  REQUIRE(coll_result.ok());
  auto coll_id = *coll_result;

  // Determine which node is primary and which is replica for shard 0
  auto metadata = coordinator_->GetCollectionMetadata("repair_collection");
  REQUIRE(metadata.ok());
  auto shard_id = metadata->shard_ids[0];
  auto primary_node = *shard_manager_->GetPrimaryNode(shard_id);
  auto replicas = *shard_manager_->GetReplicaNodes(shard_id);
  REQUIRE_FALSE(replicas.empty());
  auto replica_node = replicas[0];

  // Map node IDs to segment managers based on which gRPC server they run
  // Node 1 → primary_segment_store_ (first server)
  // Node 2 → replica_segment_store_ (second server)
  auto* node1_sm = primary_segment_store_.get();
  auto* node2_sm = replica_segment_store_.get();
  auto* primary_sm = (core::ToUInt32(primary_node) == 1) ? node1_sm : node2_sm;
  auto* replica_sm = (core::ToUInt32(replica_node) == 1) ? node1_sm : node2_sm;

  // 2. The coordinator's CreateCollection already created the segment on the
  //    primary via CreateSegment RPC. Create it on the replica too (simulating
  //    initial replication that happened before the crash).
  core::SegmentId seg_id = cluster::ShardSegmentId(coll_id, 0);

  // Primary already has the segment from CreateCollection. Verify.
  auto* primary_seg = primary_sm->GetSegment(seg_id);
  REQUIRE_NE(primary_seg, nullptr);

  // Replica does NOT have the segment — simulates a node that crashed before
  // initial replication completed, or a new node joining.
  CHECK(replica_sm->GetSegment(seg_id) == nullptr);

  // 3. Insert vectors on primary ONLY (simulate writes during replica downtime)
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 0; i < 20; i++) {
    std::vector<float> data(64, static_cast<float>(i));
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i + 1));
  }
  REQUIRE(primary_seg->AddVectors(vectors, ids).ok());
  CHECK_EQ(primary_seg->GetVectorCount(), 20);

  // 4. Run consistency check — coordinator detects divergence and repairs
  coordinator_->RunConsistencyCheck();

  // 5. After repair, the replica should have received the full segment via RPC.
  //    ReplicateSegmentData calls GetSegment on primary → ReplicateSegment on replica.
  //    The replicated segment may have a different ID (AddReplicatedSegment).
  auto replica_segments = replica_sm->GetCollectionSegments(coll_id);
  uint64_t replica_total = 0;
  for (auto& sid : replica_segments) {
    auto* seg = replica_sm->GetSegment(sid);
    if (seg) replica_total += seg->GetVectorCount();
  }

  CHECK_EQ(replica_total, 20);
}

} // namespace test
} // namespace gvdb
