// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/internal_service.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "consensus/timestamp_oracle.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"
#include "internal.grpc.pb.h"
#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>

using namespace gvdb;
using namespace gvdb::network;
using namespace gvdb::cluster;

// ============================================================================
// Fixture: Comprehensive InternalService test fixture
// ============================================================================

class InternalServiceRpcTest {
 public:
  InternalServiceRpcTest() {
    // Create a unique temp directory for segment storage
    tmp_dir_ = std::filesystem::temp_directory_path() /
               ("gvdb_rpc_test_" + std::to_string(std::rand()));
    std::filesystem::create_directories(tmp_dir_);

    // Core dependencies
    shard_manager_ = std::make_shared<ShardManager>(8, ShardingStrategy::HASH);
    node_registry_ = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_shared<Coordinator>(shard_manager_, node_registry_);
    timestamp_oracle_ = std::make_shared<consensus::TimestampOracle>();
    index_factory_ = std::make_unique<index::IndexFactory>();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        tmp_dir_.string(), index_factory_.get());

    // Register a fake data node
    proto::internal::NodeInfo proto_node;
    proto_node.set_node_id(1);
    proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    proto_node.set_grpc_address("localhost:50051");
    node_registry_->UpdateNode(proto_node);

    // Create the service under test with all dependencies
    service_ = std::make_unique<InternalService>(
        shard_manager_, segment_store_, nullptr /* query_executor */,
        node_registry_, timestamp_oracle_, coordinator_);
  }

  ~InternalServiceRpcTest() {
    service_.reset();
    segment_store_.reset();
    index_factory_.reset();
    std::filesystem::remove_all(tmp_dir_);
  }

 protected:
  std::filesystem::path tmp_dir_;
  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<NodeRegistry> node_registry_;
  std::shared_ptr<Coordinator> coordinator_;
  std::shared_ptr<consensus::TimestampOracle> timestamp_oracle_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::unique_ptr<InternalService> service_;
};

// ============================================================================
// 1. Shard Management Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "AssignShard_Primary") {
  // Register the node with ShardManager so it can accept assignments
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));

  grpc::ServerContext ctx;
  proto::internal::AssignShardRequest request;
  request.set_shard_id(1);
  request.set_node_id(1);
  request.set_is_primary(true);
  proto::internal::AssignShardResponse response;

  auto status = service_->AssignShard(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK(response.message().find("primary=1") != std::string::npos);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "AssignShard_Replica") {
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));
  // First assign a primary so the shard exists properly
  (void)shard_manager_->SetPrimaryNode(core::MakeShardId(2), core::MakeNodeId(1));

  (void)shard_manager_->RegisterNode(core::MakeNodeId(2));

  grpc::ServerContext ctx;
  proto::internal::AssignShardRequest request;
  request.set_shard_id(2);
  request.set_node_id(2);
  request.set_is_primary(false);
  proto::internal::AssignShardResponse response;

  auto status = service_->AssignShard(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK(response.message().find("primary=0") != std::string::npos);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "AssignShard_NonexistentShard") {
  // Attempt to assign a shard that does not exist (ShardManager has shards 0-7).
  // Shard 100 is out of range, so SetPrimaryNode will return NotFound.
  grpc::ServerContext ctx;
  proto::internal::AssignShardRequest request;
  request.set_shard_id(100);
  request.set_node_id(1);
  request.set_is_primary(true);
  proto::internal::AssignShardResponse response;

  auto status = service_->AssignShard(&ctx, &request, &response);

  // The gRPC call returns OK; the failure is in the response fields
  CHECK(status.ok());
  CHECK_FALSE(response.success());
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetShardAssignments_WithData") {
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));
  (void)shard_manager_->SetPrimaryNode(core::MakeShardId(1), core::MakeNodeId(1));
  (void)shard_manager_->SetPrimaryNode(core::MakeShardId(2), core::MakeNodeId(1));

  grpc::ServerContext ctx;
  proto::internal::GetShardAssignmentsRequest request;
  request.set_collection_id(0);  // 0 = all
  proto::internal::GetShardAssignmentsResponse response;

  auto status = service_->GetShardAssignments(&ctx, &request, &response);

  CHECK(status.ok());
  // ShardManager with 8 shards returns all of them from GetAllShards
  CHECK(response.assignments_size() >= 2);

  // Verify the assigned shards have the correct primary node
  bool found_shard_1 = false;
  for (int i = 0; i < response.assignments_size(); ++i) {
    const auto& assignment = response.assignments(i);
    if (assignment.shard_id() == 1) {
      CHECK_EQ(assignment.primary_node_id(), 1);
      found_shard_1 = true;
    }
  }
  CHECK(found_shard_1);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetShardAssignments_Empty") {
  // Fresh shard manager with no explicit primary assignments
  // GetAllShards still returns shards but with kInvalidNodeId as primary
  grpc::ServerContext ctx;
  proto::internal::GetShardAssignmentsRequest request;
  request.set_collection_id(0);
  proto::internal::GetShardAssignmentsResponse response;

  auto status = service_->GetShardAssignments(&ctx, &request, &response);

  CHECK(status.ok());
  // All shards are returned even without explicit assignment
  CHECK(response.assignments_size() > 0);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "RebalanceShards_NoDistributedMode") {
  grpc::ServerContext ctx;
  proto::internal::RebalanceShardsRequest request;
  request.set_collection_id(1);
  proto::internal::RebalanceShardsResponse response;

  // Coordinator has no client_factory, so rebalance returns OK with 0 moves
  auto status = service_->RebalanceShards(&ctx, &request, &response);
  CHECK(status.ok());
  CHECK_EQ(response.shards_moved(), 0);
}

// ============================================================================
// 2. Segment Operations Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_Success") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(100);
  request.set_collection_id(1);
  request.set_dimension(128);
  request.set_metric_type("L2");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK_EQ(response.segment_id(), 100);
  CHECK(response.message().find("successfully") != std::string::npos);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_InvalidSegmentId") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(0);  // Invalid
  request.set_collection_id(1);
  request.set_dimension(128);
  request.set_metric_type("L2");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_InvalidCollectionId") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(200);
  request.set_collection_id(0);  // Invalid
  request.set_dimension(128);
  request.set_metric_type("L2");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_InvalidDimension") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(201);
  request.set_collection_id(1);
  request.set_dimension(0);  // Invalid
  request.set_metric_type("L2");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_InvalidMetricType") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(202);
  request.set_collection_id(1);
  request.set_dimension(128);
  request.set_metric_type("GARBAGE");
  request.set_index_type("FLAT");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "ListSegments_WithData") {
  // Create a segment first
  {
    grpc::ServerContext ctx;
    proto::internal::CreateSegmentRequest req;
    req.set_segment_id(300);
    req.set_collection_id(5);
    req.set_dimension(64);
    req.set_metric_type("L2");
    req.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse resp;
    auto s = service_->CreateSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // List segments for collection 5
  grpc::ServerContext ctx;
  proto::internal::ListSegmentsRequest request;
  request.set_collection_id(5);
  proto::internal::ListSegmentsResponse response;

  auto status = service_->ListSegments(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.segments_size() >= 1);

  // Verify at least one segment belongs to collection 5
  bool found = false;
  for (int i = 0; i < response.segments_size(); ++i) {
    if (response.segments(i).collection_id() == 5) {
      found = true;
      break;
    }
  }
  CHECK(found);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "ListSegments_Empty") {
  // List segments for a collection that has no segments
  grpc::ServerContext ctx;
  proto::internal::ListSegmentsRequest request;
  request.set_collection_id(9999);
  proto::internal::ListSegmentsResponse response;

  auto status = service_->ListSegments(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.segments_size(), 0);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "DeleteSegment_Success") {
  // Create a segment to delete
  {
    grpc::ServerContext ctx;
    proto::internal::CreateSegmentRequest req;
    req.set_segment_id(400);
    req.set_collection_id(1);
    req.set_dimension(64);
    req.set_metric_type("L2");
    req.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse resp;
    auto s = service_->CreateSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // Delete the segment
  grpc::ServerContext ctx;
  proto::internal::DeleteSegmentRequest request;
  request.set_segment_id(400);
  request.set_force(false);
  proto::internal::DeleteSegmentResponse response;

  auto status = service_->DeleteSegment(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.success());
  CHECK(response.message().find("deleted successfully") != std::string::npos);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "DeleteSegment_InvalidId") {
  grpc::ServerContext ctx;
  proto::internal::DeleteSegmentRequest request;
  request.set_segment_id(0);  // Invalid
  proto::internal::DeleteSegmentResponse response;

  auto status = service_->DeleteSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "DeleteSegment_Nonexistent") {
  grpc::ServerContext ctx;
  proto::internal::DeleteSegmentRequest request;
  request.set_segment_id(999999);
  proto::internal::DeleteSegmentResponse response;

  auto status = service_->DeleteSegment(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK_FALSE(response.success());
  CHECK(response.message().find("not found") != std::string::npos);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetSegment_Success") {
  // Create a segment first
  {
    grpc::ServerContext ctx;
    proto::internal::CreateSegmentRequest req;
    req.set_segment_id(500);
    req.set_collection_id(1);
    req.set_dimension(32);
    req.set_metric_type("L2");
    req.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse resp;
    auto s = service_->CreateSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // Get the segment
  grpc::ServerContext ctx;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(500);
  proto::internal::GetSegmentResponse response;

  auto status = service_->GetSegment(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.has_segment_info());
  CHECK_EQ(response.segment_info().segment_id(), 500);
  CHECK_EQ(response.segment_info().collection_id(), 1);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetSegment_InvalidId") {
  grpc::ServerContext ctx;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(0);  // Invalid
  proto::internal::GetSegmentResponse response;

  auto status = service_->GetSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetSegment_Nonexistent") {
  grpc::ServerContext ctx;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(888888);
  proto::internal::GetSegmentResponse response;

  auto status = service_->GetSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// ============================================================================
// 3. Metadata Sync Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "SyncMetadata_ReturnsTimestamp") {
  grpc::ServerContext ctx;
  proto::internal::SyncMetadataRequest request;
  request.set_node_id(1);
  request.set_last_sync_timestamp(0);
  proto::internal::SyncMetadataResponse response;

  auto status = service_->SyncMetadata(&ctx, &request, &response);

  CHECK(status.ok());
  // The current_timestamp should be a positive value representing system time
  CHECK(response.current_timestamp() > 0);
}

// ============================================================================
// 4. Query Routing Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "RouteQuery_Success") {
  // Create a collection in the coordinator
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));
  auto coll_result = coordinator_->CreateCollection(
      "route_test", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(coll_result.ok());

  // Assign primary node for the shard
  auto metadata = coordinator_->GetCollectionMetadata("route_test");
  REQUIRE(metadata.ok());
  for (const auto& sid : metadata->shard_ids) {
    (void)shard_manager_->SetPrimaryNode(sid, core::MakeNodeId(1));
  }

  grpc::ServerContext ctx;
  proto::internal::RouteQueryRequest request;
  request.set_collection_name("route_test");
  request.set_top_k(10);
  proto::internal::RouteQueryResponse response;

  auto status = service_->RouteQuery(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.collection_id() > 0);
  CHECK(response.target_shard_ids_size() > 0);
  CHECK(response.target_node_ids_size() > 0);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "RouteQuery_NoCoordinator") {
  // Create service without coordinator
  auto service_no_coord = std::make_unique<InternalService>(
      shard_manager_, segment_store_, nullptr,
      node_registry_, timestamp_oracle_, nullptr /* no coordinator */);

  grpc::ServerContext ctx;
  proto::internal::RouteQueryRequest request;
  request.set_collection_name("anything");
  request.set_top_k(5);
  proto::internal::RouteQueryResponse response;

  auto status = service_no_coord->RouteQuery(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "RouteQuery_NonexistentCollection") {
  grpc::ServerContext ctx;
  proto::internal::RouteQueryRequest request;
  request.set_collection_name("does_not_exist");
  request.set_top_k(10);
  proto::internal::RouteQueryResponse response;

  auto status = service_->RouteQuery(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// Roadmap 0b.1: when a shard's primary is draining and the caller set
// prefer_routable_replica, RouteQuery must return a non-draining replica
// instead of the draining primary. This is the integration-level proof that
// the drain signal actually sheds query traffic off the draining node.
TEST_CASE_FIXTURE(InternalServiceRpcTest,
                  "RouteQuery_PrefersReplicaWhenPrimaryDraining") {
  // Register two data nodes (primary=1, replica=2) with both the registry
  // and the shard manager.
  proto::internal::NodeInfo replica_info;
  replica_info.set_node_id(2);
  replica_info.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  replica_info.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  replica_info.set_grpc_address("localhost:50052");
  node_registry_->UpdateNode(replica_info);

  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));
  (void)shard_manager_->RegisterNode(core::MakeNodeId(2));

  auto coll = coordinator_->CreateCollection(
      "drain_route_test", 8, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(coll.ok());
  auto metadata = coordinator_->GetCollectionMetadata("drain_route_test");
  REQUIRE(metadata.ok());
  for (const auto& sid : metadata->shard_ids) {
    (void)shard_manager_->SetPrimaryNode(sid, core::MakeNodeId(1));
    (void)shard_manager_->AddReplica(sid, core::MakeNodeId(2));
  }

  // Primary transitions to DRAINING via a final heartbeat.
  proto::internal::NodeInfo drain_info;
  drain_info.set_node_id(1);
  drain_info.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  drain_info.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  drain_info.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(drain_info);

  // Read path: prefer_routable_replica=true → coordinator must pick replica 2.
  {
    grpc::ServerContext ctx;
    proto::internal::RouteQueryRequest req;
    req.set_collection_name("drain_route_test");
    req.set_prefer_routable_replica(true);
    proto::internal::RouteQueryResponse resp;
    auto status = service_->RouteQuery(&ctx, &req, &resp);
    REQUIRE(status.ok());
    REQUIRE(resp.target_node_ids_size() > 0);
    for (int i = 0; i < resp.target_node_ids_size(); ++i) {
      CHECK_EQ(resp.target_node_ids(i), 2u);
      CHECK_EQ(resp.target_node_addresses(i), "localhost:50052");
    }
  }

  // Write path: prefer_routable_replica=false (default) → must stay on
  // primary 1 even though it is draining. The drain signal must not silently
  // redirect writes; 0b.3 handles primary migration.
  {
    grpc::ServerContext ctx;
    proto::internal::RouteQueryRequest req;
    req.set_collection_name("drain_route_test");
    // read_only left default (false)
    proto::internal::RouteQueryResponse resp;
    auto status = service_->RouteQuery(&ctx, &req, &resp);
    REQUIRE(status.ok());
    REQUIRE(resp.target_node_ids_size() > 0);
    for (int i = 0; i < resp.target_node_ids_size(); ++i) {
      CHECK_EQ(resp.target_node_ids(i), 1u);
      CHECK_EQ(resp.target_node_addresses(i), "localhost:50051");
    }
  }
}

// If the primary is draining AND no routable replica exists, RouteQuery must
// fall back to the primary rather than returning an empty address. Better to
// have stale routing than broken routing — the caller will see a connection
// error on the now-dead node and can retry.
TEST_CASE_FIXTURE(InternalServiceRpcTest,
                  "RouteQuery_FallsBackToPrimaryWhenNoReplicaRoutable") {
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));

  auto coll = coordinator_->CreateCollection(
      "drain_solo", 8, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(coll.ok());
  auto metadata = coordinator_->GetCollectionMetadata("drain_solo");
  REQUIRE(metadata.ok());
  for (const auto& sid : metadata->shard_ids) {
    (void)shard_manager_->SetPrimaryNode(sid, core::MakeNodeId(1));
  }

  // Primary drains; no replica exists.
  proto::internal::NodeInfo drain_info;
  drain_info.set_node_id(1);
  drain_info.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  drain_info.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  drain_info.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(drain_info);

  grpc::ServerContext ctx;
  proto::internal::RouteQueryRequest req;
  req.set_collection_name("drain_solo");
  req.set_prefer_routable_replica(true);
  proto::internal::RouteQueryResponse resp;
  auto status = service_->RouteQuery(&ctx, &req, &resp);
  REQUIRE(status.ok());
  REQUIRE(resp.target_node_ids_size() > 0);
  for (int i = 0; i < resp.target_node_ids_size(); ++i) {
    CHECK_EQ(resp.target_node_ids(i), 1u);
  }
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "ExecuteShardQuery_NonexistentSegment") {
  grpc::ServerContext ctx;
  proto::internal::ExecuteShardQueryRequest request;
  request.set_collection_id(9999);
  request.set_shard_id(0);
  request.add_query_vector(1.0f);
  request.add_query_vector(2.0f);
  request.set_top_k(5);
  proto::internal::ExecuteShardQueryResponse response;

  auto status = service_->ExecuteShardQuery(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// ============================================================================
// 5. Health Monitoring Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "Heartbeat_Acknowledged") {
  grpc::ServerContext ctx;
  proto::internal::HeartbeatRequest request;
  auto* info = request.mutable_node_info();
  info->set_node_id(10);
  info->set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  info->set_grpc_address("localhost:50060");
  proto::internal::HeartbeatResponse response;

  auto status = service_->Heartbeat(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.acknowledged());
  CHECK(response.timestamp() > 0);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "Heartbeat_UpdatesRegistry") {
  // Send heartbeat for a new node
  {
    grpc::ServerContext ctx;
    proto::internal::HeartbeatRequest request;
    auto* info = request.mutable_node_info();
    info->set_node_id(20);
    info->set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
    info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    info->set_grpc_address("localhost:50070");
    proto::internal::HeartbeatResponse response;

    auto status = service_->Heartbeat(&ctx, &request, &response);
    REQUIRE(status.ok());
  }

  // Verify node was added to the registry
  RegisteredNode node;
  bool found = node_registry_->GetNode(20, &node);
  CHECK(found);
  CHECK_EQ(node.info.node_id(), 20);
  CHECK_EQ(node.info.node_type(), proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "Heartbeat_ReturnsAssignedShards") {
  // Register node and assign shards
  (void)shard_manager_->RegisterNode(core::MakeNodeId(30));
  (void)shard_manager_->SetPrimaryNode(core::MakeShardId(1), core::MakeNodeId(30));
  (void)shard_manager_->SetPrimaryNode(core::MakeShardId(3), core::MakeNodeId(30));

  grpc::ServerContext ctx;
  proto::internal::HeartbeatRequest request;
  auto* info = request.mutable_node_info();
  info->set_node_id(30);
  info->set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto::internal::HeartbeatResponse response;

  auto status = service_->Heartbeat(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.acknowledged());
  // The node has 2 shard assignments (primary for shard 1 and 3)
  CHECK(response.assigned_shards_size() >= 2);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetClusterHealth_Healthy") {
  // Register healthy nodes
  proto::internal::NodeInfo n1;
  n1.set_node_id(40);
  n1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node_registry_->UpdateNode(n1);

  proto::internal::NodeInfo n2;
  n2.set_node_id(41);
  n2.set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
  n2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node_registry_->UpdateNode(n2);

  grpc::ServerContext ctx;
  proto::internal::GetClusterHealthRequest request;
  proto::internal::GetClusterHealthResponse response;

  auto status = service_->GetClusterHealth(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.cluster_status(), "healthy");
  // Should include the healthy nodes we registered plus the one from fixture setup
  CHECK(response.nodes_size() >= 2);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetClusterHealth_Degraded") {
  // Create a registry with a short timeout so nodes quickly become "failed"
  auto short_registry = std::make_shared<NodeRegistry>(std::chrono::milliseconds(1));

  // Register three nodes (they will all timeout immediately)
  proto::internal::NodeInfo n1;
  n1.set_node_id(50);
  n1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  short_registry->UpdateNode(n1);

  proto::internal::NodeInfo n2;
  n2.set_node_id(51);
  n2.set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
  n2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  short_registry->UpdateNode(n2);

  proto::internal::NodeInfo n3;
  n3.set_node_id(52);
  n3.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n3.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  short_registry->UpdateNode(n3);

  // Wait for timeout to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  // Re-register two nodes to make them healthy again (healthy=2, failed=1)
  // The logic is: healthy_nodes > failed_nodes -> "degraded"
  short_registry->UpdateNode(n1);
  short_registry->UpdateNode(n3);

  // Create a service with the short-timeout registry
  auto degraded_service = std::make_unique<InternalService>(
      shard_manager_, segment_store_, nullptr,
      short_registry, timestamp_oracle_, coordinator_);

  grpc::ServerContext ctx;
  proto::internal::GetClusterHealthRequest request;
  proto::internal::GetClusterHealthResponse response;

  auto status = degraded_service->GetClusterHealth(&ctx, &request, &response);

  CHECK(status.ok());
  // Stats: healthy=2, failed=1 -> "degraded" (healthy > failed, but failed > 0)
  CHECK_EQ(response.cluster_status(), "degraded");
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetClusterHealth_NoRegistry") {
  // Create service without node registry (single-node mode)
  auto service_no_registry = std::make_unique<InternalService>(
      shard_manager_, segment_store_, nullptr,
      nullptr /* no registry */, timestamp_oracle_, coordinator_);

  grpc::ServerContext ctx;
  proto::internal::GetClusterHealthRequest request;
  proto::internal::GetClusterHealthResponse response;

  auto status = service_no_registry->GetClusterHealth(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK_EQ(response.cluster_status(), "healthy");
  CHECK_EQ(response.nodes_size(), 0);
}

// ============================================================================
// 6. Timestamp Oracle Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetTimestamp_Monotonic") {
  grpc::ServerContext ctx1;
  proto::internal::GetTimestampRequest request1;
  request1.set_count(1);
  proto::internal::GetTimestampResponse response1;

  auto s1 = service_->GetTimestamp(&ctx1, &request1, &response1);
  CHECK(s1.ok());
  CHECK(response1.start_timestamp() > 0);

  grpc::ServerContext ctx2;
  proto::internal::GetTimestampRequest request2;
  request2.set_count(1);
  proto::internal::GetTimestampResponse response2;

  auto s2 = service_->GetTimestamp(&ctx2, &request2, &response2);
  CHECK(s2.ok());

  // Second timestamp must be strictly greater than the first
  CHECK(response2.start_timestamp() > response1.start_timestamp());
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetTimestamp_WithCount") {
  grpc::ServerContext ctx;
  proto::internal::GetTimestampRequest request;
  request.set_count(5);
  proto::internal::GetTimestampResponse response;

  auto status = service_->GetTimestamp(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.start_timestamp() > 0);
  CHECK(response.end_timestamp() >= response.start_timestamp());
  // With count=5, the oracle allocates 5 separate timestamps,
  // so end should be greater than start
  CHECK(response.end_timestamp() > response.start_timestamp());
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetTimestamp_WithoutOracle") {
  // Create service without timestamp oracle (fallback to system time)
  auto service_no_tso = std::make_unique<InternalService>(
      shard_manager_, segment_store_, nullptr,
      node_registry_, nullptr /* no oracle */, coordinator_);

  grpc::ServerContext ctx;
  proto::internal::GetTimestampRequest request;
  request.set_count(3);
  proto::internal::GetTimestampResponse response;

  auto status = service_no_tso->GetTimestamp(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.start_timestamp() > 0);
  CHECK(response.end_timestamp() >= response.start_timestamp());
  // Fallback logic: end = start + count - 1 = start + 2
  CHECK_EQ(response.end_timestamp(), response.start_timestamp() + 2);
}

// ============================================================================
// 7. Data Transfer Tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "TransferData_NoDistributedMode") {
  grpc::ServerContext ctx;
  proto::internal::TransferDataRequest request;
  request.set_collection_id(1);
  request.set_shard_id(1);
  request.set_source_node_id(1);
  request.set_target_node_id(2);
  proto::internal::TransferDataResponse response;

  // Coordinator has no client_factory, so transfer fails gracefully
  auto status = service_->TransferData(&ctx, &request, &response);
  CHECK(status.ok());
  CHECK_FALSE(response.success());
}

// ============================================================================
// Additional edge case tests
// ============================================================================

TEST_CASE_FIXTURE(InternalServiceRpcTest, "GetTimestamp_ZeroCountDefaultsToOne") {
  grpc::ServerContext ctx;
  proto::internal::GetTimestampRequest request;
  request.set_count(0);  // Should be treated as 1
  proto::internal::GetTimestampResponse response;

  auto status = service_->GetTimestamp(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.start_timestamp() > 0);
  // With count defaulting to 1, start and end should be equal
  CHECK_EQ(response.start_timestamp(), response.end_timestamp());
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_ThenGetSegment_RoundTrip") {
  // Create a segment
  {
    grpc::ServerContext ctx;
    proto::internal::CreateSegmentRequest req;
    req.set_segment_id(600);
    req.set_collection_id(3);
    req.set_dimension(64);
    req.set_metric_type("COSINE");
    req.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse resp;
    auto s = service_->CreateSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // Get the segment and verify fields match
  {
    grpc::ServerContext ctx;
    proto::internal::GetSegmentRequest req;
    req.set_segment_id(600);
    proto::internal::GetSegmentResponse resp;
    auto s = service_->GetSegment(&ctx, &req, &resp);
    CHECK(s.ok());
    CHECK_EQ(resp.segment_info().segment_id(), 600);
    CHECK_EQ(resp.segment_info().collection_id(), 3);
    CHECK_EQ(resp.segment_info().vector_count(), 0);
    // Newly created segment should not be sealed
    CHECK_FALSE(resp.segment_info().is_sealed());
  }
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_ThenDelete_ThenGetFails") {
  // Create
  {
    grpc::ServerContext ctx;
    proto::internal::CreateSegmentRequest req;
    req.set_segment_id(700);
    req.set_collection_id(1);
    req.set_dimension(16);
    req.set_metric_type("L2");
    req.set_index_type("FLAT");
    proto::internal::CreateSegmentResponse resp;
    auto s = service_->CreateSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // Delete
  {
    grpc::ServerContext ctx;
    proto::internal::DeleteSegmentRequest req;
    req.set_segment_id(700);
    proto::internal::DeleteSegmentResponse resp;
    auto s = service_->DeleteSegment(&ctx, &req, &resp);
    REQUIRE(s.ok());
    REQUIRE(resp.success());
  }

  // Get should now fail
  {
    grpc::ServerContext ctx;
    proto::internal::GetSegmentRequest req;
    req.set_segment_id(700);
    proto::internal::GetSegmentResponse resp;
    auto s = service_->GetSegment(&ctx, &req, &resp);
    CHECK_FALSE(s.ok());
    CHECK_EQ(s.error_code(), grpc::StatusCode::NOT_FOUND);
  }
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "Heartbeat_ZeroNodeId_NoShards") {
  // A heartbeat with node_id=0 should still be acknowledged
  // but should not return assigned shards (the code checks node_id > 0)
  grpc::ServerContext ctx;
  proto::internal::HeartbeatRequest request;
  auto* info = request.mutable_node_info();
  info->set_node_id(0);
  info->set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto::internal::HeartbeatResponse response;

  auto status = service_->Heartbeat(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.acknowledged());
  CHECK_EQ(response.assigned_shards_size(), 0);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "SyncMetadata_TimestampIsRecent") {
  auto before = std::chrono::system_clock::now().time_since_epoch().count();

  grpc::ServerContext ctx;
  proto::internal::SyncMetadataRequest request;
  request.set_node_id(1);
  request.set_last_sync_timestamp(0);
  proto::internal::SyncMetadataResponse response;

  auto status = service_->SyncMetadata(&ctx, &request, &response);

  auto after = std::chrono::system_clock::now().time_since_epoch().count();

  CHECK(status.ok());
  CHECK(response.current_timestamp() >= before);
  CHECK(response.current_timestamp() <= after);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "CreateSegment_InvalidIndexType") {
  grpc::ServerContext ctx;
  proto::internal::CreateSegmentRequest request;
  request.set_segment_id(800);
  request.set_collection_id(1);
  request.set_dimension(128);
  request.set_metric_type("L2");
  request.set_index_type("NOT_AN_INDEX");
  proto::internal::CreateSegmentResponse response;

  auto status = service_->CreateSegment(&ctx, &request, &response);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE_FIXTURE(InternalServiceRpcTest, "RouteQuery_WithShardAssignment") {
  // Full end-to-end route query: create collection, assign shards, route
  (void)shard_manager_->RegisterNode(core::MakeNodeId(1));
  auto coll_result = coordinator_->CreateCollection(
      "full_route_test", 64, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  REQUIRE(coll_result.ok());

  auto metadata = coordinator_->GetCollectionMetadata("full_route_test");
  REQUIRE(metadata.ok());
  for (const auto& sid : metadata->shard_ids) {
    (void)shard_manager_->SetPrimaryNode(sid, core::MakeNodeId(1));
  }

  grpc::ServerContext ctx;
  proto::internal::RouteQueryRequest request;
  request.set_collection_name("full_route_test");
  request.set_top_k(5);
  // Add a query vector (not used by RouteQuery but part of the message)
  request.add_query_vector(1.0f);
  request.add_query_vector(2.0f);
  proto::internal::RouteQueryResponse response;

  auto status = service_->RouteQuery(&ctx, &request, &response);

  CHECK(status.ok());
  CHECK(response.target_shard_ids_size() > 0);
  CHECK(response.target_node_ids_size() == response.target_shard_ids_size());
  CHECK(response.target_node_addresses_size() == response.target_shard_ids_size());

  // The first target node should be node 1
  CHECK_EQ(response.target_node_ids(0), 1);
}
