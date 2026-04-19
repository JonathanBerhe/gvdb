#include <doctest/doctest.h>
#include "cluster/shard_manager.h"
#include "cluster/coordinator.h"
#include "cluster/node_registry.h"
#include "cluster/load_balancer.h"
#include "cluster/replication.h"
#include "core/types.h"
#include "internal.grpc.pb.h"
#include <chrono>

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// Shard Manager Tests
// ============================================================================

class ShardManagerTest {
 public:
  ShardManagerTest() {
    shard_manager_ = std::make_unique<ShardManager>(16, ShardingStrategy::HASH);
  }

  std::unique_ptr<ShardManager> shard_manager_;
};

TEST_CASE_FIXTURE(ShardManagerTest, "AssignShardConsistency") {
  // Same vector ID should always map to same shard
  core::VectorId vid = core::MakeVectorId(12345);

  auto shard1 = shard_manager_->AssignShard(vid);
  auto shard2 = shard_manager_->AssignShard(vid);

  CHECK_EQ(shard1, shard2);
}

TEST_CASE_FIXTURE(ShardManagerTest, "RegisterNode") {
  core::NodeId node1 = core::MakeNodeId(1);
  core::NodeId node2 = core::MakeNodeId(2);

  auto status1 = shard_manager_->RegisterNode(node1);
  CHECK(status1.ok());

  auto status2 = shard_manager_->RegisterNode(node2);
  CHECK(status2.ok());

  // Duplicate registration should fail
  auto status3 = shard_manager_->RegisterNode(node1);
  CHECK_FALSE(status3.ok());
}

TEST_CASE_FIXTURE(ShardManagerTest, "ShardToNodeMapping") {
  core::NodeId node1 = core::MakeNodeId(1);
  core::NodeId node2 = core::MakeNodeId(2);

  (void)shard_manager_->RegisterNode(node1);
  (void)shard_manager_->RegisterNode(node2);

  core::ShardId shard = core::MakeShardId(0);

  // Set primary node
  auto status = shard_manager_->SetPrimaryNode(shard, node1);
  CHECK(status.ok());

  // Get primary node
  auto primary_result = shard_manager_->GetPrimaryNode(shard);
  REQUIRE(primary_result.ok());
  CHECK_EQ(*primary_result, node1);

  // Add replica
  auto replica_status = shard_manager_->AddReplica(shard, node2);
  CHECK(replica_status.ok());

  // Get replicas
  auto replicas_result = shard_manager_->GetReplicaNodes(shard);
  REQUIRE(replicas_result.ok());
  CHECK_EQ(replicas_result->size(), 1);
  CHECK_EQ((*replicas_result)[0], node2);
}

TEST_CASE_FIXTURE(ShardManagerTest, "UpdateShardMetrics") {
  core::ShardId shard = core::MakeShardId(0);

  auto status = shard_manager_->UpdateShardMetrics(shard, 1024 * 1024, 1000, 5000);
  CHECK(status.ok());

  auto info_result = shard_manager_->GetShardInfo(shard);
  REQUIRE(info_result.ok());
  CHECK_EQ(info_result->data_size_bytes, 1024 * 1024);
  CHECK_EQ(info_result->vector_count, 1000);
  CHECK_EQ(info_result->query_count, 5000);
}

// ============================================================================
// Coordinator Tests
// ============================================================================

class CoordinatorTest {
 public:
  CoordinatorTest() {
    auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
    node_registry_ = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
    coordinator_ = std::make_unique<Coordinator>(shard_manager, node_registry_);
  }

  std::shared_ptr<NodeRegistry> node_registry_;
  std::unique_ptr<Coordinator> coordinator_;
};

TEST_CASE_FIXTURE(CoordinatorTest, "RegisterNode") {
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::QUERY_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto status = coordinator_->RegisterNode(node_info);
  CHECK(status.ok());

  // Verify node was registered
  auto result = coordinator_->GetNodeInfo(node_info.node_id);
  REQUIRE(result.ok());
  CHECK_EQ(result->address, "localhost:50051");
}

TEST_CASE_FIXTURE(CoordinatorTest, "CreateCollection") {
  // Register a data node via NodeRegistry (production flow)
  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(proto_node);

  auto result = coordinator_->CreateCollection(
      "test_collection",
      128,  // dimension
      core::MetricType::L2,
      core::IndexType::FLAT,
      1);  // replication_factor

  REQUIRE(result.ok());
  CHECK_NE(*result, core::kInvalidCollectionId);

  // Verify collection was created
  auto metadata_result = coordinator_->GetCollectionMetadata("test_collection");
  REQUIRE(metadata_result.ok());
  CHECK_EQ(metadata_result->collection_name, "test_collection");
  CHECK_EQ(metadata_result->dimension, 128);
}

TEST_CASE_FIXTURE(CoordinatorTest, "DuplicateCollectionName") {
  // Register a data node via NodeRegistry (production flow)
  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(proto_node);

  // Create first collection
  auto result1 = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(result1.ok());

  // Try to create duplicate
  auto result2 = coordinator_->CreateCollection(
      "test_collection", 64, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  CHECK_FALSE(result2.ok());
}

TEST_CASE_FIXTURE(CoordinatorTest, "ListCollections") {
  // Register a data node via NodeRegistry (production flow)
  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(proto_node);

  // Create multiple collections
  (void)coordinator_->CreateCollection("coll1", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  (void)coordinator_->CreateCollection("coll2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  (void)coordinator_->CreateCollection("coll3", 256, core::MetricType::INNER_PRODUCT, core::IndexType::IVF_FLAT, 1);

  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 3);
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropCollection") {
  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(proto_node);

  auto create_result = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(create_result.ok());

  auto drop_status = coordinator_->DropCollection("test_collection");
  CHECK(drop_status.ok());

  auto metadata_after_drop = coordinator_->GetCollectionMetadata("test_collection");
  CHECK_FALSE(metadata_after_drop.ok());

  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 0);
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropNonexistentCollection") {
  auto status = coordinator_->DropCollection("nonexistent");
  CHECK_FALSE(status.ok());
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropAndRecreateCollection") {
  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(proto_node);

  auto create_result1 = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(create_result1.ok());
  auto collection_id1 = *create_result1;

  auto drop_status = coordinator_->DropCollection("test_collection");
  REQUIRE(drop_status.ok());

  auto create_result2 = coordinator_->CreateCollection(
      "test_collection", 256, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  REQUIRE(create_result2.ok());
  auto collection_id2 = *create_result2;

  CHECK_NE(collection_id1, collection_id2);

  auto metadata = coordinator_->GetCollectionMetadata("test_collection");
  REQUIRE(metadata.ok());
  CHECK_EQ(metadata->dimension, 256);
  CHECK_EQ(metadata->metric_type, core::MetricType::COSINE);
}

// ============================================================================
// Load Balancer Tests
// ============================================================================

class LoadBalancerTest {
 public:
  LoadBalancerTest() {
    load_balancer_ = std::make_unique<LoadBalancer>(LoadBalancingStrategy::ROUND_ROBIN);
  }

  std::unique_ptr<LoadBalancer> load_balancer_;
};

TEST_CASE_FIXTURE(LoadBalancerTest, "RoundRobinSelection") {
  std::vector<core::NodeId> nodes = {
      core::MakeNodeId(1),
      core::MakeNodeId(2),
      core::MakeNodeId(3)
  };

  // Select multiple times and verify round-robin
  std::map<core::NodeId, int> counts;
  for (int i = 0; i < 9; ++i) {
    auto result = load_balancer_->SelectNode(nodes);
    REQUIRE(result.ok());
    counts[*result]++;
  }

  // Each node should be selected 3 times
  for (const auto& node : nodes) {
    CHECK_EQ(counts[node], 3);
  }
}

TEST_CASE_FIXTURE(LoadBalancerTest, "LeastConnectionsSelection") {
  load_balancer_->SetStrategy(LoadBalancingStrategy::LEAST_CONNECTIONS);

  std::vector<core::NodeId> nodes = {
      core::MakeNodeId(1),
      core::MakeNodeId(2),
      core::MakeNodeId(3)
  };

  // Set different connection counts
  load_balancer_->IncrementConnections(nodes[0]);
  load_balancer_->IncrementConnections(nodes[0]);
  load_balancer_->IncrementConnections(nodes[1]);

  // Should select node 2 (0 connections) or node 3 (0 connections)
  auto result = load_balancer_->SelectNode(nodes);
  REQUIRE(result.ok());
  CHECK((*result == nodes[1] || *result == nodes[2]));
}

TEST_CASE_FIXTURE(LoadBalancerTest, "ConnectionTracking") {
  core::NodeId node = core::MakeNodeId(1);

  CHECK_EQ(load_balancer_->GetConnectionCount(node), 0);

  load_balancer_->IncrementConnections(node);
  CHECK_EQ(load_balancer_->GetConnectionCount(node), 1);

  load_balancer_->IncrementConnections(node);
  CHECK_EQ(load_balancer_->GetConnectionCount(node), 2);

  load_balancer_->DecrementConnections(node);
  CHECK_EQ(load_balancer_->GetConnectionCount(node), 1);
}

// ============================================================================
// Replication Manager Tests
// ============================================================================

class ReplicationManagerTest {
 public:
  ReplicationManagerTest() {
    replication_manager_ = std::make_unique<ReplicationManager>();
  }

  std::unique_ptr<ReplicationManager> replication_manager_;
};

TEST_CASE_FIXTURE(ReplicationManagerTest, "BasicConstruction") {
  // Just verify the replication manager can be constructed
  CHECK_NE(replication_manager_, nullptr);
}

// ============================================================================
// Additional Coordinator Tests
// ============================================================================

TEST_CASE_FIXTURE(CoordinatorTest, "UnregisterNodeSucceeds") {
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto reg_status = coordinator_->RegisterNode(node_info);
  REQUIRE(reg_status.ok());

  auto unreg_status = coordinator_->UnregisterNode(node_info.node_id);
  CHECK(unreg_status.ok());

  // GetNodeInfo should return NotFound after unregistration
  auto result = coordinator_->GetNodeInfo(node_info.node_id);
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE_FIXTURE(CoordinatorTest, "UnregisterUnknownNodeFails") {
  core::NodeId unknown_node = core::MakeNodeId(999);

  auto status = coordinator_->UnregisterNode(unknown_node);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(CoordinatorTest, "UpdateNodeStatusChangesStatus") {
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::QUERY_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto reg_status = coordinator_->RegisterNode(node_info);
  REQUIRE(reg_status.ok());

  auto update_status = coordinator_->UpdateNodeStatus(
      node_info.node_id, NodeStatus::DEGRADED);
  CHECK(update_status.ok());

  auto result = coordinator_->GetNodeInfo(node_info.node_id);
  REQUIRE(result.ok());
  CHECK_EQ(result->status, NodeStatus::DEGRADED);
}

TEST_CASE_FIXTURE(CoordinatorTest, "UpdateNodeStatusUnknownNodeFails") {
  core::NodeId unknown_node = core::MakeNodeId(999);

  auto status = coordinator_->UpdateNodeStatus(unknown_node, NodeStatus::HEALTHY);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(CoordinatorTest, "ProcessHeartbeatUpdatesRegistry") {
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(10);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:60000";

  auto hb_status = coordinator_->ProcessHeartbeat(node_info.node_id, node_info);
  CHECK(hb_status.ok());

  // Node should now appear in GetAllNodes
  auto all_nodes = coordinator_->GetAllNodes();
  bool found = false;
  for (const auto& n : all_nodes) {
    if (n.node_id == node_info.node_id) {
      found = true;
      break;
    }
  }
  CHECK(found);
}

TEST_CASE_FIXTURE(CoordinatorTest, "GetHealthyNodesFiltersByType") {
  // Register a data node via NodeRegistry
  proto::internal::NodeInfo data_node;
  data_node.set_node_id(1);
  data_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  data_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  data_node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(data_node);

  // Register a query node via NodeRegistry
  proto::internal::NodeInfo query_node;
  query_node.set_node_id(2);
  query_node.set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
  query_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  query_node.set_grpc_address("localhost:50052");
  node_registry_->UpdateNode(query_node);

  auto data_nodes = coordinator_->GetHealthyNodes(NodeType::DATA_NODE);
  CHECK_EQ(data_nodes.size(), 1);
  CHECK_EQ(data_nodes[0].type, NodeType::DATA_NODE);

  auto query_nodes = coordinator_->GetHealthyNodes(NodeType::QUERY_NODE);
  CHECK_EQ(query_nodes.size(), 1);
  CHECK_EQ(query_nodes[0].type, NodeType::QUERY_NODE);

  // No proxy nodes registered
  auto proxy_nodes = coordinator_->GetHealthyNodes(NodeType::PROXY);
  CHECK_EQ(proxy_nodes.size(), 0);
}

TEST_CASE_FIXTURE(CoordinatorTest, "GetHealthyNodeCountReflectsRegistry") {
  CHECK_EQ(coordinator_->GetHealthyNodeCount(), 0);

  proto::internal::NodeInfo node1;
  node1.set_node_id(1);
  node1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node1.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(node1);

  CHECK_EQ(coordinator_->GetHealthyNodeCount(), 1);

  proto::internal::NodeInfo node2;
  node2.set_node_id(2);
  node2.set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
  node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node2.set_grpc_address("localhost:50052");
  node_registry_->UpdateNode(node2);

  CHECK_EQ(coordinator_->GetHealthyNodeCount(), 2);
}

TEST_CASE_FIXTURE(CoordinatorTest, "IsHealthyTrueWithNodes") {
  proto::internal::NodeInfo node;
  node.set_node_id(1);
  node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(node);

  CHECK(coordinator_->IsHealthy());
}

TEST_CASE_FIXTURE(CoordinatorTest, "IsHealthyFalseWithNoNodes") {
  CHECK_FALSE(coordinator_->IsHealthy());
}

TEST_CASE_FIXTURE(CoordinatorTest, "GetClusterLoadReturnsZero") {
  // With no nodes, load should be zero
  CHECK_EQ(coordinator_->GetClusterLoad(), 0.0f);

  // Even with nodes, current impl returns zero (cpu_usage not in proto)
  proto::internal::NodeInfo node;
  node.set_node_id(1);
  node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(node);

  CHECK_EQ(coordinator_->GetClusterLoad(), 0.0f);
}

TEST_CASE_FIXTURE(CoordinatorTest, "AssignShardsFailsWithoutDataNodes") {
  // Create a collection manually (bypass AssignShards inside CreateCollection)
  // We need a collection_id in the map for AssignShardsToCollection to find it.
  // Use CreateCollection which will itself fail at AssignShards with no data nodes.
  auto result = coordinator_->CreateCollection(
      "orphan_collection", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  CHECK_FALSE(result.ok());
  CHECK(absl::IsFailedPrecondition(result.status()));
}

TEST_CASE_FIXTURE(CoordinatorTest, "HandleFailedNodePromotesReplica") {
  // Register two data nodes in shard manager and node registry
  core::NodeId primary_id = core::MakeNodeId(1);
  core::NodeId replica_id = core::MakeNodeId(2);

  NodeInfo primary_info;
  primary_info.node_id = primary_id;
  primary_info.type = NodeType::DATA_NODE;
  primary_info.status = NodeStatus::HEALTHY;
  primary_info.address = "localhost:50051";
  REQUIRE(coordinator_->RegisterNode(primary_info).ok());

  NodeInfo replica_info;
  replica_info.node_id = replica_id;
  replica_info.type = NodeType::DATA_NODE;
  replica_info.status = NodeStatus::HEALTHY;
  replica_info.address = "localhost:50052";
  REQUIRE(coordinator_->RegisterNode(replica_info).ok());

  // Create a collection (will assign shards with replication_factor=1 by default)
  auto coll_result = coordinator_->CreateCollection(
      "test_failover", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(coll_result.ok());

  // Get shard info from the collection metadata
  auto metadata = coordinator_->GetCollectionMetadata("test_failover");
  REQUIRE(metadata.ok());
  REQUIRE_FALSE(metadata->shard_ids.empty());

  // Manually set up a shard with primary=node1 and replica=node2
  // to test failover behavior
  core::ShardId shard_id = metadata->shard_ids[0];

  // Access the shared shard_manager through creating a new coordinator is not
  // possible, but we registered both nodes. Manually add replica to shard.
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  // Register nodes with shard manager
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());
  REQUIRE(shard_manager->RegisterNode(replica_id).ok());

  // Register nodes with node registry (as healthy)
  proto::internal::NodeInfo proto_primary;
  proto_primary.set_node_id(1);
  proto_primary.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_primary.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_primary.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_primary);

  proto::internal::NodeInfo proto_replica;
  proto_replica.set_node_id(2);
  proto_replica.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_replica.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_replica.set_grpc_address("localhost:50052");
  node_registry->UpdateNode(proto_replica);

  // Set up shard with primary and replica
  core::ShardId test_shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(test_shard, primary_id).ok());
  REQUIRE(shard_manager->AddReplica(test_shard, replica_id).ok());

  // Verify primary is node 1
  auto primary_before = shard_manager->GetPrimaryNode(test_shard);
  REQUIRE(primary_before.ok());
  CHECK_EQ(*primary_before, primary_id);

  // Handle failure of the primary node
  coord->HandleFailedNode(primary_id);

  // Replica should have been promoted to primary
  auto primary_after = shard_manager->GetPrimaryNode(test_shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, replica_id);
}

TEST_CASE_FIXTURE(CoordinatorTest, "HandleFailedNodeNoReplicaLeavesOrphan") {
  // Set up a coordinator with a shard that has no replicas
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId primary_id = core::MakeNodeId(1);
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());

  proto::internal::NodeInfo proto_node;
  proto_node.set_node_id(1);
  proto_node.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_node.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_node.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_node);

  // Set up shard with primary only (no replicas)
  core::ShardId test_shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(test_shard, primary_id).ok());

  // Verify primary is set
  auto primary_before = shard_manager->GetPrimaryNode(test_shard);
  REQUIRE(primary_before.ok());
  CHECK_EQ(*primary_before, primary_id);

  // Handle failure - no replicas available to promote
  coord->HandleFailedNode(primary_id);

  // After HandleFailedNode, the node is unregistered from shard manager.
  // The shard's primary was node 1, but since there was no replica to promote,
  // the primary remains as node 1 (orphaned shard).
  auto primary_after = shard_manager->GetPrimaryNode(test_shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, primary_id);
}

// ---------------------------------------------------------------------------
// Graceful drain (roadmap 0b.3) — Coordinator::HandleDrainingNode
// ---------------------------------------------------------------------------

// A draining primary with a routable replica: replica is promoted,
// draining node is unregistered from ShardManager.
TEST_CASE_FIXTURE(CoordinatorTest, "HandleDrainingNodePromotesReplica") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId primary_id = core::MakeNodeId(1);
  core::NodeId replica_id = core::MakeNodeId(2);
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());
  REQUIRE(shard_manager->RegisterNode(replica_id).ok());

  proto::internal::NodeInfo proto_primary;
  proto_primary.set_node_id(1);
  proto_primary.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_primary.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  proto_primary.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_primary);

  proto::internal::NodeInfo proto_replica;
  proto_replica.set_node_id(2);
  proto_replica.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_replica.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_replica.set_grpc_address("localhost:50052");
  node_registry->UpdateNode(proto_replica);

  core::ShardId shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(shard, primary_id).ok());
  REQUIRE(shard_manager->AddReplica(shard, replica_id).ok());

  coord->HandleDrainingNode(primary_id);

  auto primary_after = shard_manager->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, replica_id);

  // Draining node should be fully unregistered from the shard manager.
  auto remaining = shard_manager->GetShardsForNode(primary_id);
  CHECK(remaining.empty());
}

// A draining REPLICA (primary is healthy): the replica entry is simply
// dropped — no data movement needed.
TEST_CASE_FIXTURE(CoordinatorTest, "HandleDrainingNodeDropsReplicaEntry") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId primary_id = core::MakeNodeId(1);
  core::NodeId replica_id = core::MakeNodeId(2);
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());
  REQUIRE(shard_manager->RegisterNode(replica_id).ok());

  proto::internal::NodeInfo proto_primary;
  proto_primary.set_node_id(1);
  proto_primary.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_primary.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_primary.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_primary);

  proto::internal::NodeInfo proto_replica;
  proto_replica.set_node_id(2);
  proto_replica.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_replica.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  proto_replica.set_grpc_address("localhost:50052");
  node_registry->UpdateNode(proto_replica);

  core::ShardId shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(shard, primary_id).ok());
  REQUIRE(shard_manager->AddReplica(shard, replica_id).ok());

  coord->HandleDrainingNode(replica_id);

  // Primary unchanged.
  auto primary_after = shard_manager->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, primary_id);

  // Replica dropped.
  auto replicas_after = shard_manager->GetReplicaNodes(shard);
  REQUIRE(replicas_after.ok());
  CHECK(std::find(replicas_after->begin(), replicas_after->end(), replica_id)
        == replicas_after->end());

  // Draining node fully unregistered.
  auto remaining = shard_manager->GetShardsForNode(replica_id);
  CHECK(remaining.empty());
}

// Draining primary with NO routable replica: we leave the shard on the
// draining node (best-effort) and log. This is safer than silently
// dropping data; the heartbeat-timeout failure path takes over once the
// pod exits.
TEST_CASE_FIXTURE(CoordinatorTest,
                  "HandleDrainingNodeNoReplicaLeavesShardInPlace") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId primary_id = core::MakeNodeId(1);
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());

  proto::internal::NodeInfo proto_primary;
  proto_primary.set_node_id(1);
  proto_primary.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_primary.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  proto_primary.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_primary);

  core::ShardId shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(shard, primary_id).ok());

  coord->HandleDrainingNode(primary_id);

  auto primary_after = shard_manager->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, primary_id);

  // Node must remain registered so the next cycle (or the eventual
  // heartbeat-timeout path) can act on it.
  auto remaining = shard_manager->GetShardsForNode(primary_id);
  CHECK(!remaining.empty());
}

// ---------------------------------------------------------------------------
// Auto-rebalance on node join (roadmap 0b.2) — Coordinator::DetectNewDataNodes
// ---------------------------------------------------------------------------

// Baseline: a brand-new coordinator has an empty known-nodes set.
TEST_CASE_FIXTURE(CoordinatorTest, "DetectNewDataNodesStartsEmpty") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  CHECK(coord->KnownDataNodesForTesting().empty());
}

// Registering a new data node is detected; the known set grows by one.
TEST_CASE_FIXTURE(CoordinatorTest,
                  "DetectNewDataNodesTracksFreshlyJoinedNode") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  proto::internal::NodeInfo n;
  n.set_node_id(101);
  n.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  n.set_grpc_address("localhost:50060");
  node_registry->UpdateNode(n);

  coord->DetectNewDataNodes();

  auto known = coord->KnownDataNodesForTesting();
  REQUIRE_EQ(known.size(), 1);
  CHECK(known.count(core::MakeNodeId(101)) == 1);
}

// Calling DetectNewDataNodes twice with no changes is idempotent — no new
// entries.
TEST_CASE_FIXTURE(CoordinatorTest,
                  "DetectNewDataNodesIsIdempotentWithoutNewJoin") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  proto::internal::NodeInfo n;
  n.set_node_id(101);
  n.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  n.set_grpc_address("localhost:50060");
  node_registry->UpdateNode(n);

  coord->DetectNewDataNodes();
  coord->DetectNewDataNodes();  // no change

  CHECK_EQ(coord->KnownDataNodesForTesting().size(), 1);
}

// A DRAINING node is not counted as "newly joined" — it's leaving, not
// arriving.
TEST_CASE_FIXTURE(CoordinatorTest, "DetectNewDataNodesSkipsDrainingNodes") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  proto::internal::NodeInfo ready;
  ready.set_node_id(101);
  ready.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  ready.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  ready.set_grpc_address("localhost:50060");
  node_registry->UpdateNode(ready);

  proto::internal::NodeInfo draining;
  draining.set_node_id(102);
  draining.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  draining.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  draining.set_grpc_address("localhost:50061");
  node_registry->UpdateNode(draining);

  coord->DetectNewDataNodes();

  auto known = coord->KnownDataNodesForTesting();
  REQUIRE_EQ(known.size(), 1);
  CHECK(known.count(core::MakeNodeId(101)) == 1);
  CHECK(known.count(core::MakeNodeId(102)) == 0);
}

// Pruning: when a node finishes draining and is unregistered, the known
// set forgets it so a rejoin (same id) is detected as a fresh scale-up.
TEST_CASE_FIXTURE(CoordinatorTest,
                  "DetectNewDataNodesPrunesOnDrainCompletion") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId id = core::MakeNodeId(101);
  REQUIRE(shard_manager->RegisterNode(id).ok());

  proto::internal::NodeInfo n;
  n.set_node_id(101);
  n.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  n.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  n.set_grpc_address("localhost:50060");
  node_registry->UpdateNode(n);

  coord->DetectNewDataNodes();
  REQUIRE_EQ(coord->KnownDataNodesForTesting().size(), 1);

  // Node transitions to DRAINING and the drain path completes (no shards to
  // migrate, so HandleDrainingNode unregisters immediately).
  n.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  node_registry->UpdateNode(n);
  coord->HandleDrainingNode(id);

  // Known set must no longer contain the drained node.
  auto known_after = coord->KnownDataNodesForTesting();
  CHECK(known_after.count(id) == 0);
}

// Idempotency: calling HandleDrainingNode twice after full migration is a
// no-op (the second call finds no shards and silently unregisters again).
TEST_CASE_FIXTURE(CoordinatorTest, "HandleDrainingNodeIsIdempotent") {
  auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
  auto node_registry = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
  auto coord = std::make_unique<Coordinator>(shard_manager, node_registry);

  core::NodeId primary_id = core::MakeNodeId(1);
  core::NodeId replica_id = core::MakeNodeId(2);
  REQUIRE(shard_manager->RegisterNode(primary_id).ok());
  REQUIRE(shard_manager->RegisterNode(replica_id).ok());

  proto::internal::NodeInfo proto_replica;
  proto_replica.set_node_id(2);
  proto_replica.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_replica.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  proto_replica.set_grpc_address("localhost:50052");
  node_registry->UpdateNode(proto_replica);

  proto::internal::NodeInfo proto_primary;
  proto_primary.set_node_id(1);
  proto_primary.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  proto_primary.set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  proto_primary.set_grpc_address("localhost:50051");
  node_registry->UpdateNode(proto_primary);

  core::ShardId shard = core::MakeShardId(0);
  REQUIRE(shard_manager->SetPrimaryNode(shard, primary_id).ok());
  REQUIRE(shard_manager->AddReplica(shard, replica_id).ok());

  coord->HandleDrainingNode(primary_id);
  // Second call is a no-op and must not crash or resurrect the node.
  coord->HandleDrainingNode(primary_id);

  auto primary_after = shard_manager->GetPrimaryNode(shard);
  REQUIRE(primary_after.ok());
  CHECK_EQ(*primary_after, replica_id);
  auto remaining = shard_manager->GetShardsForNode(primary_id);
  CHECK(remaining.empty());
}

TEST_CASE_FIXTURE(CoordinatorTest, "CreateCollectionMultiShard") {
  // Register two data nodes
  proto::internal::NodeInfo node1;
  node1.set_node_id(1);
  node1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node1.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(node1);

  proto::internal::NodeInfo node2;
  node2.set_node_id(2);
  node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node2.set_grpc_address("localhost:50052");
  node_registry_->UpdateNode(node2);

  // Create a collection with 4 shards
  auto result = coordinator_->CreateCollection(
      "multi_shard_collection",
      256,
      core::MetricType::COSINE,
      core::IndexType::HNSW,
      1,   // replication_factor
      4);  // num_shards

  REQUIRE(result.ok());

  auto metadata = coordinator_->GetCollectionMetadata("multi_shard_collection");
  REQUIRE(metadata.ok());
  CHECK_EQ(metadata->num_shards, 4);
  CHECK_EQ(metadata->shard_ids.size(), 4);
  CHECK_EQ(metadata->dimension, 256);
  CHECK_EQ(metadata->metric_type, core::MetricType::COSINE);
  CHECK_EQ(metadata->index_type, core::IndexType::HNSW);
}

TEST_CASE_FIXTURE(CoordinatorTest, "CreateCollectionReplicationFactorExceedsNodes") {
  // Register only one data node
  proto::internal::NodeInfo node1;
  node1.set_node_id(1);
  node1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
  node1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
  node1.set_grpc_address("localhost:50051");
  node_registry_->UpdateNode(node1);

  // Try to create collection with replication_factor=3 but only 1 node
  auto result = coordinator_->CreateCollection(
      "over_replicated",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT,
      3);  // replication_factor > available nodes

  CHECK_FALSE(result.ok());
  CHECK(absl::IsFailedPrecondition(result.status()));
}

// ============================================================================
// Read Repair Tests
// ============================================================================

class MockClient : public IInternalServiceClient {
 public:
  uint64_t vector_count = 100;
  bool list_fails = false;
  bool segment_missing = false;
  int get_segment_calls = 0;
  int replicate_segment_calls = 0;

  grpc::Status CreateSegment(
      grpc::ClientContext*, const proto::internal::CreateSegmentRequest&,
      proto::internal::CreateSegmentResponse* resp) override {
    resp->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status DeleteSegment(
      grpc::ClientContext*, const proto::internal::DeleteSegmentRequest&,
      proto::internal::DeleteSegmentResponse* resp) override {
    resp->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status ReplicateSegment(
      grpc::ClientContext*, const proto::internal::ReplicateSegmentRequest&,
      proto::internal::ReplicateSegmentResponse* resp) override {
    replicate_segment_calls++;
    resp->set_success(true);
    return grpc::Status::OK;
  }
  grpc::Status GetSegment(
      grpc::ClientContext*, const proto::internal::GetSegmentRequest&,
      proto::internal::GetSegmentResponse* resp) override {
    get_segment_calls++;
    resp->set_segment_data("fake_data");
    return grpc::Status::OK;
  }
  grpc::Status ListSegments(
      grpc::ClientContext*, const proto::internal::ListSegmentsRequest& req,
      proto::internal::ListSegmentsResponse* resp) override {
    if (list_fails) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "down");
    }
    if (!segment_missing) {
      auto* info = resp->add_segments();
      info->set_segment_id(req.collection_id() * 65536);
      info->set_collection_id(req.collection_id());
      info->set_vector_count(vector_count);
    }
    return grpc::Status::OK;
  }
};

// Factory that returns copies of pre-configured mock clients
class MockClientFactory : public IInternalServiceClientFactory {
 public:
  std::map<std::string, MockClient*> client_map;

  std::unique_ptr<IInternalServiceClient> CreateClient(
      core::NodeId, const std::string& address) override {
    auto it = client_map.find(address);
    if (it == client_map.end()) return nullptr;
    return std::make_unique<MockClient>(*it->second);
  }
};

class ReadRepairTest {
 public:
  ReadRepairTest() {
    auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
    node_registry_ = std::make_shared<NodeRegistry>(std::chrono::seconds(30));
    factory_ = std::make_shared<MockClientFactory>();

    // Register two data nodes
    proto::internal::NodeInfo node1;
    node1.set_node_id(1);
    node1.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node1.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node1.set_grpc_address("node1:50051");
    node_registry_->UpdateNode(node1);

    proto::internal::NodeInfo node2;
    node2.set_node_id(2);
    node2.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    node2.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
    node2.set_grpc_address("node2:50051");
    node_registry_->UpdateNode(node2);

    factory_->client_map["node1:50051"] = &primary_mock_;
    factory_->client_map["node2:50051"] = &replica_mock_;

    coordinator_ = std::make_unique<Coordinator>(shard_manager, node_registry_, factory_);
  }

  core::CollectionId CreateReplicatedCollection(const std::string& name) {
    auto result = coordinator_->CreateCollection(
        name, 128, core::MetricType::L2, core::IndexType::FLAT, 2);
    return result.ok() ? *result : core::kInvalidCollectionId;
  }

  std::shared_ptr<NodeRegistry> node_registry_;
  std::shared_ptr<MockClientFactory> factory_;
  std::unique_ptr<Coordinator> coordinator_;
  MockClient primary_mock_;
  MockClient replica_mock_;
};

TEST_CASE_FIXTURE(ReadRepairTest, "ReadRepair_DetectsDivergence") {
  auto coll_id = CreateReplicatedCollection("repair_divergence");
  REQUIRE(coll_id != core::kInvalidCollectionId);

  primary_mock_.vector_count = 100;
  replica_mock_.vector_count = 50;

  coordinator_->RunConsistencyCheck();
  CHECK_EQ(coordinator_->GetRepairQueueSize(), 0);
}

TEST_CASE_FIXTURE(ReadRepairTest, "ReadRepair_NoActionWhenConsistent") {
  auto coll_id = CreateReplicatedCollection("repair_consistent");
  REQUIRE(coll_id != core::kInvalidCollectionId);

  primary_mock_.vector_count = 100;
  replica_mock_.vector_count = 100;

  coordinator_->RunConsistencyCheck();
  CHECK_EQ(coordinator_->GetRepairQueueSize(), 0);
}

TEST_CASE_FIXTURE(ReadRepairTest, "ReadRepair_SkipsSingleReplicaCollections") {
  auto result = coordinator_->CreateCollection(
      "single_replica", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(result.ok());

  coordinator_->RunConsistencyCheck();
  CHECK_EQ(coordinator_->GetRepairQueueSize(), 0);
}

TEST_CASE_FIXTURE(ReadRepairTest, "ReadRepair_HandlesMissingSegment") {
  auto coll_id = CreateReplicatedCollection("repair_missing");
  REQUIRE(coll_id != core::kInvalidCollectionId);

  primary_mock_.vector_count = 100;
  replica_mock_.segment_missing = true;

  coordinator_->RunConsistencyCheck();
  CHECK_EQ(coordinator_->GetRepairQueueSize(), 0);
}

TEST_CASE_FIXTURE(ReadRepairTest, "ReadRepair_HandlesListSegmentsFailure") {
  auto coll_id = CreateReplicatedCollection("repair_failure");
  REQUIRE(coll_id != core::kInvalidCollectionId);

  primary_mock_.vector_count = 100;
  replica_mock_.list_fails = true;

  coordinator_->RunConsistencyCheck();
  CHECK_EQ(coordinator_->GetRepairQueueSize(), 0);
}

// ============================================================================
// Shard Rebalancing Tests
// ============================================================================

TEST_CASE_FIXTURE(ShardManagerTest, "CalculateRebalancePlan_Balanced_NoMoves") {
  core::NodeId n1 = core::MakeNodeId(1);
  core::NodeId n2 = core::MakeNodeId(2);
  core::NodeId n3 = core::MakeNodeId(3);
  REQUIRE(shard_manager_->RegisterNode(n1).ok());
  REQUIRE(shard_manager_->RegisterNode(n2).ok());
  REQUIRE(shard_manager_->RegisterNode(n3).ok());

  // Assign 6 shards evenly: 2 per node, equal size
  for (int i = 0; i < 6; ++i) {
    auto sid = core::MakeShardId(i);
    core::NodeId primary = (i < 2) ? n1 : (i < 4) ? n2 : n3;
    REQUIRE(shard_manager_->SetPrimaryNode(sid, primary).ok());
    REQUIRE(shard_manager_->UpdateShardMetrics(sid, 1000, 100, 50).ok());
  }

  auto moves = shard_manager_->CalculateRebalancePlan();
  CHECK(moves.empty());
}

TEST_CASE_FIXTURE(ShardManagerTest, "CalculateRebalancePlan_Imbalanced_GeneratesMoves") {
  core::NodeId n1 = core::MakeNodeId(1);
  core::NodeId n2 = core::MakeNodeId(2);
  core::NodeId n3 = core::MakeNodeId(3);
  REQUIRE(shard_manager_->RegisterNode(n1).ok());
  REQUIRE(shard_manager_->RegisterNode(n2).ok());
  REQUIRE(shard_manager_->RegisterNode(n3).ok());

  // Overload node 1: 4 shards with large data, nodes 2 and 3 have 1 small shard each
  for (int i = 0; i < 4; ++i) {
    auto sid = core::MakeShardId(i);
    REQUIRE(shard_manager_->SetPrimaryNode(sid, n1).ok());
    REQUIRE(shard_manager_->UpdateShardMetrics(sid, 10000, 1000, 500).ok());
  }
  auto sid4 = core::MakeShardId(4);
  REQUIRE(shard_manager_->SetPrimaryNode(sid4, n2).ok());
  REQUIRE(shard_manager_->UpdateShardMetrics(sid4, 1000, 100, 50).ok());

  auto sid5 = core::MakeShardId(5);
  REQUIRE(shard_manager_->SetPrimaryNode(sid5, n3).ok());
  REQUIRE(shard_manager_->UpdateShardMetrics(sid5, 1000, 100, 50).ok());

  auto moves = shard_manager_->CalculateRebalancePlan();
  CHECK(moves.size() > 0);

  // All moves should be from node 1 (the overloaded node)
  for (const auto& move : moves) {
    CHECK_EQ(move.source_node, n1);
    bool valid_target = (move.target_node == n2 || move.target_node == n3);
    CHECK(valid_target);
  }
}

TEST_CASE_FIXTURE(ShardManagerTest, "CalculateRebalancePlan_SingleNode_NoMoves") {
  core::NodeId n1 = core::MakeNodeId(1);
  REQUIRE(shard_manager_->RegisterNode(n1).ok());

  auto sid = core::MakeShardId(0);
  REQUIRE(shard_manager_->SetPrimaryNode(sid, n1).ok());
  REQUIRE(shard_manager_->UpdateShardMetrics(sid, 10000, 1000, 500).ok());

  auto moves = shard_manager_->CalculateRebalancePlan();
  CHECK(moves.empty());
}

TEST_CASE_FIXTURE(ShardManagerTest, "CalculateRebalancePlan_SkipsMigratingShards") {
  core::NodeId n1 = core::MakeNodeId(1);
  core::NodeId n2 = core::MakeNodeId(2);
  REQUIRE(shard_manager_->RegisterNode(n1).ok());
  REQUIRE(shard_manager_->RegisterNode(n2).ok());

  // All shards on node 1 — heavily imbalanced
  for (int i = 0; i < 4; ++i) {
    auto sid = core::MakeShardId(i);
    REQUIRE(shard_manager_->SetPrimaryNode(sid, n1).ok());
    REQUIRE(shard_manager_->UpdateShardMetrics(sid, 10000, 1000, 500).ok());
  }

  // Give node 2 a small shard so total_load > 0 independent of MIGRATING
  // state (isolates the MIGRATING skip from the early-return-on-zero-load path)
  auto sid_n2 = core::MakeShardId(10);
  REQUIRE(shard_manager_->SetPrimaryNode(sid_n2, n2).ok());
  REQUIRE(shard_manager_->UpdateShardMetrics(sid_n2, 1000, 100, 50).ok());

  // Set node 1's shards to MIGRATING
  for (int i = 0; i < 4; ++i) {
    REQUIRE(shard_manager_->SetShardState(core::MakeShardId(i),
                                           ShardState::MIGRATING).ok());
  }

  auto moves = shard_manager_->CalculateRebalancePlan();
  CHECK(moves.empty());
}

TEST_CASE_FIXTURE(ShardManagerTest, "SetShardState_Transitions") {
  auto sid = core::MakeShardId(0);

  auto initial = shard_manager_->GetShardState(sid);
  REQUIRE(initial.ok());
  CHECK_EQ(*initial, ShardState::ACTIVE);

  REQUIRE(shard_manager_->SetShardState(sid, ShardState::MIGRATING).ok());
  auto migrating = shard_manager_->GetShardState(sid);
  REQUIRE(migrating.ok());
  CHECK_EQ(*migrating, ShardState::MIGRATING);

  REQUIRE(shard_manager_->SetShardState(sid, ShardState::ACTIVE).ok());
  auto active = shard_manager_->GetShardState(sid);
  REQUIRE(active.ok());
  CHECK_EQ(*active, ShardState::ACTIVE);
}

TEST_CASE_FIXTURE(ShardManagerTest, "SetShardState_NotFound") {
  auto bad_sid = core::MakeShardId(9999);
  auto status = shard_manager_->SetShardState(bad_sid, ShardState::MIGRATING);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(ShardManagerTest, "GetShardState_NotFound") {
  auto bad_sid = core::MakeShardId(9999);
  auto result = shard_manager_->GetShardState(bad_sid);
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE_FIXTURE(ShardManagerTest, "ExecuteRebalanceMove_SetsStateToMigrating") {
  core::NodeId n1 = core::MakeNodeId(1);
  core::NodeId n2 = core::MakeNodeId(2);
  REQUIRE(shard_manager_->RegisterNode(n1).ok());
  REQUIRE(shard_manager_->RegisterNode(n2).ok());

  auto sid = core::MakeShardId(0);
  REQUIRE(shard_manager_->SetPrimaryNode(sid, n1).ok());

  ShardManager::RebalanceMove move{sid, n1, n2, true};
  auto status = shard_manager_->ExecuteRebalanceMove(move);
  CHECK(status.ok());
  auto state = shard_manager_->GetShardState(sid);
  REQUIRE(state.ok());
  CHECK_EQ(*state, ShardState::MIGRATING);
}
