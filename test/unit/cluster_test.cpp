#include <doctest/doctest.h>
#include "cluster/shard_manager.h"
#include "cluster/coordinator.h"
#include "cluster/node_registry.h"
#include "cluster/query_node.h"
#include "cluster/data_node.h"
#include "cluster/load_balancer.h"
#include "cluster/replication.h"
#include "core/types.h"
#include <chrono>

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// Shard Manager Tests
// ============================================================================

struct ShardManagerTest {
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

  shard_manager_->RegisterNode(node1);
  shard_manager_->RegisterNode(node2);

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

struct CoordinatorTest {
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
  // Register a data node first
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";
  coordinator_->RegisterNode(node_info);

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
  // Register a data node
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";
  coordinator_->RegisterNode(node_info);

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
  // Register a data node
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";
  coordinator_->RegisterNode(node_info);

  // Create multiple collections
  coordinator_->CreateCollection("coll1", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  coordinator_->CreateCollection("coll2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  coordinator_->CreateCollection("coll3", 256, core::MetricType::INNER_PRODUCT, core::IndexType::IVF_FLAT, 1);

  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 3);
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropCollection") {
  // Register a data node
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";
  coordinator_->RegisterNode(node_info);

  // Create collection
  auto create_result = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(create_result.ok());

  // Verify it exists
  auto metadata_result = coordinator_->GetCollectionMetadata("test_collection");
  REQUIRE(metadata_result.ok());

  // Drop the collection (NOTE: distributed cleanup requires RPC infrastructure,
  // tested via integration tests in test/e2e/test_crash_client.go)
  auto drop_status = coordinator_->DropCollection("test_collection");
  CHECK(drop_status.ok());

  // Verify it's gone from metadata
  auto metadata_after_drop = coordinator_->GetCollectionMetadata("test_collection");
  CHECK_FALSE(metadata_after_drop.ok());

  // Verify list shows 0 collections
  auto collections = coordinator_->ListCollections();
  CHECK_EQ(collections.size(), 0);
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropNonexistentCollection") {
  // Try to drop collection that doesn't exist
  auto status = coordinator_->DropCollection("nonexistent");
  CHECK_FALSE(status.ok());
}

TEST_CASE_FIXTURE(CoordinatorTest, "DropAndRecreateCollection") {
  // Register a data node
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::DATA_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";
  coordinator_->RegisterNode(node_info);

  // Create collection
  auto create_result1 = coordinator_->CreateCollection(
      "test_collection", 128, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(create_result1.ok());
  auto collection_id1 = *create_result1;

  // Drop it
  auto drop_status = coordinator_->DropCollection("test_collection");
  REQUIRE(drop_status.ok());

  // Recreate with same name but different params
  auto create_result2 = coordinator_->CreateCollection(
      "test_collection", 256, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  REQUIRE(create_result2.ok());
  auto collection_id2 = *create_result2;

  // Should get new collection ID
  CHECK_NE(collection_id1, collection_id2);

  // Verify new metadata
  auto metadata = coordinator_->GetCollectionMetadata("test_collection");
  REQUIRE(metadata.ok());
  CHECK_EQ(metadata->dimension, 256);
  CHECK_EQ(metadata->metric_type, core::MetricType::COSINE);
}

// ============================================================================
// Load Balancer Tests
// ============================================================================

struct LoadBalancerTest {
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

TEST_CASE_FIXTURE(LoadBalancerTest, "WeightedSelection") {
  load_balancer_->SetStrategy(LoadBalancingStrategy::WEIGHTED);

  std::vector<core::NodeId> nodes = {
      core::MakeNodeId(1),
      core::MakeNodeId(2)
  };

  // Set weights
  load_balancer_->SetNodeWeight(nodes[0], 3.0f);
  load_balancer_->SetNodeWeight(nodes[1], 1.0f);

  // Node 0 should be selected more often due to higher weight
  std::map<core::NodeId, int> counts;
  for (int i = 0; i < 1000; ++i) {
    auto result = load_balancer_->SelectNode(nodes);
    REQUIRE(result.ok());
    counts[*result]++;
  }

  // Node 0 should have roughly 3x more selections
  // Allow some variance due to randomness
  CHECK_GT(counts[nodes[0]], counts[nodes[1]]);
}

// ============================================================================
// Replication Manager Tests
// ============================================================================

struct ReplicationManagerTest {
  ReplicationManagerTest() {
    replication_manager_ = std::make_unique<ReplicationManager>();
  }

  std::unique_ptr<ReplicationManager> replication_manager_;
};

TEST_CASE_FIXTURE(ReplicationManagerTest, "ConsistencyLevels") {
  // This test would require mocked network operations
  // For now, just verify the replication manager can be constructed
  CHECK_NE(replication_manager_, nullptr);
}
