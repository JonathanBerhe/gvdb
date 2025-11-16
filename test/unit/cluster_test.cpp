#include <gtest/gtest.h>
#include "cluster/shard_manager.h"
#include "cluster/coordinator.h"
#include "cluster/query_node.h"
#include "cluster/data_node.h"
#include "cluster/load_balancer.h"
#include "cluster/replication.h"
#include "core/types.h"

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// Shard Manager Tests
// ============================================================================

class ShardManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    shard_manager_ = std::make_unique<ShardManager>(16, ShardingStrategy::HASH);
  }

  std::unique_ptr<ShardManager> shard_manager_;
};

TEST_F(ShardManagerTest, AssignShardConsistency) {
  // Same vector ID should always map to same shard
  core::VectorId vid = core::MakeVectorId(12345);

  auto shard1 = shard_manager_->AssignShard(vid);
  auto shard2 = shard_manager_->AssignShard(vid);

  EXPECT_EQ(shard1, shard2);
}

TEST_F(ShardManagerTest, RegisterNode) {
  core::NodeId node1 = core::MakeNodeId(1);
  core::NodeId node2 = core::MakeNodeId(2);

  auto status1 = shard_manager_->RegisterNode(node1);
  EXPECT_TRUE(status1.ok());

  auto status2 = shard_manager_->RegisterNode(node2);
  EXPECT_TRUE(status2.ok());

  // Duplicate registration should fail
  auto status3 = shard_manager_->RegisterNode(node1);
  EXPECT_FALSE(status3.ok());
}

TEST_F(ShardManagerTest, ShardToNodeMapping) {
  core::NodeId node1 = core::MakeNodeId(1);
  core::NodeId node2 = core::MakeNodeId(2);

  shard_manager_->RegisterNode(node1);
  shard_manager_->RegisterNode(node2);

  core::ShardId shard = core::MakeShardId(0);

  // Set primary node
  auto status = shard_manager_->SetPrimaryNode(shard, node1);
  EXPECT_TRUE(status.ok());

  // Get primary node
  auto primary_result = shard_manager_->GetPrimaryNode(shard);
  ASSERT_TRUE(primary_result.ok());
  EXPECT_EQ(*primary_result, node1);

  // Add replica
  auto replica_status = shard_manager_->AddReplica(shard, node2);
  EXPECT_TRUE(replica_status.ok());

  // Get replicas
  auto replicas_result = shard_manager_->GetReplicaNodes(shard);
  ASSERT_TRUE(replicas_result.ok());
  EXPECT_EQ(replicas_result->size(), 1);
  EXPECT_EQ((*replicas_result)[0], node2);
}

TEST_F(ShardManagerTest, UpdateShardMetrics) {
  core::ShardId shard = core::MakeShardId(0);

  auto status = shard_manager_->UpdateShardMetrics(shard, 1024 * 1024, 1000, 5000);
  EXPECT_TRUE(status.ok());

  auto info_result = shard_manager_->GetShardInfo(shard);
  ASSERT_TRUE(info_result.ok());
  EXPECT_EQ(info_result->data_size_bytes, 1024 * 1024);
  EXPECT_EQ(info_result->vector_count, 1000);
  EXPECT_EQ(info_result->query_count, 5000);
}

// ============================================================================
// Coordinator Tests
// ============================================================================

class CoordinatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto shard_manager = std::make_shared<ShardManager>(16, ShardingStrategy::HASH);
    coordinator_ = std::make_unique<Coordinator>(shard_manager);
  }

  std::unique_ptr<Coordinator> coordinator_;
};

TEST_F(CoordinatorTest, RegisterNode) {
  NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(1);
  node_info.type = NodeType::QUERY_NODE;
  node_info.status = NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto status = coordinator_->RegisterNode(node_info);
  EXPECT_TRUE(status.ok());

  // Verify node was registered
  auto result = coordinator_->GetNodeInfo(node_info.node_id);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->address, "localhost:50051");
}

TEST_F(CoordinatorTest, CreateCollection) {
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

  ASSERT_TRUE(result.ok());
  EXPECT_NE(*result, core::kInvalidCollectionId);

  // Verify collection was created
  auto metadata_result = coordinator_->GetCollectionMetadata("test_collection");
  ASSERT_TRUE(metadata_result.ok());
  EXPECT_EQ(metadata_result->collection_name, "test_collection");
  EXPECT_EQ(metadata_result->dimension, 128);
}

TEST_F(CoordinatorTest, DuplicateCollectionName) {
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
  ASSERT_TRUE(result1.ok());

  // Try to create duplicate
  auto result2 = coordinator_->CreateCollection(
      "test_collection", 64, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  EXPECT_FALSE(result2.ok());
}

TEST_F(CoordinatorTest, ListCollections) {
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
  EXPECT_EQ(collections.size(), 3);
}

// ============================================================================
// Load Balancer Tests
// ============================================================================

class LoadBalancerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    load_balancer_ = std::make_unique<LoadBalancer>(LoadBalancingStrategy::ROUND_ROBIN);
  }

  std::unique_ptr<LoadBalancer> load_balancer_;
};

TEST_F(LoadBalancerTest, RoundRobinSelection) {
  std::vector<core::NodeId> nodes = {
      core::MakeNodeId(1),
      core::MakeNodeId(2),
      core::MakeNodeId(3)
  };

  // Select multiple times and verify round-robin
  std::map<core::NodeId, int> counts;
  for (int i = 0; i < 9; ++i) {
    auto result = load_balancer_->SelectNode(nodes);
    ASSERT_TRUE(result.ok());
    counts[*result]++;
  }

  // Each node should be selected 3 times
  for (const auto& node : nodes) {
    EXPECT_EQ(counts[node], 3);
  }
}

TEST_F(LoadBalancerTest, LeastConnectionsSelection) {
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
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result == nodes[1] || *result == nodes[2]);
}

TEST_F(LoadBalancerTest, ConnectionTracking) {
  core::NodeId node = core::MakeNodeId(1);

  EXPECT_EQ(load_balancer_->GetConnectionCount(node), 0);

  load_balancer_->IncrementConnections(node);
  EXPECT_EQ(load_balancer_->GetConnectionCount(node), 1);

  load_balancer_->IncrementConnections(node);
  EXPECT_EQ(load_balancer_->GetConnectionCount(node), 2);

  load_balancer_->DecrementConnections(node);
  EXPECT_EQ(load_balancer_->GetConnectionCount(node), 1);
}

TEST_F(LoadBalancerTest, WeightedSelection) {
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
    ASSERT_TRUE(result.ok());
    counts[*result]++;
  }

  // Node 0 should have roughly 3x more selections
  // Allow some variance due to randomness
  EXPECT_GT(counts[nodes[0]], counts[nodes[1]]);
}

// ============================================================================
// Replication Manager Tests
// ============================================================================

class ReplicationManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    replication_manager_ = std::make_unique<ReplicationManager>();
  }

  std::unique_ptr<ReplicationManager> replication_manager_;
};

TEST_F(ReplicationManagerTest, ConsistencyLevels) {
  // This test would require mocked network operations
  // For now, just verify the replication manager can be constructed
  EXPECT_NE(replication_manager_, nullptr);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
