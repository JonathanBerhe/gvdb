#include <gtest/gtest.h>
#include "consensus/timestamp_oracle.h"
#include "consensus/metadata_store.h"
#include "consensus/raft_node.h"
#include "core/types.h"

using namespace gvdb;
using namespace gvdb::consensus;

// ============================================================================
// TimestampOracle Tests
// ============================================================================

class TimestampOracleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tso_ = std::make_unique<TimestampOracle>();
  }

  std::unique_ptr<TimestampOracle> tso_;
};

TEST_F(TimestampOracleTest, MonotonicIncrease) {
  core::Timestamp ts1 = tso_->GetTimestamp();
  core::Timestamp ts2 = tso_->GetTimestamp();
  core::Timestamp ts3 = tso_->GetTimestamp();

  EXPECT_LT(ts1, ts2);
  EXPECT_LT(ts2, ts3);
}

TEST_F(TimestampOracleTest, BatchAllocation) {
  core::Timestamp base = tso_->AllocateBatch(100);

  // Next single timestamp should be after the batch
  core::Timestamp next = tso_->GetTimestamp();
  EXPECT_GE(next, base + 100);
}

TEST_F(TimestampOracleTest, CurrentTimestamp) {
  core::Timestamp current1 = tso_->CurrentTimestamp();
  core::Timestamp ts = tso_->GetTimestamp();
  core::Timestamp current2 = tso_->CurrentTimestamp();

  EXPECT_GE(ts, current1);
  EXPECT_GE(current2, ts);
}

TEST_F(TimestampOracleTest, UpdateIfHigher) {
  core::Timestamp current = tso_->CurrentTimestamp();

  // Update with higher value
  bool updated1 = tso_->UpdateIfHigher(current + 1000);
  EXPECT_TRUE(updated1);
  EXPECT_GE(tso_->CurrentTimestamp(), current + 1000);

  // Update with lower value (should fail)
  bool updated2 = tso_->UpdateIfHigher(current);
  EXPECT_FALSE(updated2);
}

TEST_F(TimestampOracleTest, Reset) {
  tso_->GetTimestamp();  // Advance timestamp

  core::Timestamp reset_value = 12345;
  tso_->Reset(reset_value);

  EXPECT_EQ(tso_->CurrentTimestamp(), reset_value);

  core::Timestamp next = tso_->GetTimestamp();
  EXPECT_EQ(next, reset_value);
}

// ============================================================================
// MetadataStore Tests
// ============================================================================

class MetadataStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    store_ = std::make_unique<MetadataStore>();
  }

  std::unique_ptr<MetadataStore> store_;
};

TEST_F(MetadataStoreTest, CreateCollection) {
  auto result = store_->CreateCollection(
      "test_collection", 128, core::MetricType::L2,
      core::IndexType::FLAT, 1, 1000);

  ASSERT_TRUE(result.ok());
  EXPECT_NE(*result, core::kInvalidCollectionId);

  // Verify created
  auto metadata = store_->GetCollectionMetadata("test_collection");
  ASSERT_TRUE(metadata.ok());
  EXPECT_EQ(metadata->collection_name, "test_collection");
  EXPECT_EQ(metadata->dimension, 128);
}

TEST_F(MetadataStoreTest, CreateDuplicateCollection) {
  auto result1 = store_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  ASSERT_TRUE(result1.ok());

  // Duplicate should fail
  auto result2 = store_->CreateCollection(
      "test", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  EXPECT_FALSE(result2.ok());
}

TEST_F(MetadataStoreTest, DropCollection) {
  auto create_result = store_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  ASSERT_TRUE(create_result.ok());

  auto drop_status = store_->DropCollection(*create_result, 2000);
  EXPECT_TRUE(drop_status.ok());

  // Verify dropped
  auto metadata = store_->GetCollectionMetadata("test");
  EXPECT_FALSE(metadata.ok());
}

TEST_F(MetadataStoreTest, ListCollections) {
  store_->CreateCollection("coll1", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  store_->CreateCollection("coll2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  store_->CreateCollection("coll3", 256, core::MetricType::INNER_PRODUCT, core::IndexType::IVF_FLAT, 1, 3000);

  auto collections = store_->ListCollections();
  EXPECT_EQ(collections.size(), 3);
}

TEST_F(MetadataStoreTest, RegisterNode) {
  cluster::NodeInfo node;
  node.node_id = core::MakeNodeId(1);
  node.type = cluster::NodeType::QUERY_NODE;
  node.status = cluster::NodeStatus::HEALTHY;
  node.address = "localhost:50051";

  auto status = store_->RegisterNode(node, 1000);
  EXPECT_TRUE(status.ok());

  // Verify registered
  auto info = store_->GetNodeInfo(node.node_id);
  ASSERT_TRUE(info.ok());
  EXPECT_EQ(info->address, "localhost:50051");
}

TEST_F(MetadataStoreTest, UnregisterNode) {
  cluster::NodeInfo node;
  node.node_id = core::MakeNodeId(1);
  node.type = cluster::NodeType::DATA_NODE;
  node.status = cluster::NodeStatus::HEALTHY;
  node.address = "localhost:50052";

  store_->RegisterNode(node, 1000);

  auto status = store_->UnregisterNode(node.node_id, 2000);
  EXPECT_TRUE(status.ok());

  // Verify unregistered
  auto info = store_->GetNodeInfo(node.node_id);
  EXPECT_FALSE(info.ok());
}

TEST_F(MetadataStoreTest, VersionTracking) {
  size_t v0 = store_->GetVersion();
  EXPECT_EQ(v0, 0);

  store_->CreateCollection("test1", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  size_t v1 = store_->GetVersion();
  EXPECT_EQ(v1, 1);

  store_->CreateCollection("test2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  size_t v2 = store_->GetVersion();
  EXPECT_EQ(v2, 2);
}

// ============================================================================
// RaftNode Tests
// ============================================================================

class RaftNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    RaftConfig config;
    config.node_id = 1;
    config.single_node_mode = true;
    config.data_dir = "/tmp/gvdb_test/raft";

    node_ = std::make_unique<RaftNode>(config);
  }

  void TearDown() override {
    if (node_ && node_->IsRunning()) {
      node_->Shutdown();
    }
  }

  std::unique_ptr<RaftNode> node_;
};

TEST_F(RaftNodeTest, StartShutdown) {
  EXPECT_FALSE(node_->IsRunning());

  auto start_status = node_->Start();
  EXPECT_TRUE(start_status.ok());
  EXPECT_TRUE(node_->IsRunning());

  auto shutdown_status = node_->Shutdown();
  EXPECT_TRUE(shutdown_status.ok());
  EXPECT_FALSE(node_->IsRunning());
}

TEST_F(RaftNodeTest, LeadershipSingleNode) {
  node_->Start();

  // In single-node mode, should immediately be leader
  EXPECT_TRUE(node_->IsLeader());
  EXPECT_EQ(node_->GetLeaderId(), 1);
}

TEST_F(RaftNodeTest, CreateCollectionThroughRaft) {
  node_->Start();

  auto result = node_->CreateCollection(
      "test", 128, core::MetricType::L2, core::IndexType::FLAT, 1);

  ASSERT_TRUE(result.ok());
  EXPECT_NE(*result, core::kInvalidCollectionId);

  // Verify through metadata query
  auto metadata = node_->GetCollectionMetadata("test");
  ASSERT_TRUE(metadata.ok());
  EXPECT_EQ(metadata->collection_name, "test");
}

TEST_F(RaftNodeTest, DropCollectionThroughRaft) {
  node_->Start();

  auto create_result = node_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  ASSERT_TRUE(create_result.ok());

  auto drop_status = node_->DropCollection(*create_result);
  EXPECT_TRUE(drop_status.ok());

  // Verify dropped
  auto metadata = node_->GetCollectionMetadata("test");
  EXPECT_FALSE(metadata.ok());
}

TEST_F(RaftNodeTest, OperationsFailWhenNotLeader) {
  // Don't start the node, so it won't be leader

  auto result = node_->CreateCollection(
      "test", 128, core::MetricType::L2, core::IndexType::FLAT, 1);

  EXPECT_FALSE(result.ok());
}

TEST_F(RaftNodeTest, TimestampOracleAccess) {
  node_->Start();

  auto* tso = node_->GetTimestampOracle();
  ASSERT_NE(tso, nullptr);

  core::Timestamp ts1 = tso->GetTimestamp();
  core::Timestamp ts2 = tso->GetTimestamp();

  EXPECT_LT(ts1, ts2);
}

TEST_F(RaftNodeTest, NodeRegistration) {
  node_->Start();

  cluster::NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(2);
  node_info.type = cluster::NodeType::QUERY_NODE;
  node_info.status = cluster::NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto status = node_->RegisterNode(node_info);
  EXPECT_TRUE(status.ok());

  auto info = node_->GetNodeInfo(node_info.node_id);
  ASSERT_TRUE(info.ok());
  EXPECT_EQ(info->address, "localhost:50051");
}

TEST_F(RaftNodeTest, CommittedOpCounter) {
  node_->Start();

  size_t initial = node_->GetCommittedOpCount();

  node_->CreateCollection("test1", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  EXPECT_EQ(node_->GetCommittedOpCount(), initial + 1);

  node_->CreateCollection("test2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  EXPECT_EQ(node_->GetCommittedOpCount(), initial + 2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
