#include <doctest/doctest.h>
#include "consensus/timestamp_oracle.h"
#include "consensus/metadata_store.h"
#include "consensus/raft_node.h"
#include "core/types.h"

using namespace gvdb;
using namespace gvdb::consensus;

// ============================================================================
// TimestampOracle Tests
// ============================================================================

class TimestampOracleTest {
 public:
  TimestampOracleTest() {
    tso_ = std::make_unique<TimestampOracle>();
  }

  std::unique_ptr<TimestampOracle> tso_;
};

TEST_CASE_FIXTURE(TimestampOracleTest, "MonotonicIncrease") {
  core::Timestamp ts1 = tso_->GetTimestamp();
  core::Timestamp ts2 = tso_->GetTimestamp();
  core::Timestamp ts3 = tso_->GetTimestamp();

  CHECK_LT(ts1, ts2);
  CHECK_LT(ts2, ts3);
}

TEST_CASE_FIXTURE(TimestampOracleTest, "BatchAllocation") {
  core::Timestamp base = tso_->AllocateBatch(100);

  // Next single timestamp should be after the batch
  core::Timestamp next = tso_->GetTimestamp();
  CHECK_GE(next, base + 100);
}

TEST_CASE_FIXTURE(TimestampOracleTest, "CurrentTimestamp") {
  core::Timestamp current1 = tso_->CurrentTimestamp();
  core::Timestamp ts = tso_->GetTimestamp();
  core::Timestamp current2 = tso_->CurrentTimestamp();

  CHECK_GE(ts, current1);
  CHECK_GE(current2, ts);
}

TEST_CASE_FIXTURE(TimestampOracleTest, "UpdateIfHigher") {
  core::Timestamp current = tso_->CurrentTimestamp();

  // Update with higher value
  bool updated1 = tso_->UpdateIfHigher(current + 1000);
  CHECK(updated1);
  CHECK_GE(tso_->CurrentTimestamp(), current + 1000);

  // Update with lower value (should fail)
  bool updated2 = tso_->UpdateIfHigher(current);
  CHECK_FALSE(updated2);
}

TEST_CASE_FIXTURE(TimestampOracleTest, "Reset") {
  tso_->GetTimestamp();  // Advance timestamp

  core::Timestamp reset_value = 12345;
  tso_->Reset(reset_value);

  CHECK_EQ(tso_->CurrentTimestamp(), reset_value);

  core::Timestamp next = tso_->GetTimestamp();
  CHECK_EQ(next, reset_value);
}

// ============================================================================
// MetadataStore Tests
// ============================================================================

class MetadataStoreTest {
 public:
  MetadataStoreTest() {
    store_ = std::make_unique<MetadataStore>();
  }

  std::unique_ptr<MetadataStore> store_;
};

TEST_CASE_FIXTURE(MetadataStoreTest, "CreateCollection") {
  auto result = store_->CreateCollection(
      "test_collection", 128, core::MetricType::L2,
      core::IndexType::FLAT, 1, 1000);

  REQUIRE(result.ok());
  CHECK_NE(*result, core::kInvalidCollectionId);

  // Verify created
  auto metadata = store_->GetCollectionMetadata("test_collection");
  REQUIRE(metadata.ok());
  CHECK_EQ(metadata->collection_name, "test_collection");
  CHECK_EQ(metadata->dimension, 128);
}

TEST_CASE_FIXTURE(MetadataStoreTest, "CreateDuplicateCollection") {
  auto result1 = store_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  REQUIRE(result1.ok());

  // Duplicate should fail
  auto result2 = store_->CreateCollection(
      "test", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  CHECK_FALSE(result2.ok());
}

TEST_CASE_FIXTURE(MetadataStoreTest, "DropCollection") {
  auto create_result = store_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  REQUIRE(create_result.ok());

  auto drop_status = store_->DropCollection(*create_result, 2000);
  CHECK(drop_status.ok());

  // Verify dropped
  auto metadata = store_->GetCollectionMetadata("test");
  CHECK_FALSE(metadata.ok());
}

TEST_CASE_FIXTURE(MetadataStoreTest, "ListCollections") {
  store_->CreateCollection("coll1", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  store_->CreateCollection("coll2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  store_->CreateCollection("coll3", 256, core::MetricType::INNER_PRODUCT, core::IndexType::IVF_FLAT, 1, 3000);

  auto collections = store_->ListCollections();
  CHECK_EQ(collections.size(), 3);
}

TEST_CASE_FIXTURE(MetadataStoreTest, "RegisterNode") {
  cluster::NodeInfo node;
  node.node_id = core::MakeNodeId(1);
  node.type = cluster::NodeType::QUERY_NODE;
  node.status = cluster::NodeStatus::HEALTHY;
  node.address = "localhost:50051";

  auto status = store_->RegisterNode(node, 1000);
  CHECK(status.ok());

  // Verify registered
  auto info = store_->GetNodeInfo(node.node_id);
  REQUIRE(info.ok());
  CHECK_EQ(info->address, "localhost:50051");
}

TEST_CASE_FIXTURE(MetadataStoreTest, "UnregisterNode") {
  cluster::NodeInfo node;
  node.node_id = core::MakeNodeId(1);
  node.type = cluster::NodeType::DATA_NODE;
  node.status = cluster::NodeStatus::HEALTHY;
  node.address = "localhost:50052";

  store_->RegisterNode(node, 1000);

  auto status = store_->UnregisterNode(node.node_id, 2000);
  CHECK(status.ok());

  // Verify unregistered
  auto info = store_->GetNodeInfo(node.node_id);
  CHECK_FALSE(info.ok());
}

TEST_CASE_FIXTURE(MetadataStoreTest, "VersionTracking") {
  size_t v0 = store_->GetVersion();
  CHECK_EQ(v0, 0);

  store_->CreateCollection("test1", 64, core::MetricType::L2, core::IndexType::FLAT, 1, 1000);
  size_t v1 = store_->GetVersion();
  CHECK_EQ(v1, 1);

  store_->CreateCollection("test2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1, 2000);
  size_t v2 = store_->GetVersion();
  CHECK_EQ(v2, 2);
}

// ============================================================================
// Raft Peer Parsing Tests (roadmap 0b.4)
// ============================================================================

TEST_CASE("ParseRaftPeerSpec accepts valid id:host:port") {
  auto p = ParseRaftPeerSpec("2:gvdb-coordinator-1.svc.cluster.local:8300");
  REQUIRE(p.ok());
  CHECK_EQ(p->id, 2);
  CHECK_EQ(p->endpoint, "gvdb-coordinator-1.svc.cluster.local:8300");
}

TEST_CASE("ParseRaftPeerSpec accepts simple host:port") {
  auto p = ParseRaftPeerSpec("3:localhost:9002");
  REQUIRE(p.ok());
  CHECK_EQ(p->id, 3);
  CHECK_EQ(p->endpoint, "localhost:9002");
}

TEST_CASE("ParseRaftPeerSpec rejects missing id") {
  auto p = ParseRaftPeerSpec(":localhost:9000");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects non-integer id") {
  auto p = ParseRaftPeerSpec("abc:localhost:9000");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects zero id") {
  auto p = ParseRaftPeerSpec("0:localhost:9000");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects negative id") {
  auto p = ParseRaftPeerSpec("-1:localhost:9000");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects endpoint without port") {
  auto p = ParseRaftPeerSpec("1:localhost");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects empty endpoint") {
  auto p = ParseRaftPeerSpec("1:");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects trailing colon with empty port") {
  auto p = ParseRaftPeerSpec("1:host:");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec rejects spec without any colon") {
  auto p = ParseRaftPeerSpec("just-a-name");
  CHECK_FALSE(p.ok());
}

TEST_CASE("ParseRaftPeerSpec tolerates leading/trailing whitespace") {
  auto p = ParseRaftPeerSpec("  2:host:8300  ");
  REQUIRE(p.ok());
  CHECK_EQ(p->id, 2);
  CHECK_EQ(p->endpoint, "host:8300");
}

TEST_CASE("ParseRaftPeerSpec rejects whitespace inside endpoint") {
  auto p = ParseRaftPeerSpec("1:my host:8300");
  CHECK_FALSE(p.ok());
}

TEST_CASE("PrepareRaftPeerList happy path with self present and fresh config") {
  std::vector<std::string> peers = {
      "1:host1:8300", "2:host2:8300", "3:host3:8300"};
  auto plan = PrepareRaftPeerList(2, peers, 1);
  REQUIRE(plan.ok());
  CHECK(plan->needs_seed);
  CHECK_EQ(plan->peers.size(), 3);
}

TEST_CASE("PrepareRaftPeerList rejects missing self in declared peers") {
  std::vector<std::string> peers = {
      "1:host1:8300", "2:host2:8300", "3:host3:8300"};
  auto plan = PrepareRaftPeerList(99, peers, 1);
  CHECK_FALSE(plan.ok());
}

TEST_CASE("PrepareRaftPeerList rejects duplicate peer ids") {
  std::vector<std::string> peers = {
      "1:host1:8300", "1:host2:8300", "3:host3:8300"};
  auto plan = PrepareRaftPeerList(1, peers, 1);
  CHECK_FALSE(plan.ok());
}

TEST_CASE("PrepareRaftPeerList propagates parser errors") {
  std::vector<std::string> peers = {"bogus"};
  auto plan = PrepareRaftPeerList(1, peers, 1);
  CHECK_FALSE(plan.ok());
}

TEST_CASE("PrepareRaftPeerList does not seed when persisted size > 1") {
  // Simulates restart: the persisted cluster_config already has 3 servers
  // from a prior run. We should trust it and not reseed.
  std::vector<std::string> peers = {
      "1:host1:8300", "2:host2:8300", "3:host3:8300"};
  auto plan = PrepareRaftPeerList(2, peers, 3);
  REQUIRE(plan.ok());
  CHECK_FALSE(plan->needs_seed);
}

TEST_CASE("PrepareRaftPeerList seeds when persisted size is 0 (corruption)") {
  std::vector<std::string> peers = {
      "1:host1:8300", "2:host2:8300", "3:host3:8300"};
  auto plan = PrepareRaftPeerList(1, peers, 0);
  REQUIRE(plan.ok());
  CHECK(plan->needs_seed);
}

TEST_CASE("PrepareRaftPeerList accepts empty peers (single-node mode)") {
  std::vector<std::string> peers;
  auto plan = PrepareRaftPeerList(1, peers, 1);
  REQUIRE(plan.ok());
  // With no declared peers the self-presence check is skipped and needs_seed
  // reflects the persisted size; caller ensures we're not in multi-node mode.
  CHECK(plan->peers.empty());
}

// ============================================================================
// RaftNode Tests
// ============================================================================

class RaftNodeTest {
 public:
  RaftNodeTest() {
    RaftConfig config;
    config.node_id = 1;
    config.single_node_mode = true;
    config.data_dir = "/tmp/gvdb_test/raft";

    node_ = std::make_unique<RaftNode>(config);
  }

  ~RaftNodeTest() {
    if (node_ && node_->IsRunning()) {
      node_->Shutdown();
    }
  }

  std::unique_ptr<RaftNode> node_;
};

TEST_CASE_FIXTURE(RaftNodeTest, "StartShutdown") {
  CHECK_FALSE(node_->IsRunning());

  auto start_status = node_->Start();
  CHECK(start_status.ok());
  CHECK(node_->IsRunning());

  auto shutdown_status = node_->Shutdown();
  CHECK(shutdown_status.ok());
  CHECK_FALSE(node_->IsRunning());
}

TEST_CASE_FIXTURE(RaftNodeTest, "LeadershipSingleNode") {
  node_->Start();

  // In single-node mode, should immediately be leader
  CHECK(node_->IsLeader());
  CHECK_EQ(node_->GetLeaderId(), 1);
}

TEST_CASE_FIXTURE(RaftNodeTest, "CreateCollectionThroughRaft") {
  node_->Start();

  auto result = node_->CreateCollection(
      "test", 128, core::MetricType::L2, core::IndexType::FLAT, 1);

  REQUIRE(result.ok());
  CHECK_NE(*result, core::kInvalidCollectionId);

  // Verify through metadata query
  auto metadata = node_->GetCollectionMetadata("test");
  REQUIRE(metadata.ok());
  CHECK_EQ(metadata->collection_name, "test");
}

TEST_CASE_FIXTURE(RaftNodeTest, "DropCollectionThroughRaft") {
  node_->Start();

  auto create_result = node_->CreateCollection(
      "test", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  REQUIRE(create_result.ok());

  auto drop_status = node_->DropCollection(*create_result);
  CHECK(drop_status.ok());

  // Verify dropped
  auto metadata = node_->GetCollectionMetadata("test");
  CHECK_FALSE(metadata.ok());
}

TEST_CASE_FIXTURE(RaftNodeTest, "OperationsFailWhenNotLeader") {
  // Don't start the node, so it won't be leader

  auto result = node_->CreateCollection(
      "test", 128, core::MetricType::L2, core::IndexType::FLAT, 1);

  CHECK_FALSE(result.ok());
}

TEST_CASE_FIXTURE(RaftNodeTest, "TimestampOracleAccess") {
  node_->Start();

  auto* tso = node_->GetTimestampOracle();
  REQUIRE_NE(tso, nullptr);

  core::Timestamp ts1 = tso->GetTimestamp();
  core::Timestamp ts2 = tso->GetTimestamp();

  CHECK_LT(ts1, ts2);
}

TEST_CASE_FIXTURE(RaftNodeTest, "NodeRegistration") {
  node_->Start();

  cluster::NodeInfo node_info;
  node_info.node_id = core::MakeNodeId(2);
  node_info.type = cluster::NodeType::QUERY_NODE;
  node_info.status = cluster::NodeStatus::HEALTHY;
  node_info.address = "localhost:50051";

  auto status = node_->RegisterNode(node_info);
  CHECK(status.ok());

  auto info = node_->GetNodeInfo(node_info.node_id);
  REQUIRE(info.ok());
  CHECK_EQ(info->address, "localhost:50051");
}

TEST_CASE_FIXTURE(RaftNodeTest, "CommittedOpCounter") {
  node_->Start();

  size_t initial = node_->GetCommittedOpCount();

  node_->CreateCollection("test1", 64, core::MetricType::L2, core::IndexType::FLAT, 1);
  CHECK_EQ(node_->GetCommittedOpCount(), initial + 1);

  node_->CreateCollection("test2", 128, core::MetricType::COSINE, core::IndexType::HNSW, 1);
  CHECK_EQ(node_->GetCommittedOpCount(), initial + 2);
}
