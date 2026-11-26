#include <doctest/doctest.h>
#include "consensus/timestamp_oracle.h"
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

// ============================================================================
// Raft Peer Parsing Tests
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
      node_->Shutdown().IgnoreError();
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
  REQUIRE(node_->Start().ok());

  // In single-node mode, should immediately be leader
  CHECK(node_->IsLeader());
  CHECK_EQ(node_->GetLeaderId(), 1);
}

TEST_CASE_FIXTURE(RaftNodeTest, "TimestampOracleAccess") {
  REQUIRE(node_->Start().ok());

  auto* tso = node_->GetTimestampOracle();
  REQUIRE_NE(tso, nullptr);

  core::Timestamp ts1 = tso->GetTimestamp();
  core::Timestamp ts2 = tso->GetTimestamp();

  CHECK_LT(ts1, ts2);
}
