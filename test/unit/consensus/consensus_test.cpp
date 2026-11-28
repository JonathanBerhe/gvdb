// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include "consensus/raft_node.h"
#include "consensus/timestamp_oracle.h"
#include "core/types.h"
#include "support/temp_dir.h"

namespace {

using namespace gvdb;
using namespace gvdb::consensus;

// ============================================================================
// TimestampOracle Tests
// ============================================================================

class TimestampOracleTest {
 public:
  TimestampOracleTest() { tso_ = std::make_unique<TimestampOracle>(); }

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

TEST_CASE("ParseRaftPeerSpec") {
  struct Row {
    const char* spec;
    bool ok;
    int id;
    const char* endpoint;
  };
  const std::vector<Row> rows = {
      {"2:gvdb-coordinator-1.svc.cluster.local:8300", true, 2,
       "gvdb-coordinator-1.svc.cluster.local:8300"},
      {"3:localhost:9002", true, 3, "localhost:9002"},
      {"  2:host:8300  ", true, 2, "host:8300"},  // surrounding whitespace is trimmed
      {":localhost:9000", false, 0, ""},          // missing id
      {"abc:localhost:9000", false, 0, ""},       // non-integer id
      {"0:localhost:9000", false, 0, ""},         // zero id
      {"-1:localhost:9000", false, 0, ""},        // negative id
      {"1:localhost", false, 0, ""},              // endpoint without port
      {"1:", false, 0, ""},                       // empty endpoint
      {"1:host:", false, 0, ""},                  // trailing colon with empty port
      {"just-a-name", false, 0, ""},              // no colon at all
      {"1:my host:8300", false, 0, ""},           // whitespace inside the endpoint
  };
  for (const Row& row : rows) {
    CAPTURE(row.spec);
    auto p = ParseRaftPeerSpec(row.spec);
    REQUIRE_EQ(p.ok(), row.ok);
    if (row.ok) {
      CHECK_EQ(p->id, row.id);
      CHECK_EQ(p->endpoint, row.endpoint);
    }
  }
}

TEST_CASE("PrepareRaftPeerList") {
  const std::vector<std::string> three = {"1:host1:8300", "2:host2:8300", "3:host3:8300"};
  struct Row {
    const char* name;
    int self_id;
    std::vector<std::string> peers;
    size_t persisted_size;
    bool ok;
    bool needs_seed;    // checked only when ok
    size_t peer_count;  // checked only when ok
  };
  const std::vector<Row> rows = {
      {"self present, fresh config", 2, three, 1, true, true, 3},
      {"self missing from the declared peers", 99, three, 1, false, false, 0},
      {"duplicate peer ids",
       1,
       {"1:host1:8300", "1:host2:8300", "3:host3:8300"},
       1,
       false,
       false,
       0},
      {"parser error", 1, {"bogus"}, 1, false, false, 0},
      // A restart: the persisted cluster config already has 3 servers.
      {"no seed when the persisted size is above 1", 2, three, 3, true, false, 3},
      {"seed when the persisted size is 0 (corruption)", 1, three, 0, true, true, 3},
      // Single-node mode: without declared peers the self check is skipped.
      {"empty peers", 1, {}, 1, true, true, 0},
  };
  for (const Row& row : rows) {
    CAPTURE(row.name);
    auto plan = PrepareRaftPeerList(row.self_id, row.peers, row.persisted_size);
    REQUIRE_EQ(plan.ok(), row.ok);
    if (row.ok) {
      CHECK_EQ(plan->needs_seed, row.needs_seed);
      CHECK_EQ(plan->peers.size(), row.peer_count);
    }
  }
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
    config.data_dir = (data_dir_ / "raft").string();

    auto created = RaftNode::Create(config);
    REQUIRE(created.ok());
    node_ = std::move(*created);
  }

  ~RaftNodeTest() {
    if (node_ && node_->IsRunning()) {
      node_->Shutdown().IgnoreError();
    }
  }

  // Declared first, so the folder is removed after the node.
  test::ScopedTempDir data_dir_{"gvdb_raft_node_test"};
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

TEST_CASE("RaftNode Start fails when coordinator gRPC peers are set without a peer client") {
  gvdb::test::ScopedTempDir dir("raft_no_peer_client");
  RaftConfig config;
  config.node_id = 1;
  config.single_node_mode = false;
  config.peers = {"1:127.0.0.1:1"};
  config.coordinator_grpc_peers = {"127.0.0.1:1"};
  config.data_dir = dir.str();

  auto node = RaftNode::Create(config);
  REQUIRE(node.ok());
  auto status = (*node)->Start();
  CHECK_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  CHECK_FALSE((*node)->IsRunning());
}

}  // namespace
