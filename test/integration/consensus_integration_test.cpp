#include "consensus/gvdb_log_store.h"
#include "consensus/gvdb_state_manager.h"

#include <doctest/doctest.h>
#include <libnuraft/nuraft.hxx>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

namespace gvdb {
namespace consensus {
namespace integration {

using nuraft::buffer;
using nuraft::log_entry;
using nuraft::log_val_type;
using nuraft::ptr;
using nuraft::raft_params;
using nuraft::raft_server;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;
using nuraft::ulong;

// ============================================================================
// Phase 1: Single-Node Persistence Tests
// ============================================================================

class ConsensusSingleNodeTest {
 public:
  ConsensusSingleNodeTest() {
    // Create unique temporary directory for each test
    test_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_consensus_single_" +
                 std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(test_dir_);

    log_path_ = (test_dir_ / "logs").string();
    state_path_ = (test_dir_ / "state").string();
  }

  ~ConsensusSingleNodeTest() {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  ptr<log_entry> create_log_entry(ulong term, const std::string& data) {
    auto buf = buffer::alloc(data.size());
    std::memcpy(buf->data_begin(), data.data(), data.size());
    buf->pos(0);
    return nuraft::cs_new<log_entry>(term, buf, log_val_type::app_log);
  }

  std::filesystem::path test_dir_;
  std::string log_path_;
  std::string state_path_;
};

// Test 1: Basic restart recovery
TEST_CASE_FIXTURE(ConsensusSingleNodeTest, "BasicRestartRecovery") {
  const int kNumEntries = 50;

  // Phase 1: Create server and append entries
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    // Append test entries
    for (int i = 1; i <= kNumEntries; ++i) {
      auto entry = create_log_entry(1, "entry_" + std::to_string(i));
      ulong idx = log_store->append(entry);
      CHECK_EQ(idx, static_cast<ulong>(i));
    }

    // Verify entries exist
    CHECK_EQ(log_store->next_slot(), kNumEntries + 1);

    // Update server state
    auto server_state = nuraft::cs_new<srv_state>();
    server_state->set_term(5);
    server_state->set_voted_for(1);
    state_mgr->save_state(*server_state);

    log_store->flush();
  }

  // Phase 2: Restart and verify recovery
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    // Verify all entries recovered
    CHECK_EQ(log_store->next_slot(), kNumEntries + 1);
    CHECK_EQ(log_store->start_index(), 1);

    for (int i = 1; i <= kNumEntries; ++i) {
      auto entry = log_store->entry_at(i);
      INFO("Entry " << i << " not found");
      REQUIRE_NE(entry, nullptr);
      CHECK_EQ(entry->get_term(), 1);
    }

    // Verify server state recovered
    auto server_state = state_mgr->read_state();
    REQUIRE_NE(server_state, nullptr);
    CHECK_EQ(server_state->get_term(), 5);
    CHECK_EQ(server_state->get_voted_for(), 1);
  }
}

// Test 2: Crash recovery (simulated power loss)
TEST_CASE_FIXTURE(ConsensusSingleNodeTest, "CrashRecovery") {
  const int kEntriesBeforeCrash = 30;
  const int kEntriesAfterCrash = 20;

  // Phase 1: Normal operation
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    for (int i = 1; i <= kEntriesBeforeCrash; ++i) {
      auto entry = create_log_entry(1, "before_crash_" + std::to_string(i));
      log_store->append(entry);
    }

    log_store->flush();
    // Simulate crash (destructor runs, closing DB properly)
  }

  // Phase 2: Recovery after crash
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    // Verify pre-crash data recovered
    CHECK_EQ(log_store->next_slot(), kEntriesBeforeCrash + 1);

    // Continue normal operation
    for (int i = 1; i <= kEntriesAfterCrash; ++i) {
      auto entry = create_log_entry(2, "after_crash_" + std::to_string(i));
      log_store->append(entry);
    }

    CHECK_EQ(log_store->next_slot(), kEntriesBeforeCrash + kEntriesAfterCrash + 1);

    // Verify all entries present
    for (int i = 1; i <= kEntriesBeforeCrash; ++i) {
      auto entry = log_store->entry_at(i);
      REQUIRE_NE(entry, nullptr);
      CHECK_EQ(entry->get_term(), 1);
    }

    for (int i = 1; i <= kEntriesAfterCrash; ++i) {
      auto entry = log_store->entry_at(kEntriesBeforeCrash + i);
      REQUIRE_NE(entry, nullptr);
      CHECK_EQ(entry->get_term(), 2);
    }
  }
}

// Test 3: Log compaction with recovery
TEST_CASE_FIXTURE(ConsensusSingleNodeTest, "LogCompactionRecovery") {
  const int kTotalEntries = 100;
  const int kCompactUpTo = 60;

  // Phase 1: Fill log and compact
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    // Append entries
    for (int i = 1; i <= kTotalEntries; ++i) {
      auto entry = create_log_entry(1, "entry_" + std::to_string(i));
      log_store->append(entry);
    }

    // Compact up to index 60
    CHECK(log_store->compact(kCompactUpTo));
    CHECK_EQ(log_store->start_index(), kCompactUpTo + 1);

    // Verify compacted entries are gone
    for (int i = 1; i <= kCompactUpTo; ++i) {
      CHECK_EQ(log_store->entry_at(i), nullptr);
    }

    // Verify remaining entries exist
    for (int i = kCompactUpTo + 1; i <= kTotalEntries; ++i) {
      CHECK_NE(log_store->entry_at(i), nullptr);
    }

    log_store->flush();
  }

  // Phase 2: Restart and verify compaction persisted
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto log_store = state_mgr->load_log_store();

    // Verify start index recovered
    CHECK_EQ(log_store->start_index(), kCompactUpTo + 1);

    // Verify compacted entries still gone
    for (int i = 1; i <= kCompactUpTo; ++i) {
      CHECK_EQ(log_store->entry_at(i), nullptr);
    }

    // Verify remaining entries still exist
    for (int i = kCompactUpTo + 1; i <= kTotalEntries; ++i) {
      auto entry = log_store->entry_at(i);
      INFO("Entry " << i << " not found after restart");
      REQUIRE_NE(entry, nullptr);
    }
  }
}

// Test 4: Configuration persistence
TEST_CASE_FIXTURE(ConsensusSingleNodeTest, "ConfigurationPersistence") {
  // Phase 1: Create multi-server configuration
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);

    auto srv2 = nuraft::cs_new<srv_config>(2, "localhost:9001");
    auto srv3 = nuraft::cs_new<srv_config>(3, "localhost:9002");

    auto config = state_mgr->load_config();
    config->get_servers().push_back(srv2);
    config->get_servers().push_back(srv3);

    state_mgr->save_config(*config);
  }

  // Phase 2: Restart and verify configuration
  {
    auto state_mgr = nuraft::cs_new<GvdbStateManager>(1, "localhost:9000", log_path_, state_path_);
    auto config = state_mgr->load_config();

    REQUIRE_NE(config, nullptr);
    CHECK_EQ(config->get_servers().size(), 3);

    // Verify server IDs
    std::vector<int> server_ids;
    for (const auto& srv : config->get_servers()) {
      server_ids.push_back(srv->get_id());
    }
    std::sort(server_ids.begin(), server_ids.end());

    CHECK_EQ(server_ids[0], 1);
    CHECK_EQ(server_ids[1], 2);
    CHECK_EQ(server_ids[2], 3);
  }
}

// ============================================================================
// Phase 2: Multi-Node Cluster Tests (In-Process)
// ============================================================================

class ConsensusMultiNodeTest {
 public:
  struct RaftNode {
    int node_id;
    std::string endpoint;
    std::string log_path;
    std::string state_path;
    ptr<GvdbStateManager> state_mgr;
  };

  ConsensusMultiNodeTest() {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_consensus_multi_" +
                 std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(test_dir_);
  }

  ~ConsensusMultiNodeTest() {
    // Clean up all nodes
    nodes_.clear();

    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  RaftNode create_node(int node_id) {
    RaftNode node;
    node.node_id = node_id;
    node.endpoint = "localhost:" + std::to_string(9000 + node_id);
    node.log_path = (test_dir_ / ("node" + std::to_string(node_id)) / "logs").string();
    node.state_path = (test_dir_ / ("node" + std::to_string(node_id)) / "state").string();

    std::filesystem::create_directories(node.log_path);
    std::filesystem::create_directories(node.state_path);

    node.state_mgr = nuraft::cs_new<GvdbStateManager>(node_id, node.endpoint, node.log_path,
                                                       node.state_path);

    return node;
  }

  ptr<log_entry> create_log_entry(ulong term, const std::string& data) {
    auto buf = buffer::alloc(data.size());
    std::memcpy(buf->data_begin(), data.data(), data.size());
    buf->pos(0);
    return nuraft::cs_new<log_entry>(term, buf, log_val_type::app_log);
  }

  std::filesystem::path test_dir_;
  std::vector<RaftNode> nodes_;
};

// Test 1: Cluster formation and restart
TEST_CASE_FIXTURE(ConsensusMultiNodeTest, "ClusterFormationAndRestart") {
  const int kNumNodes = 3;
  const int kNumEntries = 20;

  // Phase 1: Create 3-node cluster
  for (int i = 1; i <= kNumNodes; ++i) {
    auto node = create_node(i);

    // Configure all nodes to know about each other
    auto config = node.state_mgr->load_config();
    for (int j = 1; j <= kNumNodes; ++j) {
      if (j != i) {
        auto peer = nuraft::cs_new<srv_config>(j, "localhost:" + std::to_string(9000 + j));
        config->get_servers().push_back(peer);
      }
    }
    node.state_mgr->save_config(*config);

    // Append some entries
    auto log_store = node.state_mgr->load_log_store();
    for (int e = 1; e <= kNumEntries; ++e) {
      auto entry = create_log_entry(1, "node" + std::to_string(i) + "_entry" + std::to_string(e));
      log_store->append(entry);
    }
    log_store->flush();

    nodes_.push_back(std::move(node));
  }

  // Phase 2: Restart all nodes
  // First, explicitly close all existing nodes to release RocksDB locks
  nodes_.clear();

  // Now recreate nodes
  for (int i = 1; i <= kNumNodes; ++i) {
    auto node = create_node(i);

    // Verify log recovered
    auto log_store = node.state_mgr->load_log_store();
    CHECK_EQ(log_store->next_slot(), kNumEntries + 1);

    // Verify configuration recovered
    auto config = node.state_mgr->load_config();
    CHECK_EQ(config->get_servers().size(), kNumNodes);

    nodes_.push_back(std::move(node));
  }
}

// Test 2: Leader crash and recovery
TEST_CASE_FIXTURE(ConsensusMultiNodeTest, "LeaderCrashRecovery") {
  const int kNumNodes = 3;

  // Create nodes with initial data
  for (int i = 1; i <= kNumNodes; ++i) {
    auto node = create_node(i);
    auto log_store = node.state_mgr->load_log_store();

    // Leader (node 1) has more entries
    int num_entries = (i == 1) ? 50 : 40;
    for (int e = 1; e <= num_entries; ++e) {
      auto entry = create_log_entry(1, "entry_" + std::to_string(e));
      log_store->append(entry);
    }

    // Node 1 is the leader
    if (i == 1) {
      auto state = nuraft::cs_new<srv_state>();
      state->set_term(5);
      node.state_mgr->save_state(*state);
    }

    log_store->flush();
    nodes_.push_back(std::move(node));
  }

  // Simulate leader crash and recovery
  {
    // "Crash" node 1 (leader)
    nodes_[0].state_mgr.reset();

    // "Recover" node 1
    auto recovered = create_node(1);
    auto log_store = recovered.state_mgr->load_log_store();

    // Verify leader's log recovered
    CHECK_EQ(log_store->next_slot(), 51);

    // Verify state recovered
    auto state = recovered.state_mgr->read_state();
    REQUIRE_NE(state, nullptr);
    CHECK_EQ(state->get_term(), 5);

    nodes_[0] = std::move(recovered);
  }
}

// Test 3: Majority crash recovery
TEST_CASE_FIXTURE(ConsensusMultiNodeTest, "MajorityCrashRecovery") {
  const int kNumNodes = 5;
  const int kNumEntries = 30;

  // Create 5-node cluster
  for (int i = 1; i <= kNumNodes; ++i) {
    auto node = create_node(i);
    auto log_store = node.state_mgr->load_log_store();

    for (int e = 1; e <= kNumEntries; ++e) {
      auto entry = create_log_entry(1, "entry_" + std::to_string(e));
      log_store->append(entry);
    }

    log_store->flush();
    nodes_.push_back(std::move(node));
  }

  // Simulate crash of 3 out of 5 nodes
  std::vector<int> crashed_nodes = {1, 2, 3};
  for (int node_id : crashed_nodes) {
    nodes_[node_id - 1].state_mgr.reset();
  }

  // Recover crashed nodes
  for (int node_id : crashed_nodes) {
    auto recovered = create_node(node_id);
    auto log_store = recovered.state_mgr->load_log_store();

    // Verify data recovered
    CHECK_EQ(log_store->next_slot(), kNumEntries + 1);

    for (int e = 1; e <= kNumEntries; ++e) {
      auto entry = log_store->entry_at(e);
      REQUIRE_NE(entry, nullptr);
    }

    nodes_[node_id - 1] = std::move(recovered);
  }
}

}  // namespace integration
}  // namespace consensus
}  // namespace gvdb
