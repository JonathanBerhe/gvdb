#include "consensus/gvdb_log_store.h"
#include "consensus/gvdb_state_manager.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>

namespace gvdb {
namespace consensus {
namespace {

using nuraft::buffer;
using nuraft::log_entry;
using nuraft::log_val_type;
using nuraft::ptr;
using nuraft::ulong;

class GvdbLogStorePersistenceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create unique temporary directory for each test
    test_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_log_test_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
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
};

// Test 1: Basic persistence - write and read back
TEST_F(GvdbLogStorePersistenceTest, BasicPersistence) {
  std::string log_path = (test_dir_ / "logs").string();

  // Create log store and write entries
  {
    GvdbLogStore log_store(log_path);

    auto entry1 = create_log_entry(1, "data1");
    auto entry2 = create_log_entry(2, "data2");
    auto entry3 = create_log_entry(3, "data3");

    ulong idx1 = log_store.append(entry1);
    ulong idx2 = log_store.append(entry2);
    ulong idx3 = log_store.append(entry3);

    EXPECT_EQ(idx1, 1);
    EXPECT_EQ(idx2, 2);
    EXPECT_EQ(idx3, 3);

    // Verify we can read them back
    auto read1 = log_store.entry_at(1);
    auto read2 = log_store.entry_at(2);
    auto read3 = log_store.entry_at(3);

    ASSERT_NE(read1, nullptr);
    ASSERT_NE(read2, nullptr);
    ASSERT_NE(read3, nullptr);

    EXPECT_EQ(read1->get_term(), 1);
    EXPECT_EQ(read2->get_term(), 2);
    EXPECT_EQ(read3->get_term(), 3);
  }

  // Reopen and verify data persisted
  {
    GvdbLogStore log_store(log_path);

    auto read1 = log_store.entry_at(1);
    auto read2 = log_store.entry_at(2);
    auto read3 = log_store.entry_at(3);

    ASSERT_NE(read1, nullptr);
    ASSERT_NE(read2, nullptr);
    ASSERT_NE(read3, nullptr);

    EXPECT_EQ(read1->get_term(), 1);
    EXPECT_EQ(read2->get_term(), 2);
    EXPECT_EQ(read3->get_term(), 3);

    // Verify next_slot is correct
    EXPECT_EQ(log_store.next_slot(), 4);
  }
}

// Test 2: Crash recovery after write_at (log truncation)
TEST_F(GvdbLogStorePersistenceTest, TruncationPersistence) {
  std::string log_path = (test_dir_ / "logs").string();

  // Write entries and truncate
  {
    GvdbLogStore log_store(log_path);

    auto entry1 = create_log_entry(1, "data1");
    auto entry2 = create_log_entry(2, "data2");
    auto entry3 = create_log_entry(3, "data3");
    auto entry4 = create_log_entry(4, "data4");

    log_store.append(entry1);
    log_store.append(entry2);
    log_store.append(entry3);
    log_store.append(entry4);

    // Truncate from index 3
    auto new_entry = create_log_entry(5, "new_data_at_3");
    log_store.write_at(3, new_entry);

    // Verify truncation worked
    EXPECT_NE(log_store.entry_at(1), nullptr);
    EXPECT_NE(log_store.entry_at(2), nullptr);
    EXPECT_NE(log_store.entry_at(3), nullptr);
    EXPECT_EQ(log_store.entry_at(3)->get_term(), 5);
    EXPECT_EQ(log_store.entry_at(4), nullptr);  // Should be truncated
  }

  // Reopen and verify truncation persisted
  {
    GvdbLogStore log_store(log_path);

    EXPECT_NE(log_store.entry_at(1), nullptr);
    EXPECT_NE(log_store.entry_at(2), nullptr);
    EXPECT_NE(log_store.entry_at(3), nullptr);
    EXPECT_EQ(log_store.entry_at(3)->get_term(), 5);
    EXPECT_EQ(log_store.entry_at(4), nullptr);
  }
}

// Test 3: Compaction persistence
TEST_F(GvdbLogStorePersistenceTest, CompactionPersistence) {
  std::string log_path = (test_dir_ / "logs").string();

  // Write entries and compact
  {
    GvdbLogStore log_store(log_path);

    for (int i = 1; i <= 10; ++i) {
      auto entry = create_log_entry(i, "data" + std::to_string(i));
      log_store.append(entry);
    }

    // Compact up to index 5
    EXPECT_TRUE(log_store.compact(5));

    // Verify compaction
    EXPECT_EQ(log_store.start_index(), 6);
    EXPECT_EQ(log_store.entry_at(1), nullptr);  // Compacted
    EXPECT_EQ(log_store.entry_at(5), nullptr);  // Compacted
    EXPECT_NE(log_store.entry_at(6), nullptr);  // Still exists
    EXPECT_NE(log_store.entry_at(10), nullptr); // Still exists
  }

  // Reopen and verify compaction persisted
  {
    GvdbLogStore log_store(log_path);

    EXPECT_EQ(log_store.start_index(), 6);
    EXPECT_EQ(log_store.entry_at(1), nullptr);
    EXPECT_EQ(log_store.entry_at(5), nullptr);
    EXPECT_NE(log_store.entry_at(6), nullptr);
    EXPECT_NE(log_store.entry_at(10), nullptr);
  }
}

// Test 4: Large entries
TEST_F(GvdbLogStorePersistenceTest, LargeEntries) {
  std::string log_path = (test_dir_ / "logs").string();

  // Create a large entry (1MB)
  std::string large_data(1024 * 1024, 'X');

  {
    GvdbLogStore log_store(log_path);
    auto entry = create_log_entry(1, large_data);
    log_store.append(entry);
  }

  // Reopen and verify large entry persisted
  {
    GvdbLogStore log_store(log_path);
    auto read = log_store.entry_at(1);
    ASSERT_NE(read, nullptr);
    EXPECT_EQ(read->get_term(), 1);
    EXPECT_EQ(read->get_buf().size(), large_data.size());
  }
}

// Test 5: Multiple sequential writes
TEST_F(GvdbLogStorePersistenceTest, SequentialWrites) {
  std::string log_path = (test_dir_ / "logs").string();

  GvdbLogStore log_store(log_path);

  // Write 1000 entries
  for (int i = 1; i <= 1000; ++i) {
    auto entry = create_log_entry(i, std::to_string(i));
    ulong idx = log_store.append(entry);
    EXPECT_EQ(idx, static_cast<ulong>(i));
  }

  // Verify all entries
  for (int i = 1; i <= 1000; ++i) {
    auto entry = log_store.entry_at(i);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->get_term(), static_cast<ulong>(i));
  }

  // Flush to ensure durability
  EXPECT_TRUE(log_store.flush());
}

// State Manager Persistence Tests
class GvdbStateManagerPersistenceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_state_test_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::filesystem::path test_dir_;
};

// Test 1: State manager persistence
TEST_F(GvdbStateManagerPersistenceTest, BasicPersistence) {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Create state manager and save some state
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    // Create and save a server state
    auto srv_state = nuraft::cs_new<nuraft::srv_state>();
    srv_state->set_term(10);
    srv_state->set_voted_for(2);

    mgr.save_state(*srv_state);
  }

  // Reopen and verify state persisted
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    auto loaded_state = mgr.read_state();
    ASSERT_NE(loaded_state, nullptr);
    EXPECT_EQ(loaded_state->get_term(), 10);
    EXPECT_EQ(loaded_state->get_voted_for(), 2);
  }
}

// Test 2: Cluster config persistence
TEST_F(GvdbStateManagerPersistenceTest, ClusterConfigPersistence) {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Create multi-server cluster config
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    // Add more servers
    auto srv2 = nuraft::cs_new<nuraft::srv_config>(2, "localhost:9001");
    auto srv3 = nuraft::cs_new<nuraft::srv_config>(3, "localhost:9002");

    auto config = mgr.load_config();
    config->get_servers().push_back(srv2);
    config->get_servers().push_back(srv3);

    mgr.save_config(*config);
  }

  // Reopen and verify cluster config persisted
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    auto config = mgr.load_config();
    ASSERT_NE(config, nullptr);
    EXPECT_EQ(config->get_servers().size(), 3);
  }
}

// Integration Test: Full crash recovery scenario
TEST_F(GvdbStateManagerPersistenceTest, FullCrashRecovery) {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Simulate normal operation
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    // Write some log entries
    auto log_store = mgr.load_log_store();
    auto buf = buffer::alloc(10);
    for (int i = 0; i < 10; ++i) {
      buf->data_begin()[i] = static_cast<char>(i);
    }
    buf->pos(0);
    auto entry = nuraft::cs_new<log_entry>(5, buf);
    log_store->append(entry);

    // Update server state
    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(100);
    state->set_voted_for(1);
    mgr.save_state(*state);

    // Flush everything
    log_store->flush();
  }

  // Simulate crash and recovery
  {
    GvdbStateManager mgr(1, "localhost:9000", log_path, state_path);

    // Verify log entries recovered
    auto log_store = mgr.load_log_store();
    auto recovered_entry = log_store->entry_at(1);
    ASSERT_NE(recovered_entry, nullptr);
    EXPECT_EQ(recovered_entry->get_term(), 5);

    // Verify state recovered
    auto state = mgr.read_state();
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->get_term(), 100);
    EXPECT_EQ(state->get_voted_for(), 1);
  }
}

}  // namespace
}  // namespace consensus
}  // namespace gvdb
