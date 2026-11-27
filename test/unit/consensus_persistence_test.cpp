// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/gvdb_log_store.h"
#include "consensus/gvdb_state_manager.h"

#include <doctest/doctest.h>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include "support/temp_dir.h"

namespace gvdb::consensus {
namespace {

using nuraft::buffer;
using nuraft::log_entry;
using nuraft::log_val_type;
using nuraft::ptr;
using nuraft::ulong;

ptr<log_entry> CreateLogEntry(ulong term, const std::string& data) {
  auto buf = buffer::alloc(data.size());
  std::memcpy(buf->data_begin(), data.data(), data.size());
  buf->pos(0);
  return nuraft::cs_new<log_entry>(term, buf, log_val_type::app_log);
}

class GvdbLogStorePersistenceTest {
 public:
  // A new folder for each test case, removed when the case ends.
  test::ScopedTempDir test_dir_{"gvdb_log_test"};
};

// Test 1: Basic persistence - write and read back
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "BasicPersistence") {
  std::string log_path = (test_dir_ / "logs").string();

  // Create log store and write entries
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    auto entry1 = CreateLogEntry(1, "data1");
    auto entry2 = CreateLogEntry(2, "data2");
    auto entry3 = CreateLogEntry(3, "data3");

    ulong idx1 = log_store.append(entry1);
    ulong idx2 = log_store.append(entry2);
    ulong idx3 = log_store.append(entry3);

    CHECK_EQ(idx1, 1);
    CHECK_EQ(idx2, 2);
    CHECK_EQ(idx3, 3);

    // The append counter moves in the same session, without a reopen.
    CHECK_EQ(log_store.next_slot(), 4);
    CHECK_EQ(log_store.start_index(), 1);

    // Verify we can read them back
    auto read1 = log_store.entry_at(1);
    auto read2 = log_store.entry_at(2);
    auto read3 = log_store.entry_at(3);

    REQUIRE_NE(read1, nullptr);
    REQUIRE_NE(read2, nullptr);
    REQUIRE_NE(read3, nullptr);

    CHECK_EQ(read1->get_term(), 1);
    CHECK_EQ(read2->get_term(), 2);
    CHECK_EQ(read3->get_term(), 3);
  }

  // Reopen and verify data persisted
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    auto read1 = log_store.entry_at(1);
    auto read2 = log_store.entry_at(2);
    auto read3 = log_store.entry_at(3);

    REQUIRE_NE(read1, nullptr);
    REQUIRE_NE(read2, nullptr);
    REQUIRE_NE(read3, nullptr);

    CHECK_EQ(read1->get_term(), 1);
    CHECK_EQ(read2->get_term(), 2);
    CHECK_EQ(read3->get_term(), 3);

    // Verify next_slot is correct
    CHECK_EQ(log_store.next_slot(), 4);

    // Nothing was compacted, so the store has no saved start index and
    // starts at 1.
    CHECK_EQ(log_store.start_index(), 1);
  }
}

// Appends after a reopen continue at the recovered next_slot and do not
// overwrite the entries written before the reopen.
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "AppendAfterReopen") {
  std::string log_path = (test_dir_ / "logs").string();
  constexpr ulong kBefore = 30;
  constexpr ulong kAfter = 20;

  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;
    for (ulong i = 1; i <= kBefore; ++i) {
      auto entry = CreateLogEntry(1, "before_" + std::to_string(i));
      log_store.append(entry);
    }
  }

  auto log_store_created = GvdbLogStore::Create(log_path);
  REQUIRE(log_store_created.ok());
  GvdbLogStore& log_store = **log_store_created;
  REQUIRE_EQ(log_store.next_slot(), kBefore + 1);

  for (ulong i = 1; i <= kAfter; ++i) {
    auto entry = CreateLogEntry(2, "after_" + std::to_string(i));
    ulong idx = log_store.append(entry);
    CHECK_EQ(idx, kBefore + i);
  }
  CHECK_EQ(log_store.next_slot(), kBefore + kAfter + 1);

  for (ulong i = 1; i <= kBefore; ++i) {
    INFO("entry " << i);
    auto entry = log_store.entry_at(i);
    REQUIRE_NE(entry, nullptr);
    CHECK_EQ(entry->get_term(), 1);
  }
  for (ulong i = kBefore + 1; i <= kBefore + kAfter; ++i) {
    INFO("entry " << i);
    auto entry = log_store.entry_at(i);
    REQUIRE_NE(entry, nullptr);
    CHECK_EQ(entry->get_term(), 2);
  }
}

// Test 2: Crash recovery after write_at (log truncation)
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "TruncationPersistence") {
  std::string log_path = (test_dir_ / "logs").string();

  // Write entries and truncate
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    auto entry1 = CreateLogEntry(1, "data1");
    auto entry2 = CreateLogEntry(2, "data2");
    auto entry3 = CreateLogEntry(3, "data3");
    auto entry4 = CreateLogEntry(4, "data4");

    log_store.append(entry1);
    log_store.append(entry2);
    log_store.append(entry3);
    log_store.append(entry4);

    // Truncate from index 3
    auto new_entry = CreateLogEntry(5, "new_data_at_3");
    log_store.write_at(3, new_entry);

    // Verify truncation worked
    CHECK_NE(log_store.entry_at(1), nullptr);
    CHECK_NE(log_store.entry_at(2), nullptr);
    CHECK_NE(log_store.entry_at(3), nullptr);
    CHECK_EQ(log_store.entry_at(3)->get_term(), 5);
    CHECK_EQ(log_store.entry_at(4), nullptr);  // Should be truncated
  }

  // Reopen and verify truncation persisted
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    CHECK_NE(log_store.entry_at(1), nullptr);
    CHECK_NE(log_store.entry_at(2), nullptr);
    CHECK_NE(log_store.entry_at(3), nullptr);
    CHECK_EQ(log_store.entry_at(3)->get_term(), 5);
    CHECK_EQ(log_store.entry_at(4), nullptr);
  }
}

// Test 3: Compaction persistence
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "CompactionPersistence") {
  std::string log_path = (test_dir_ / "logs").string();

  // Write entries and compact
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    for (int i = 1; i <= 10; ++i) {
      auto entry = CreateLogEntry(i, "data" + std::to_string(i));
      log_store.append(entry);
    }

    // Compact up to index 5
    CHECK(log_store.compact(5));

    // Verify compaction: every compacted index is gone, every later one stays.
    CHECK_EQ(log_store.start_index(), 6);
    for (ulong i = 1; i <= 5; ++i) {
      INFO("entry " << i);
      CHECK_EQ(log_store.entry_at(i), nullptr);
    }
    for (ulong i = 6; i <= 10; ++i) {
      INFO("entry " << i);
      CHECK_NE(log_store.entry_at(i), nullptr);
    }
  }

  // Reopen and verify compaction persisted
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;

    CHECK_EQ(log_store.start_index(), 6);
    for (ulong i = 1; i <= 5; ++i) {
      INFO("entry " << i);
      CHECK_EQ(log_store.entry_at(i), nullptr);
    }
    for (ulong i = 6; i <= 10; ++i) {
      INFO("entry " << i);
      CHECK_NE(log_store.entry_at(i), nullptr);
    }
  }
}

// Test 4: Large entries
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "LargeEntries") {
  std::string log_path = (test_dir_ / "logs").string();

  // Create a large entry (1MB)
  std::string large_data(size_t{1024} * 1024, 'X');

  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;
    auto entry = CreateLogEntry(1, large_data);
    log_store.append(entry);
  }

  // Reopen and verify large entry persisted
  {
    auto log_store_created = GvdbLogStore::Create(log_path);
    REQUIRE(log_store_created.ok());
    GvdbLogStore& log_store = **log_store_created;
    auto read = log_store.entry_at(1);
    REQUIRE_NE(read, nullptr);
    CHECK_EQ(read->get_term(), 1);
    CHECK_EQ(read->get_buf().size(), large_data.size());
  }
}

// Test 5: Multiple sequential writes
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "SequentialWrites") {
  std::string log_path = (test_dir_ / "logs").string();

  auto log_store_created = GvdbLogStore::Create(log_path);
  REQUIRE(log_store_created.ok());
  GvdbLogStore& log_store = **log_store_created;

  // Write 1000 entries
  for (int i = 1; i <= 1000; ++i) {
    auto entry = CreateLogEntry(i, std::to_string(i));
    ulong idx = log_store.append(entry);
    CHECK_EQ(idx, static_cast<ulong>(i));
  }

  // Verify all entries
  for (int i = 1; i <= 1000; ++i) {
    auto entry = log_store.entry_at(i);
    REQUIRE_NE(entry, nullptr);
    CHECK_EQ(entry->get_term(), static_cast<ulong>(i));
  }

  // Flush to ensure durability
  CHECK(log_store.flush());
}

// Test 6: last_entry() honors persistent mode (replication read path)
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "LastEntryPersistent") {
  std::string log_path = (test_dir_ / "logs").string();
  auto log_store_created = GvdbLogStore::Create(log_path);
  REQUIRE(log_store_created.ok());
  GvdbLogStore& log_store = **log_store_created;

  // Empty log → dummy sentinel with term 0
  auto empty = log_store.last_entry();
  REQUIRE_NE(empty, nullptr);
  CHECK_EQ(empty->get_term(), 0);

  // After append, last_entry must match the last appended entry,
  // the pre-fix bug returned the sentinel here because the method
  // only consulted the in-memory `logs_` map.
  auto e1 = CreateLogEntry(7, "first");
  auto e2 = CreateLogEntry(8, "last");
  log_store.append(e1);
  log_store.append(e2);

  auto last = log_store.last_entry();
  REQUIRE_NE(last, nullptr);
  CHECK_EQ(last->get_term(), 8);
}

// Test 7: log_entries() honors persistent mode (leader replication path)
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "LogEntriesPersistent") {
  std::string log_path = (test_dir_ / "logs").string();
  auto log_store_created = GvdbLogStore::Create(log_path);
  REQUIRE(log_store_created.ok());
  GvdbLogStore& log_store = **log_store_created;

  for (int i = 1; i <= 5; ++i) {
    auto e = CreateLogEntry(i, "v" + std::to_string(i));
    log_store.append(e);
  }

  // Range read spanning multiple entries must pull from RocksDB,
  // the pre-fix bug returned nullptr because it consulted `logs_`.
  auto entries = log_store.log_entries(2, 5);
  REQUIRE_NE(entries, nullptr);
  REQUIRE_EQ(entries->size(), 3);
  CHECK_EQ((*entries)[0]->get_term(), 2);
  CHECK_EQ((*entries)[1]->get_term(), 3);
  CHECK_EQ((*entries)[2]->get_term(), 4);

  // Missing-index case returns nullptr (compacted / beyond tail)
  CHECK_EQ(log_store.log_entries(100, 105), nullptr);
}

// Test 8: apply_pack() persists entries and advances next_idx_
TEST_CASE_FIXTURE(GvdbLogStorePersistenceTest, "ApplyPackPersistent") {
  std::string log_path = (test_dir_ / "logs").string();

  // Build a pack from a source store.
  ptr<buffer> packed;
  {
    auto src_created = GvdbLogStore::Create(log_path + "_src");
    REQUIRE(src_created.ok());
    GvdbLogStore& src = **src_created;
    for (int i = 1; i <= 3; ++i) {
      auto e = CreateLogEntry(10 + i, "pkt" + std::to_string(i));
      src.append(e);
    }
    packed = src.pack(1, 3);
    REQUIRE_NE(packed, nullptr);
  }

  // Apply into a fresh persistent store at index 1.
  auto dst_created = GvdbLogStore::Create(log_path);
  REQUIRE(dst_created.ok());
  GvdbLogStore& dst = **dst_created;
  dst.apply_pack(1, *packed);

  // next_slot must advance to one past the highest applied index,
  // the pre-fix bug wrote into `logs_` (which is unused in persistent
  // mode) and left next_idx_ untouched.
  CHECK_EQ(dst.next_slot(), 4);
  auto read = dst.entry_at(3);
  REQUIRE_NE(read, nullptr);
  CHECK_EQ(read->get_term(), 13);
}

// State Manager Persistence Tests
class GvdbStateManagerPersistenceTest {
 public:
  // A new folder for each test case, removed when the case ends.
  test::ScopedTempDir test_dir_{"gvdb_state_test"};
};

// Test 1: State manager persistence
TEST_CASE_FIXTURE(GvdbStateManagerPersistenceTest, "BasicPersistence") {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Create state manager and save some state
  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    // Create and save a server state
    auto srv_state = nuraft::cs_new<nuraft::srv_state>();
    srv_state->set_term(10);
    srv_state->set_voted_for(2);

    mgr.save_state(*srv_state);
  }

  // Reopen and verify state persisted
  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    auto loaded_state = mgr.read_state();
    REQUIRE_NE(loaded_state, nullptr);
    CHECK_EQ(loaded_state->get_term(), 10);
    CHECK_EQ(loaded_state->get_voted_for(), 2);
  }
}

// Test 2: Cluster config persistence
TEST_CASE_FIXTURE(GvdbStateManagerPersistenceTest, "ClusterConfigPersistence") {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Create multi-server cluster config
  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

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
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    auto config = mgr.load_config();
    REQUIRE_NE(config, nullptr);
    CHECK_EQ(config->get_servers().size(), 3);

    // With size 3, three successful lookups mean the IDs are exactly
    // {1, 2, 3}. get_server() returns nullptr for an unknown ID.
    auto srv1 = config->get_server(1);
    auto srv2 = config->get_server(2);
    auto srv3 = config->get_server(3);
    REQUIRE_NE(srv1, nullptr);
    REQUIRE_NE(srv2, nullptr);
    REQUIRE_NE(srv3, nullptr);
    CHECK_EQ(srv1->get_endpoint(), "localhost:9000");
    CHECK_EQ(srv2->get_endpoint(), "localhost:9001");
    CHECK_EQ(srv3->get_endpoint(), "localhost:9002");
  }
}

// A node can start on log and state directories that already exist but are
// empty, then reopen from what it saved there.
TEST_CASE_FIXTURE(GvdbStateManagerPersistenceTest, "StartOnExistingEmptyDirs") {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();
  std::filesystem::create_directories(log_path);
  std::filesystem::create_directories(state_path);
  constexpr int kNumEntries = 20;

  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    // Nothing saved yet: no server state, and a config with only this node.
    CHECK_EQ(mgr.read_state(), nullptr);
    auto config = mgr.load_config();
    REQUIRE_NE(config, nullptr);
    CHECK_EQ(config->get_servers().size(), 1);
    auto self = config->get_server(1);
    REQUIRE_NE(self, nullptr);
    CHECK_EQ(self->get_endpoint(), "localhost:9000");

    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(2, "localhost:9001"));
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(3, "localhost:9002"));
    mgr.save_config(*config);

    auto log_store = mgr.load_log_store();
    REQUIRE_NE(log_store, nullptr);
    CHECK_EQ(log_store->next_slot(), 1);
    for (int i = 1; i <= kNumEntries; ++i) {
      auto entry = CreateLogEntry(1, "entry_" + std::to_string(i));
      log_store->append(entry);
    }
  }

  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    auto log_store = mgr.load_log_store();
    CHECK_EQ(log_store->next_slot(), static_cast<ulong>(kNumEntries + 1));

    auto config = mgr.load_config();
    REQUIRE_NE(config, nullptr);
    CHECK_EQ(config->get_servers().size(), 3);
  }
}

// Integration Test: Full crash recovery scenario
TEST_CASE_FIXTURE(GvdbStateManagerPersistenceTest, "FullCrashRecovery") {
  std::string log_path = (test_dir_ / "logs").string();
  std::string state_path = (test_dir_ / "state").string();

  // Simulate normal operation
  {
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

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
    auto mgr_created = GvdbStateManager::Create(1, "localhost:9000", log_path, state_path);
    REQUIRE(mgr_created.ok());
    GvdbStateManager& mgr = **mgr_created;

    // Verify log entries recovered
    auto log_store = mgr.load_log_store();
    auto recovered_entry = log_store->entry_at(1);
    REQUIRE_NE(recovered_entry, nullptr);
    CHECK_EQ(recovered_entry->get_term(), 5);

    // Verify state recovered
    auto state = mgr.read_state();
    REQUIRE_NE(state, nullptr);
    CHECK_EQ(state->get_term(), 100);
    CHECK_EQ(state->get_voted_for(), 1);
  }
}

}  // namespace
}  // namespace gvdb::consensus
