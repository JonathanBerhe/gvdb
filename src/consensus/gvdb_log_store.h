// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <libnuraft/log_store.hxx>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/statusor.h"

// Forward declare RocksDB types
namespace rocksdb {
class DB;
class ColumnFamilyHandle;
}  // namespace rocksdb

namespace gvdb {
namespace consensus {

/**
 * GVDB implementation of NuRaft's log_store interface, backed by RocksDB.
 *
 * - Durable across restarts
 * - Uses RocksDB column family for log entries
 * - Automatic crash recovery
 * - Production-ready
 *
 * Thread-safe: All operations are protected by mutex.
 */
class GvdbLogStore : public nuraft::log_store {
 public:
  // Opens or creates the RocksDB log store in `db_path`. Internal when
  // RocksDB cannot be opened.
  static absl::StatusOr<std::shared_ptr<GvdbLogStore>> Create(const std::string& db_path);

  ~GvdbLogStore() override;

  // Disable copy and move
  GvdbLogStore(const GvdbLogStore&) = delete;
  GvdbLogStore& operator=(const GvdbLogStore&) = delete;

  // ============================================================================
  // nuraft::log_store interface implementation
  // ============================================================================

  /**
   * Get the next available slot (last index + 1).
   * @return Next log index number (starts at 1)
   */
  nuraft::ulong next_slot() const override;

  /**
   * Get the start index of the log store.
   * Initially 1, but may increase after compaction.
   * @return Start log index
   */
  nuraft::ulong start_index() const override;

  /**
   * Get the last log entry.
   * @return Last log entry, or dummy entry if empty
   */
  nuraft::ptr<nuraft::log_entry> last_entry() const override;

  /**
   * Append a log entry.
   * @param entry Log entry to append
   * @return Index of the appended entry
   */
  nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;

  /**
   * Overwrite log entry at index and truncate all entries after it.
   * @param index Index to write at
   * @param entry Log entry to write
   */
  void write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) override;

  /**
   * Called after a batch of logs is written.
   * @param start Start index
   * @param cnt Number of entries written
   */
  void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;

  /**
   * Get log entries in range [start, end).
   * @param start Start index (inclusive)
   * @param end End index (exclusive)
   * @return Vector of log entries
   */
  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(nuraft::ulong start,
                                                                       nuraft::ulong end) override;

  /**
   * Get log entry at specific index.
   * @param index Log index
   * @return Log entry, or nullptr if not found
   */
  nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;

  /**
   * Get term for log entry at index.
   * @param index Log index
   * @return Term number, or 0 if index < start_index
   */
  nuraft::ulong term_at(nuraft::ulong index) override;

  /**
   * Pack log entries for transfer.
   * @param index Start index
   * @param cnt Number of entries to pack
   * @return Packed buffer
   */
  nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index, nuraft::int32 cnt) override;

  /**
   * Apply packed log entries.
   * @param index Start index
   * @param pack Packed buffer
   */
  void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;

  /**
   * Compact log by purging entries up to last_log_index (inclusive).
   * @param last_log_index Last index to purge
   * @return true on success
   */
  bool compact(nuraft::ulong last_log_index) override;

  /**
   * Flush all entries to durable storage.
   * @return true on success
   */
  bool flush() override;

 private:
  explicit GvdbLogStore(const std::string& db_path);

  // Mutex protecting all data structures
  mutable std::mutex mutex_;

  // RocksDB storage
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::ColumnFamilyHandle* log_cf_{nullptr};  // Column family for log entries
  std::string db_path_;

  // Start index after compaction
  std::atomic<nuraft::ulong> start_idx_{1};

  // Next index for appends
  std::atomic<nuraft::ulong> next_idx_{1};

  // Helpers
  nuraft::ptr<nuraft::log_entry> GetEntryInternal(nuraft::ulong index) const;
  bool InitRocksDb();
  void CloseRocksDb();

  // RocksDB-specific helpers
  std::string MakeLogKey(nuraft::ulong index) const;
  bool WriteEntryToDb(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry);
  nuraft::ptr<nuraft::log_entry> ReadEntryFromDb(nuraft::ulong index) const;
  bool DeleteRangeFromDb(nuraft::ulong start_index, nuraft::ulong end_index);
};

}  // namespace consensus
}  // namespace gvdb