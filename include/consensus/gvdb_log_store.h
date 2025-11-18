#pragma once

#include <libnuraft/log_store.hxx>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Forward declare RocksDB types
namespace rocksdb {
class DB;
class ColumnFamilyHandle;
}

namespace gvdb {
namespace consensus {

/**
 * GVDB implementation of NuRaft's log_store interface.
 *
 * Supports both in-memory (for development/testing) and persistent (RocksDB-backed)
 * storage modes.
 *
 * **In-Memory Mode**:
 * - Fast, no disk I/O
 * - Suitable for testing and development
 * - Data lost on restart
 *
 * **Persistent Mode (RocksDB)**:
 * - Durable across restarts
 * - Uses RocksDB column family for log entries
 * - Automatic crash recovery
 * - Production-ready
 *
 * Thread-safe: All operations are protected by mutex.
 */
class GvdbLogStore : public nuraft::log_store {
 public:
  /**
   * Create an in-memory log store (for testing).
   */
  GvdbLogStore();

  /**
   * Create a persistent log store backed by RocksDB.
   *
   * @param db_path Directory path for RocksDB data
   */
  explicit GvdbLogStore(const std::string& db_path);

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
  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
  log_entries(nuraft::ulong start, nuraft::ulong end) override;

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
  // Mutex protecting all data structures
  mutable std::mutex mutex_;

  // Storage mode
  bool persistent_mode_{false};

  // In-memory storage (used when persistent_mode_ == false)
  std::map<nuraft::ulong, nuraft::ptr<nuraft::log_entry>> logs_;

  // RocksDB storage (used when persistent_mode_ == true)
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::ColumnFamilyHandle* log_cf_{nullptr};  // Column family for log entries
  std::string db_path_;

  // Start index after compaction
  std::atomic<nuraft::ulong> start_idx_{1};

  // Next index for appends (only used in persistent mode)
  std::atomic<nuraft::ulong> next_idx_{1};

  // Helpers
  nuraft::ptr<nuraft::log_entry> get_entry_internal(nuraft::ulong index) const;
  bool init_rocksdb();
  void close_rocksdb();

  // RocksDB-specific helpers
  std::string make_log_key(nuraft::ulong index) const;
  bool write_entry_to_db(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry);
  nuraft::ptr<nuraft::log_entry> read_entry_from_db(nuraft::ulong index) const;
  bool delete_range_from_db(nuraft::ulong start_index, nuraft::ulong end_index);
};

}  // namespace consensus
}  // namespace gvdb
