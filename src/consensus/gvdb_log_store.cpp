#include "consensus/gvdb_log_store.h"

#include "utils/logger.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cassert>
#include <filesystem>

namespace gvdb {
namespace consensus {

using nuraft::buffer;
using nuraft::log_entry;
using nuraft::ptr;
using nuraft::ulong;

GvdbLogStore::GvdbLogStore() : persistent_mode_(false) {
  utils::Logger::Instance().Info("GvdbLogStore initialized (in-memory mode)");
}

GvdbLogStore::GvdbLogStore(const std::string& db_path)
    : persistent_mode_(true), db_path_(db_path) {
  if (!init_rocksdb()) {
    throw std::runtime_error("Failed to initialize RocksDB at " + db_path);
  }
  utils::Logger::Instance().Info("GvdbLogStore initialized (persistent mode, path={})", db_path_);
}

GvdbLogStore::~GvdbLogStore() {
  if (persistent_mode_) {
    close_rocksdb();
  }
}

ulong GvdbLogStore::next_slot() const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (persistent_mode_) {
    return next_idx_.load(std::memory_order_acquire);
  } else {
    if (logs_.empty()) {
      return start_idx_.load(std::memory_order_acquire);
    }
    return logs_.rbegin()->first + 1;
  }
}

ulong GvdbLogStore::start_index() const {
  return start_idx_.load(std::memory_order_acquire);
}

ptr<log_entry> GvdbLogStore::last_entry() const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (logs_.empty()) {
    // Return dummy entry with term 0 and null value
    return nuraft::cs_new<log_entry>(0, nuraft::buffer::alloc(0));
  }

  return logs_.rbegin()->second;
}

ulong GvdbLogStore::append(ptr<log_entry>& entry) {
  std::lock_guard<std::mutex> lock(mutex_);

  ulong index;

  if (persistent_mode_) {
    // Use atomic counter for next index
    index = next_idx_.fetch_add(1, std::memory_order_acq_rel);
    if (!write_entry_to_db(index, entry)) {
      utils::Logger::Instance().Error("Failed to persist log entry at index {}", index);
      // Rollback counter on failure
      next_idx_.fetch_sub(1, std::memory_order_acq_rel);
      throw std::runtime_error("Failed to write log entry to RocksDB");
    }
  } else {
    if (logs_.empty()) {
      index = start_idx_.load(std::memory_order_acquire);
    } else {
      index = logs_.rbegin()->first + 1;
    }
    logs_[index] = entry;
  }

  return index;
}

void GvdbLogStore::write_at(ulong index, ptr<log_entry>& entry) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (persistent_mode_) {
    // Delete entries from index+1 to infinity (truncate)
    // RocksDB doesn't have "infinity", so we delete up to a large number
    ulong max_index = index + 100000;  // Reasonable upper bound
    delete_range_from_db(index + 1, max_index);

    // Write the entry at index
    if (!write_entry_to_db(index, entry)) {
      throw std::runtime_error("Failed to write log entry to RocksDB");
    }

    // Update next_idx to point after this entry
    next_idx_.store(index + 1, std::memory_order_release);
  } else {
    // Truncate all entries after index (including index itself will be overwritten)
    auto it = logs_.upper_bound(index);
    if (it != logs_.end()) {
      logs_.erase(it, logs_.end());
    }

    // Write the entry at index
    logs_[index] = entry;
  }
}

void GvdbLogStore::end_of_append_batch(ulong start, ulong cnt) {
  // Optional hook for batch write optimization
  // For in-memory store, nothing to do
  (void)start;
  (void)cnt;
}

ptr<std::vector<ptr<log_entry>>> GvdbLogStore::log_entries(ulong start, ulong end) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto result = nuraft::cs_new<std::vector<ptr<log_entry>>>();
  result->reserve(end - start);

  for (ulong i = start; i < end; ++i) {
    auto it = logs_.find(i);
    if (it == logs_.end()) {
      // Entry not found - log has been compacted or doesn't exist
      utils::Logger::Instance().Error(
          "Log entry at index {} not found (start={}, end={})", i, start, end);
      return nullptr;  // Indicate error
    }
    result->push_back(it->second);
  }

  return result;
}

ptr<log_entry> GvdbLogStore::entry_at(ulong index) {
  std::lock_guard<std::mutex> lock(mutex_);
  return get_entry_internal(index);
}

ulong GvdbLogStore::term_at(ulong index) {
  std::lock_guard<std::mutex> lock(mutex_);

  ulong start = start_idx_.load(std::memory_order_acquire);
  if (index < start) {
    return 0;  // Compacted, return 0
  }

  auto entry = get_entry_internal(index);
  if (!entry) {
    // Should not happen for valid index >= start_index
    utils::Logger::Instance().Error("term_at: entry at index {} not found", index);
    return 0;
  }

  return entry->get_term();
}

ptr<buffer> GvdbLogStore::pack(ulong index, nuraft::int32 cnt) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Calculate total size needed
  size_t total_size = 0;
  std::vector<ptr<buffer>> entry_buffers;
  entry_buffers.reserve(cnt);

  for (nuraft::int32 i = 0; i < cnt; ++i) {
    auto entry = get_entry_internal(index + i);
    if (!entry) {
      utils::Logger::Instance().Error("pack: entry at index {} not found", index + i);
      return nullptr;
    }

    auto buf = entry->serialize();
    total_size += buf->size();
    entry_buffers.push_back(buf);
  }

  // Pack all entries into single buffer
  // Format: [entry1_size][entry1_data][entry2_size][entry2_data]...
  auto packed = buffer::alloc(total_size + cnt * sizeof(nuraft::int32));

  for (const auto& entry_buf : entry_buffers) {
    nuraft::int32 size = static_cast<nuraft::int32>(entry_buf->size());
    packed->put(size);
    packed->put(*entry_buf);
  }

  packed->pos(0);  // Reset position for reading
  return packed;
}

void GvdbLogStore::apply_pack(ulong index, buffer& pack) {
  std::lock_guard<std::mutex> lock(mutex_);

  pack.pos(0);  // Reset position

  ulong current_index = index;
  while (pack.pos() < pack.size()) {
    // Read entry size
    nuraft::int32 entry_size = pack.get_int();

    // Read entry data
    ptr<buffer> entry_buf = buffer::alloc(entry_size);
    pack.get(entry_buf);

    // Deserialize and store
    ptr<log_entry> entry = log_entry::deserialize(*entry_buf);
    logs_[current_index++] = entry;
  }
}

bool GvdbLogStore::compact(ulong last_log_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (persistent_mode_) {
    // Delete entries from start_idx to last_log_index (inclusive)
    ulong start = start_idx_.load(std::memory_order_acquire);
    if (!delete_range_from_db(start, last_log_index + 1)) {
      return false;
    }

    // Update and persist start_idx
    ulong new_start = last_log_index + 1;
    start_idx_.store(new_start, std::memory_order_release);

    // Write start_idx to DB metadata
    std::string start_idx_key = "start_idx";
    std::string start_idx_value(sizeof(ulong), '\0');
    std::memcpy(&start_idx_value[0], &new_start, sizeof(ulong));

    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    rocksdb::Status status = db_->Put(write_opts, start_idx_key, start_idx_value);

    if (!status.ok()) {
      utils::Logger::Instance().Error("Failed to persist start_idx: {}", status.ToString());
      return false;
    }

    utils::Logger::Instance().Info(
        "Compacted log up to index {} (new start index: {})",
        last_log_index, new_start);
  } else {
    // Remove all entries up to and including last_log_index
    auto it = logs_.upper_bound(last_log_index);
    logs_.erase(logs_.begin(), it);

    // Update start index
    ulong new_start = last_log_index + 1;
    start_idx_.store(new_start, std::memory_order_release);

    utils::Logger::Instance().Info(
        "Compacted log up to index {} (new start index: {})",
        last_log_index, new_start);
  }

  return true;
}

bool GvdbLogStore::flush() {
  if (persistent_mode_) {
    // RocksDB WAL is fsynced on every write (write_opts.sync = true)
    // So flush is a no-op, but we could call FlushWAL() for extra safety
    rocksdb::FlushOptions flush_opts;
    flush_opts.wait = true;
    rocksdb::Status status = db_->Flush(flush_opts, log_cf_);
    return status.ok();
  } else {
    // For in-memory store, always durable (no-op)
    return true;
  }
}

// Private helper
ptr<log_entry> GvdbLogStore::get_entry_internal(ulong index) const {
  // Caller must hold mutex
  if (persistent_mode_) {
    return read_entry_from_db(index);
  } else {
    auto it = logs_.find(index);
    if (it == logs_.end()) {
      return nullptr;
    }
    return it->second;
  }
}

// RocksDB initialization
bool GvdbLogStore::init_rocksdb() {
  namespace fs = std::filesystem;

  // Create directory if it doesn't exist
  fs::path db_dir(db_path_);
  if (!fs::exists(db_dir)) {
    std::error_code ec;
    if (!fs::create_directories(db_dir, ec)) {
      utils::Logger::Instance().Error(
          "Failed to create RocksDB directory: {} (error: {})", db_path_, ec.message());
      return false;
    }
  }

  // Configure RocksDB options
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.wal_recovery_mode = rocksdb::WALRecoveryMode::kAbsoluteConsistency;
  options.max_open_files = 256;

  // Optimize for Raft log workload (sequential writes, range scans)
  options.OptimizeLevelStyleCompaction();
  options.IncreaseParallelism(2);

  // Column families: default and log_entries
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors{
      {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
      {"log_entries", rocksdb::ColumnFamilyOptions()}};

  // Try to open existing DB first
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  rocksdb::DB* db_ptr = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path_, cf_descriptors, &cf_handles, &db_ptr);

  if (!status.ok()) {
    // DB doesn't exist or column families missing, create new one
    utils::Logger::Instance().Info("Creating new RocksDB instance at {}", db_path_);

    // Create DB with default CF only
    status = rocksdb::DB::Open(options, db_path_, &db_ptr);
    if (!status.ok()) {
      utils::Logger::Instance().Error("Failed to create RocksDB: {}", status.ToString());
      return false;
    }

    // Create log_entries column family
    rocksdb::ColumnFamilyHandle* cf_handle;
    status = db_ptr->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "log_entries", &cf_handle);
    if (!status.ok()) {
      utils::Logger::Instance().Error("Failed to create log_entries CF: {}", status.ToString());
      delete db_ptr;
      return false;
    }

    delete cf_handle;
    delete db_ptr;

    // Re-open with both column families
    status = rocksdb::DB::Open(options, db_path_, cf_descriptors, &cf_handles, &db_ptr);
    if (!status.ok()) {
      utils::Logger::Instance().Error("Failed to re-open RocksDB: {}", status.ToString());
      return false;
    }
  }

  db_.reset(db_ptr);

  // cf_handles[0] is default CF, cf_handles[1] is log_entries CF
  if (cf_handles.size() >= 2) {
    delete cf_handles[0];  // Don't need default CF handle
    log_cf_ = cf_handles[1];
  } else {
    utils::Logger::Instance().Error("Missing log_entries column family");
    return false;
  }

  // Read start_idx from metadata (stored in default CF)
  std::string start_idx_value;
  status = db_->Get(rocksdb::ReadOptions(), "start_idx", &start_idx_value);
  if (status.ok() && start_idx_value.size() == sizeof(ulong)) {
    ulong stored_start_idx;
    std::memcpy(&stored_start_idx, start_idx_value.data(), sizeof(ulong));
    start_idx_.store(stored_start_idx, std::memory_order_release);
    utils::Logger::Instance().Info("Recovered start_idx: {}", stored_start_idx);
  } else {
    // First initialization, start_idx remains 1
    utils::Logger::Instance().Info("New log store, start_idx: 1");
  }

  // Find the last log entry to initialize next_idx_
  // Use reverse iterator to find max key
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions(), log_cf_));
  it->SeekToLast();

  if (it->Valid()) {
    // Found last entry - extract index from key
    ulong last_index;
    std::memcpy(&last_index, it->key().data(), sizeof(ulong));
    next_idx_.store(last_index + 1, std::memory_order_release);
    utils::Logger::Instance().Info("Recovered next_idx: {} (last entry: {})", last_index + 1, last_index);
  } else {
    // No entries, use start_idx
    ulong start = start_idx_.load(std::memory_order_acquire);
    next_idx_.store(start, std::memory_order_release);
    utils::Logger::Instance().Info("No existing entries, next_idx: {}", start);
  }

  return true;
}

void GvdbLogStore::close_rocksdb() {
  if (log_cf_) {
    db_->DestroyColumnFamilyHandle(log_cf_);
    log_cf_ = nullptr;
  }
  db_.reset();
  utils::Logger::Instance().Info("RocksDB closed");
}

std::string GvdbLogStore::make_log_key(ulong index) const {
  // Use fixed-width binary key for efficient range scans
  std::string key(sizeof(ulong), '\0');
  std::memcpy(&key[0], &index, sizeof(ulong));
  return key;
}

bool GvdbLogStore::write_entry_to_db(ulong index, ptr<log_entry>& entry) {
  if (!db_ || !log_cf_) {
    return false;
  }

  // Serialize log entry
  auto serialized = entry->serialize();

  std::string key = make_log_key(index);
  rocksdb::Slice value(reinterpret_cast<const char*>(serialized->data_begin()),
                        serialized->size());

  rocksdb::WriteOptions write_opts;
  write_opts.sync = true;  // fsync for durability

  rocksdb::Status status = db_->Put(write_opts, log_cf_, key, value);

  if (!status.ok()) {
    utils::Logger::Instance().Error("Failed to write log entry {}: {}", index, status.ToString());
    return false;
  }

  return true;
}

ptr<log_entry> GvdbLogStore::read_entry_from_db(ulong index) const {
  if (!db_ || !log_cf_) {
    return nullptr;
  }

  std::string key = make_log_key(index);
  std::string value;

  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), log_cf_, key, &value);

  if (!status.ok()) {
    if (!status.IsNotFound()) {
      utils::Logger::Instance().Error("Failed to read log entry {}: {}", index, status.ToString());
    }
    return nullptr;
  }

  // Deserialize log entry
  auto buffer_ptr = buffer::alloc(value.size());
  std::memcpy(buffer_ptr->data_begin(), value.data(), value.size());
  buffer_ptr->pos(0);

  return log_entry::deserialize(*buffer_ptr);
}

bool GvdbLogStore::delete_range_from_db(ulong start_index, ulong end_index) {
  if (!db_ || !log_cf_) {
    return false;
  }

  std::string start_key = make_log_key(start_index);
  std::string end_key = make_log_key(end_index);

  rocksdb::WriteOptions write_opts;
  write_opts.sync = true;

  rocksdb::Status status = db_->DeleteRange(write_opts, log_cf_, start_key, end_key);

  if (!status.ok()) {
    utils::Logger::Instance().Error("Failed to delete range [{}, {}): {}",
                                     start_index, end_index, status.ToString());
    return false;
  }

  return true;
}

}  // namespace consensus
}  // namespace gvdb
