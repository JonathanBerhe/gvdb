// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_BACKUP_MANAGER_H_
#define GVDB_STORAGE_BACKUP_MANAGER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "storage/backup_manifest.h"
#include "storage/object_store.h"
#include "storage/segment_store.h"
#include "utils/thread_pool.h"

namespace gvdb {
namespace storage {

// ============================================================================
// Backup / Restore types (C++ mirror of the proto enums + oneof)
// ============================================================================

enum class BackupState {
  PENDING = 0,
  RUNNING = 1,
  COMPLETED = 2,
  FAILED = 3,
  CANCELLED = 4,
};

struct S3BackupTarget {
  std::string bucket;
  std::string prefix;
};

struct LocalBackupTarget {
  std::string path;
};

// Mirrors proto BackupTarget oneof; std::monostate represents "unset".
using BackupTarget =
    std::variant<std::monostate, S3BackupTarget, LocalBackupTarget>;

// ============================================================================
// BackupJob / RestoreJob — shared atomic-progress shape with BulkImporter
// ============================================================================

struct BackupJob {
  std::string id;
  std::string collection_name;
  uint32_t collection_id = 0;
  BackupTarget target;
  std::atomic<BackupState> state{BackupState::PENDING};
  std::atomic<uint32_t> shards_total{0};
  std::atomic<uint32_t> shards_completed{0};
  std::atomic<uint64_t> bytes_uploaded{0};
  std::atomic<bool> cancelled{false};
  mutable std::mutex error_mutex;
  std::string error_message;
  std::string manifest_uri;  // populated on success
  std::chrono::steady_clock::time_point start_time;

  void SetError(const std::string& msg) {
    std::lock_guard<std::mutex> lk(error_mutex);
    error_message = msg;
  }
  std::string GetError() const {
    std::lock_guard<std::mutex> lk(error_mutex);
    return error_message;
  }
};

struct BackupJobStatus {
  std::string backup_id;
  BackupState state = BackupState::PENDING;
  uint32_t shards_total = 0;
  uint32_t shards_completed = 0;
  uint64_t bytes_uploaded = 0;
  std::string error_message;
  float elapsed_seconds = 0.0f;
  std::string manifest_uri;
};

struct RestoreJob {
  std::string id;
  BackupTarget source;
  std::string backup_id;
  uint32_t target_collection_id = 0;
  std::atomic<BackupState> state{BackupState::PENDING};
  std::atomic<uint32_t> shards_total{0};
  std::atomic<uint32_t> shards_completed{0};
  std::atomic<bool> cancelled{false};
  mutable std::mutex error_mutex;
  std::string error_message;
  std::chrono::steady_clock::time_point start_time;

  void SetError(const std::string& msg) {
    std::lock_guard<std::mutex> lk(error_mutex);
    error_message = msg;
  }
  std::string GetError() const {
    std::lock_guard<std::mutex> lk(error_mutex);
    return error_message;
  }
};

struct RestoreJobStatus {
  std::string restore_id;
  BackupState state = BackupState::PENDING;
  uint32_t shards_total = 0;
  uint32_t shards_completed = 0;
  std::string error_message;
  float elapsed_seconds = 0.0f;
};

// ============================================================================
// BackupManagerOptions
// ============================================================================
//
// One BackupManager (and one RestoreManager) is created per server. It
// is configured at startup; per-request behavior is parameterized by the
// BackupTarget on each call.
//
// `default_object_store` is the IObjectStore the server uses for S3
// targets. The pointer is owned by the caller (typically the same
// object the segment manager uses) and must outlive the manager. May
// be nullptr — in that case requests with S3BackupTarget are rejected.
//
// `local_root_allowlist` gates LocalBackupTarget requests: a request
// whose path does not lie under this root is rejected. Empty disables
// local targets entirely.
//
// `flushed_segments_root` points at the directory where the segment
// manager flushes sealed segments — i.e. the value such that flushed
// segment N lives under <flushed_segments_root>/segment_N/.
struct BackupManagerOptions {
  IObjectStore* default_object_store = nullptr;
  std::string s3_bucket;            // server's configured bucket (for validation)
  std::string local_root_allowlist;
  std::string flushed_segments_root;
  std::string tmp_dir = "/tmp/gvdb-backup";
  int max_concurrent_jobs = 2;
  std::string gvdb_version;
  uint32_t node_id = 0;
};

struct RestoreManagerOptions {
  IObjectStore* default_object_store = nullptr;
  std::string s3_bucket;
  std::string local_root_allowlist;
  std::string staging_dir = "/tmp/gvdb-restore";
  int max_concurrent_jobs = 2;
};

// ============================================================================
// Per-shard executor results (used by the RPC handler in commits 3/4)
// ============================================================================

struct ShardBackupSegmentResult {
  uint64_t segment_id = 0;
  bool is_growing = false;
  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;
  std::vector<std::string> object_keys;
};

struct ShardBackupResult {
  std::vector<ShardBackupSegmentResult> segments;
  std::string shard_manifest_key;
  uint64_t bytes_uploaded = 0;
};

// ============================================================================
// BackupManager
// ============================================================================
//
// Orchestrates one or more shards' worth of backup work into a single
// IObjectStore target. In commit 2 the manager handles single-shard
// (single-node) backups inline; multi-shard fan-out via the
// Coordinator is added in a later commit and re-uses the same per-shard
// executor (`RunShardBackup`).
class BackupManager {
 public:
  explicit BackupManager(BackupManagerOptions opts);
  ~BackupManager() = default;

  BackupManager(const BackupManager&) = delete;
  BackupManager& operator=(const BackupManager&) = delete;

  // Schedule a single-shard backup asynchronously. Returns the assigned
  // backup_id immediately; caller polls GetStatus.
  // `requested_backup_id` lets the caller supply an idempotency key; if
  // a job for the same id already exists in PENDING/RUNNING/COMPLETED
  // state, the existing id is returned.
  [[nodiscard]] core::StatusOr<std::string> StartBackupSingleShard(
      const std::string& collection_name,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type,
      uint32_t num_shards,
      uint32_t replication_factor,
      core::ShardId shard_id,
      uint64_t primary_term,
      const BackupTarget& target,
      std::shared_ptr<ISegmentStore> segment_store,
      const std::string& requested_backup_id = "");

  // Per-shard executor used by the multi-shard orchestration path. The
  // caller (Coordinator) supplies a callable that runs the equivalent
  // of RunShardBackup on a remote data-node (via the BackupShard RPC)
  // and returns the per-shard result. The manager handles job tracking,
  // sequencing across shards, top-manifest write, and cleanup-on-failure.
  using ShardBackupExecutor = std::function<core::StatusOr<ShardBackupResult>(
      const std::string& backup_id, core::ShardId shard_id,
      uint64_t primary_term, const BackupTarget& target)>;

  // Schedule a distributed (multi-shard) backup asynchronously. Each
  // shard's primary is called via `executor` in sequence; on failure
  // the partial backup is cleaned up under the target prefix. The
  // top-level manifest is written ONLY after every shard succeeds,
  // so a restore that cannot find it treats the backup as nonexistent.
  [[nodiscard]] core::StatusOr<std::string> StartBackupDistributed(
      const std::string& collection_name,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type,
      uint32_t replication_factor,
      const std::vector<core::ShardId>& shard_ids,
      const std::vector<uint64_t>& primary_terms,  // parallel to shard_ids
      const BackupTarget& target,
      ShardBackupExecutor executor,
      const std::string& requested_backup_id = "");

  [[nodiscard]] core::StatusOr<BackupJobStatus> GetStatus(
      const std::string& backup_id) const;

  [[nodiscard]] core::Status Cancel(const std::string& backup_id);

  // Pure synchronous per-shard executor. Uploads every segment of the
  // collection belonging to `shard_id` (in commit 2 single-shard mode
  // this is every segment of the collection on the local store), writes
  // a per-shard manifest, and returns the manifest's object key. Does
  // NOT write the top-level manifest.
  [[nodiscard]] core::StatusOr<ShardBackupResult> RunShardBackup(
      const std::string& backup_id,
      core::CollectionId collection_id,
      core::ShardId shard_id,
      uint64_t primary_term,
      const BackupTarget& target,
      std::shared_ptr<ISegmentStore> segment_store);

  // Test helper: list all known job ids (deterministic order).
  std::vector<std::string> JobIds() const;

 private:
  // Bundles an IObjectStore handle, optional owner (for transient stores
  // like a per-request FilesystemObjectStore), and a printable URI prefix
  // used in the manifest_uri.
  struct ResolvedTarget {
    IObjectStore* store = nullptr;
    std::unique_ptr<IObjectStore> owned;
    std::string display_prefix;  // e.g. "s3://bucket/prefix" or "file:///path"
  };
  [[nodiscard]] core::StatusOr<ResolvedTarget> ResolveTarget(
      const BackupTarget& target);

  void ExecuteSingleShardJob(std::shared_ptr<BackupJob> job,
                             core::Dimension dimension,
                             core::MetricType metric,
                             core::IndexType index_type,
                             uint32_t num_shards,
                             uint32_t replication_factor,
                             core::ShardId shard_id,
                             uint64_t primary_term,
                             std::shared_ptr<ISegmentStore> segment_store);

  void ExecuteDistributedJob(
      std::shared_ptr<BackupJob> job,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type,
      uint32_t replication_factor,
      std::vector<core::ShardId> shard_ids,
      std::vector<uint64_t> primary_terms,
      ShardBackupExecutor executor);

  // Write the top-level manifest after every shard succeeded. Shared by
  // both single- and multi-shard paths. Sets `job->manifest_uri` and
  // returns OK on success; on failure the partial upload is cleaned up
  // and the job is marked FAILED.
  core::Status FinalizeTopManifest(
      std::shared_ptr<BackupJob> job,
      const ResolvedTarget& rt,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type,
      uint32_t num_shards,
      uint32_t replication_factor,
      const std::vector<BackupShardRef>& shard_refs,
      uint64_t vector_count,
      uint64_t size_bytes);

  void CleanupOnFailure(const ResolvedTarget& target,
                        const std::string& backup_id);

  static std::string AllocateBackupId();

  BackupManagerOptions opts_;
  std::unique_ptr<utils::ThreadPool> pool_;
  mutable std::mutex jobs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<BackupJob>> jobs_;
};

// ============================================================================
// RestoreManager
// ============================================================================
class RestoreManager {
 public:
  explicit RestoreManager(RestoreManagerOptions opts);
  ~RestoreManager() = default;

  RestoreManager(const RestoreManager&) = delete;
  RestoreManager& operator=(const RestoreManager&) = delete;

  // Read the top-level manifest of a backup without restoring it.
  // Caller uses this to learn the original collection metadata (so the
  // coordinator can CreateCollection before issuing per-shard restores).
  [[nodiscard]] core::StatusOr<BackupManifestV1> ReadManifest(
      const BackupTarget& source,
      const std::string& backup_id);

  // Schedule a single-shard restore asynchronously.
  // The restored segments keep their original collection_id from the
  // backup manifest. The caller MUST ensure `target_collection_id`
  // matches that id (the single-node path does not remap).
  [[nodiscard]] core::StatusOr<std::string> StartRestoreSingleShard(
      const BackupTarget& source,
      const std::string& backup_id,
      core::CollectionId target_collection_id,
      core::ShardId shard_id,
      std::shared_ptr<ISegmentStore> segment_store);

  // Per-shard restore executor used by the multi-shard orchestration
  // path. Caller (Coordinator) dispatches RestoreShard to the data-node
  // hosting each shard's primary.
  using ShardRestoreExecutor = std::function<core::Status(
      const BackupTarget& source, const std::string& backup_id,
      core::CollectionId target_collection_id, core::ShardId shard_id)>;

  // Schedule a distributed restore asynchronously. Each shard is
  // restored in sequence via `executor`; partial successes are tolerated
  // only inasmuch as the restore is repeatable (segments are installed
  // idempotently via AddReplicatedSegment), but the job's terminal
  // state reflects the first failure.
  [[nodiscard]] core::StatusOr<std::string> StartRestoreDistributed(
      const BackupTarget& source,
      const std::string& backup_id,
      core::CollectionId target_collection_id,
      const std::vector<core::ShardId>& shard_ids,
      ShardRestoreExecutor executor);

  [[nodiscard]] core::StatusOr<RestoreJobStatus> GetStatus(
      const std::string& restore_id) const;

  // Pure synchronous per-shard executor.
  [[nodiscard]] core::Status RunShardRestore(
      const BackupTarget& source,
      const std::string& backup_id,
      core::CollectionId target_collection_id,
      core::ShardId shard_id,
      std::shared_ptr<ISegmentStore> segment_store);

 private:
  struct ResolvedTarget {
    IObjectStore* store = nullptr;
    std::unique_ptr<IObjectStore> owned;
    std::string display_prefix;
  };
  [[nodiscard]] core::StatusOr<ResolvedTarget> ResolveTarget(
      const BackupTarget& source);

  void ExecuteSingleShardJob(std::shared_ptr<RestoreJob> job,
                             core::ShardId shard_id,
                             std::shared_ptr<ISegmentStore> segment_store);

  void ExecuteDistributedJob(std::shared_ptr<RestoreJob> job,
                             std::vector<core::ShardId> shard_ids,
                             ShardRestoreExecutor executor);

  static std::string AllocateRestoreId();

  RestoreManagerOptions opts_;
  std::unique_ptr<utils::ThreadPool> pool_;
  mutable std::mutex jobs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<RestoreJob>> jobs_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_BACKUP_MANAGER_H_
