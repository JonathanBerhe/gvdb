// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/thread_pool.h"
#include "core/status.h"
#include "core/types.h"
#include "storage/backup/backup_manifest.h"
#include "storage/backup/backup_target.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_store.h"

namespace gvdb {
namespace storage {

// Progress of one restore job. The atomics have the same shape as the
// BackupJob and BulkImporter jobs.
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

// One RestoreManager runs per server. `default_object_store` and
// `local_root_allowlist` work as in BackupManagerOptions.
struct RestoreManagerOptions {
  IObjectStore* default_object_store = nullptr;
  std::string s3_bucket;
  std::string local_root_allowlist;
  std::string staging_dir = "/tmp/gvdb-restore";
  int max_concurrent_jobs = 2;
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
  std::unique_ptr<common::ThreadPool> pool_;
  mutable std::mutex jobs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<RestoreJob>> jobs_;
};

}  // namespace storage
}  // namespace gvdb
