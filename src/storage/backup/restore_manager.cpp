// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// RestoreManager: single-shard and distributed restores.

#include "storage/backup/restore_manager.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <system_error>
#include <utility>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "storage/backup/backup_util.h"
#include "storage/backup/segment_export.h"
#include "storage/objstore/filesystem_object_store.h"

namespace gvdb::storage {

// ============================================================================
// RestoreManager
// ============================================================================

RestoreManager::RestoreManager(RestoreManagerOptions opts) : opts_(std::move(opts)) {
  pool_ = std::make_unique<common::ThreadPool>(
      static_cast<size_t>(std::max(1, opts_.max_concurrent_jobs)));
  std::error_code ec;
  std::filesystem::create_directories(opts_.staging_dir, ec);
}

std::string RestoreManager::AllocateRestoreId() {
  return absl::StrCat("rs-", RFC3339Now(), "-", RandomToken());
}

absl::StatusOr<RestoreManager::ResolvedTarget> RestoreManager::ResolveTarget(
    const BackupTarget& source) {
  if (std::holds_alternative<std::monostate>(source)) {
    return absl::InvalidArgumentError("BackupTarget source is unset");
  }
  if (auto* s3 = std::get_if<S3BackupTarget>(&source)) {
    if (!opts_.default_object_store) {
      return absl::FailedPreconditionError("Server is not configured with an S3 object store");
    }
    if (!opts_.s3_bucket.empty() && s3->bucket != opts_.s3_bucket) {
      return absl::InvalidArgumentError(absl::StrCat("Requested S3 bucket '", s3->bucket,
                                                     "' does not match server-configured bucket '",
                                                     opts_.s3_bucket, "'"));
    }
    ResolvedTarget r;
    r.store = opts_.default_object_store;
    r.display_prefix =
        absl::StrCat("s3://", s3->bucket, s3->prefix.empty() ? "" : absl::StrCat("/", s3->prefix));
    return r;
  }
  if (auto* loc = std::get_if<LocalBackupTarget>(&source)) {
    if (opts_.local_root_allowlist.empty()) {
      return absl::FailedPreconditionError("LocalBackupTarget restore is disabled on this server");
    }
    if (!PathUnderRoot(opts_.local_root_allowlist, loc->path)) {
      return absl::InvalidArgumentError(absl::StrCat(
          "LocalBackupTarget path '", loc->path, "' is not under the configured allowlist root '",
          opts_.local_root_allowlist, "'"));
    }
    auto store_or = FilesystemObjectStore::Create(loc->path);
    if (!store_or.ok()) return store_or.status();
    ResolvedTarget r;
    r.owned = std::move(*store_or);
    r.store = r.owned.get();
    r.display_prefix = absl::StrCat("file://", loc->path);
    return r;
  }
  return absl::InvalidArgumentError("Unknown BackupTarget variant");
}

absl::StatusOr<BackupManifestV1> RestoreManager::ReadManifest(const BackupTarget& source,
                                                              const std::string& backup_id) {
  auto rt_or = ResolveTarget(source);
  if (!rt_or.ok()) return rt_or.status();
  auto& rt = *rt_or;
  std::string s3_prefix;
  if (auto* s3 = std::get_if<S3BackupTarget>(&source)) {
    s3_prefix = s3->prefix;
  }
  auto full_key = BuildKey(s3_prefix, BackupManifest::TopManifestKey(backup_id));
  auto json_or = rt.store->GetObject(full_key);
  if (!json_or.ok()) return json_or.status();
  return BackupManifest::DeserializeTop(*json_or);
}

absl::Status RestoreManager::RunShardRestore(const BackupTarget& source,
                                             const std::string& backup_id,
                                             core::CollectionId target_collection_id,
                                             core::ShardId shard_id,
                                             const std::shared_ptr<ISegmentStore>& segment_store) {
  if (backup_id.empty()) {
    return absl::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return absl::InvalidArgumentError("segment_store is null");
  }
  auto rt_or = ResolveTarget(source);
  if (!rt_or.ok()) return rt_or.status();
  auto& rt = *rt_or;

  std::string s3_prefix;
  if (auto* s3 = std::get_if<S3BackupTarget>(&source)) {
    s3_prefix = s3->prefix;
  }

  // Fetch shard manifest.
  auto shard_key =
      BuildKey(s3_prefix, BackupManifest::ShardManifestKey(backup_id, core::ToUInt16(shard_id)));
  auto json_or = rt.store->GetObject(shard_key);
  if (!json_or.ok()) return json_or.status();
  auto mani_or = BackupManifest::DeserializeShard(*json_or);
  if (!mani_or.ok()) return mani_or.status();

  // Staging directory layout matches what Segment::Load expects:
  //   <staging_root>/restore-<id>/segment_<seg_id>/<files>
  std::error_code ec;
  std::filesystem::create_directories(opts_.staging_dir, ec);
  auto staging_root =
      std::filesystem::path(opts_.staging_dir) /
      absl::StrCat("restore-", backup_id, "-shard-", core::ToUInt16(shard_id), "-", RandomToken());
  std::filesystem::create_directories(staging_root, ec);

  // Inner cleanup helper: Status return preserves the original error
  // while still removing the staging tree.
  auto cleanup = [&]() {
    std::error_code rec;
    std::filesystem::remove_all(staging_root, rec);
  };

  // Restoring into a fresh target collection (different collection_id
  // than the backup recorded) requires remapping segment_ids too, because
  // the original ids may still be live under the source collection on the
  // same data-node. core::ShardSegmentId gives the restored segments the
  // same ids that the resolver hands out for fresh segments. The remap is
  // a no-op when the manifest's collection_id already matches the target.
  const bool remap_needed =
      !mani_or->segments.empty() &&
      mani_or->segments.front().segment_id !=
          core::ToUInt32(core::ShardSegmentId(target_collection_id, core::ToUInt16(shard_id)));

  for (size_t seg_index = 0; seg_index < mani_or->segments.size(); ++seg_index) {
    const auto& seg = mani_or->segments[seg_index];
    auto seg_dir = staging_root / absl::StrCat("segment_", seg.segment_id);
    std::filesystem::create_directories(seg_dir, ec);

    for (const auto& f : seg.files) {
      auto local = seg_dir / f.name;
      auto status = rt.store->GetObjectToFile(f.key, local.string());
      if (!status.ok()) {
        cleanup();
        return status;
      }
    }

    auto seg_or = SegmentExporter::Import(
        seg_dir, core::MakeSegmentId(static_cast<uint32_t>(seg.segment_id)));
    if (!seg_or.ok()) {
      cleanup();
      return seg_or.status();
    }

    if ((*seg_or)->GetCollectionId() != target_collection_id) {
      (*seg_or)->SetCollectionIdForRestore(target_collection_id);
    }

    if (remap_needed) {
      // seg_index=0 overwrites the empty segment the resolver may have
      // eagerly created on CreateCollection: that's exactly what we
      // want for the common one-segment-per-shard restore.
      auto new_seg_id = core::MakeSegmentId(
          core::ToUInt32(core::ShardSegmentId(target_collection_id, core::ToUInt16(shard_id))) +
          static_cast<uint32_t>(seg_index));
      (*seg_or)->SetSegmentIdForRestore(new_seg_id);
      // Drop any existing segment at this id (the empty
      // CreateCollection-allocated one) so AddReplicatedSegment doesn't
      // see AlreadyExists. Idempotent: NotFound is treated as OK.
      (void)segment_store->DropSegment(new_seg_id,
                                       /*delete_files=*/false);
    }

    const auto installed_id = (*seg_or)->GetId();
    const bool installed_growing = (*seg_or)->GetState() == core::SegmentState::GROWING;

    auto add_status = segment_store->AddReplicatedSegment(std::move(*seg_or));
    if (!add_status.ok()) {
      cleanup();
      return add_status;
    }

    // Persist non-growing restored segments on THIS node. Their pre-install
    // FLUSHED state referred to the source node's disk; without a local
    // flush they are memory-only and a restart silently loses the restored
    // collection. On a tiered store the flush also schedules the cold-tier
    // upload. Growing segments follow the normal seal-then-flush lifecycle.
    // Restore exists to bring data back durably, so a flush failure fails
    // the shard restore (the job reports FAILED and can be retried) rather
    // than leaving a restore that quietly evaporates on the next restart.
    if (!installed_growing) {
      auto flush_status = segment_store->FlushSegment(installed_id);
      if (!flush_status.ok()) {
        common::Logger::Instance().Error("Failed to flush restored segment {}: {}",
                                         core::ToUInt32(installed_id), flush_status.message());
        cleanup();
        return flush_status;
      }
    }
  }

  cleanup();
  return absl::OkStatus();
}

absl::StatusOr<std::string> RestoreManager::StartRestoreSingleShard(
    const BackupTarget& source, const std::string& backup_id,
    core::CollectionId target_collection_id, core::ShardId shard_id,
    const std::shared_ptr<ISegmentStore>& segment_store) {
  if (backup_id.empty()) {
    return absl::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return absl::InvalidArgumentError("segment_store is null");
  }
  // Pre-validate target so async caller can't paper over a config error.
  auto rt = ResolveTarget(source);
  if (!rt.ok()) return rt.status();

  auto id = AllocateRestoreId();
  auto job = std::make_shared<RestoreJob>();
  job->id = id;
  job->source = source;
  job->backup_id = backup_id;
  job->target_collection_id = core::ToUInt32(target_collection_id);
  job->shards_total = 1;
  job->start_time = std::chrono::steady_clock::now();

  {
    std::scoped_lock lk(jobs_mutex_);
    jobs_[id] = job;
  }

  (void)pool_->Enqueue([this, job, shard_id, segment_store]() {
    ExecuteSingleShardJob(job, shard_id, segment_store);
  });

  return id;
}

void RestoreManager::ExecuteSingleShardJob(const std::shared_ptr<RestoreJob>& job,
                                           core::ShardId shard_id,
                                           const std::shared_ptr<ISegmentStore>& segment_store) {
  job->state.store(BackupState::RUNNING);

  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }
  auto status =
      RunShardRestore(job->source, job->backup_id,
                      core::MakeCollectionId(job->target_collection_id), shard_id, segment_store);
  if (!status.ok()) {
    job->SetError(std::string(status.message()));
    job->state.store(BackupState::FAILED);
    return;
  }
  job->shards_completed.store(1);
  job->state.store(BackupState::COMPLETED);
}

absl::StatusOr<std::string> RestoreManager::StartRestoreDistributed(
    const BackupTarget& source, const std::string& backup_id,
    core::CollectionId target_collection_id, const std::vector<core::ShardId>& shard_ids,
    ShardRestoreExecutor executor) {
  if (backup_id.empty()) {
    return absl::InvalidArgumentError("backup_id is required");
  }
  if (shard_ids.empty()) {
    return absl::InvalidArgumentError("shard_ids is empty");
  }
  if (!executor) {
    return absl::InvalidArgumentError("executor is required");
  }
  auto rt = ResolveTarget(source);
  if (!rt.ok()) return rt.status();

  auto id = AllocateRestoreId();
  auto job = std::make_shared<RestoreJob>();
  job->id = id;
  job->source = source;
  job->backup_id = backup_id;
  job->target_collection_id = core::ToUInt32(target_collection_id);
  job->shards_total = static_cast<uint32_t>(shard_ids.size());
  job->start_time = std::chrono::steady_clock::now();

  {
    std::scoped_lock lk(jobs_mutex_);
    jobs_[id] = job;
  }

  std::vector<core::ShardId> shards_copy = shard_ids;
  (void)pool_->Enqueue(
      [this, job, shards_copy = std::move(shards_copy), executor = std::move(executor)]() mutable {
        ExecuteDistributedJob(job, shards_copy, executor);
      });

  return id;
}

void RestoreManager::ExecuteDistributedJob(const std::shared_ptr<RestoreJob>& job,
                                           const std::vector<core::ShardId>& shard_ids,
                                           const ShardRestoreExecutor& executor) {
  job->state.store(BackupState::RUNNING);
  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }
  for (auto shard_id : shard_ids) {
    auto status = executor(job->source, job->backup_id,
                           core::MakeCollectionId(job->target_collection_id), shard_id);
    if (!status.ok()) {
      job->SetError(std::string(status.message()));
      job->state.store(BackupState::FAILED);
      return;
    }
    job->shards_completed.fetch_add(1);
  }
  job->state.store(BackupState::COMPLETED);
}

absl::StatusOr<RestoreJobStatus> RestoreManager::GetStatus(const std::string& restore_id) const {
  std::shared_ptr<RestoreJob> job;
  {
    std::scoped_lock lk(jobs_mutex_);
    auto it = jobs_.find(restore_id);
    if (it == jobs_.end()) {
      return absl::NotFoundError(absl::StrCat("Restore not found: ", restore_id));
    }
    job = it->second;
  }
  RestoreJobStatus s;
  s.restore_id = job->id;
  s.state = job->state.load();
  s.shards_total = job->shards_total.load();
  s.shards_completed = job->shards_completed.load();
  s.error_message = job->GetError();
  auto now = std::chrono::steady_clock::now();
  s.elapsed_seconds = std::chrono::duration<float>(now - job->start_time).count();
  return s;
}

}  // namespace gvdb::storage
