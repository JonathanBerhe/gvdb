// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// BackupManager: single-shard and distributed backups.

#include "storage/backup/backup_manager.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <system_error>
#include "absl/strings/str_cat.h"
#include "core/status.h"
#include "storage/backup/backup_util.h"
#include "storage/backup/segment_export.h"
#include "storage/objstore/filesystem_object_store.h"

namespace gvdb {
namespace storage {

namespace {

int64_t UnixMillisNow() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace

// ============================================================================
// BackupManager
// ============================================================================

BackupManager::BackupManager(BackupManagerOptions opts) : opts_(std::move(opts)) {
  pool_ = std::make_unique<common::ThreadPool>(
      static_cast<size_t>(std::max(1, opts_.max_concurrent_jobs)));
  std::error_code ec;
  std::filesystem::create_directories(opts_.tmp_dir, ec);
}

std::string BackupManager::AllocateBackupId() {
  return absl::StrCat("bk-", RFC3339Now(), "-", RandomToken());
}

absl::StatusOr<BackupManager::ResolvedTarget> BackupManager::ResolveTarget(
    const BackupTarget& target) {
  if (std::holds_alternative<std::monostate>(target)) {
    return absl::InvalidArgumentError("BackupTarget is unset (must select S3 or Local)");
  }
  if (auto* s3 = std::get_if<S3BackupTarget>(&target)) {
    if (!opts_.default_object_store) {
      return absl::FailedPreconditionError(
          "Server is not configured with an S3 object store; "
          "use a LocalBackupTarget or rebuild with -DGVDB_WITH_S3=ON");
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
  if (auto* loc = std::get_if<LocalBackupTarget>(&target)) {
    if (opts_.local_root_allowlist.empty()) {
      return absl::FailedPreconditionError(
          "LocalBackupTarget is disabled on this server (no allowlist root)");
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

// Per-shard executor: pure synchronous logic. Iterates the collection's
// segments on this node, uploads each, and writes the per-shard manifest.
absl::StatusOr<ShardBackupResult> BackupManager::RunShardBackup(
    const std::string& backup_id, core::CollectionId collection_id, core::ShardId shard_id,
    uint64_t primary_term, const BackupTarget& target,
    std::shared_ptr<ISegmentStore> segment_store) {
  if (backup_id.empty()) {
    return absl::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return absl::InvalidArgumentError("segment_store is null");
  }

  auto rt_or = ResolveTarget(target);
  if (!rt_or.ok()) return rt_or.status();
  auto& rt = *rt_or;

  // Compute the IObjectStore-relative prefix for this backup. For S3
  // we honor the user-supplied prefix; for local the FilesystemObjectStore
  // is already rooted at the user's path, so the prefix is empty.
  std::string s3_prefix;
  if (auto* s3 = std::get_if<S3BackupTarget>(&target)) {
    s3_prefix = s3->prefix;
  }

  ShardBackupResult result;
  ShardManifestV1 shard_manifest;
  shard_manifest.shard_id = core::ToUInt16(shard_id);
  shard_manifest.node_id = opts_.node_id;
  shard_manifest.primary_term = primary_term;

  auto segment_ids = segment_store->GetCollectionSegments(collection_id);
  // Sort for deterministic manifest ordering: tests rely on it.
  std::sort(segment_ids.begin(), segment_ids.end(), [](core::SegmentId a, core::SegmentId b) {
    return core::ToUInt32(a) < core::ToUInt32(b);
  });

  for (auto seg_id : segment_ids) {
    Segment* seg = segment_store->GetSegment(seg_id);
    if (seg == nullptr) continue;

    auto export_or = SegmentExporter::Export(*seg, opts_.flushed_segments_root, opts_.tmp_dir);
    if (!export_or.ok()) {
      return export_or.status();
    }
    auto& exp = *export_or;

    ShardBackupSegment manifest_seg;
    manifest_seg.segment_id = exp.segment_id;
    manifest_seg.state = exp.is_growing ? "growing" : "sealed";
    manifest_seg.vector_count = exp.vector_count;
    manifest_seg.size_bytes = exp.total_size_bytes;

    ShardBackupSegmentResult seg_result;
    seg_result.segment_id = exp.segment_id;
    seg_result.is_growing = exp.is_growing;
    seg_result.vector_count = exp.vector_count;
    seg_result.size_bytes = exp.total_size_bytes;

    for (const auto& f : exp.files) {
      auto rel_key = BackupManifest::SegmentFileKey(backup_id, core::ToUInt16(shard_id),
                                                    exp.segment_id, f.name);
      auto full_key = BuildKey(s3_prefix, rel_key);
      auto put_status = rt.store->PutObjectFromFile(full_key, f.source_path.string());
      if (!put_status.ok()) {
        // Cleanup tmp_dir if we made one before propagating.
        if (!exp.tmp_dir.empty()) {
          std::error_code ec;
          std::filesystem::remove_all(exp.tmp_dir, ec);
        }
        return put_status;
      }
      manifest_seg.files.push_back(ShardBackupFile{f.name, full_key, f.size});
      seg_result.object_keys.push_back(full_key);
      result.bytes_uploaded += f.size;
    }

    // Tmp directory is no longer needed for this segment.
    if (!exp.tmp_dir.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(exp.tmp_dir, ec);
    }

    shard_manifest.segments.push_back(std::move(manifest_seg));
    result.segments.push_back(std::move(seg_result));
  }

  // Write the per-shard manifest.
  auto rel_shard_key = BackupManifest::ShardManifestKey(backup_id, core::ToUInt16(shard_id));
  auto full_shard_key = BuildKey(s3_prefix, rel_shard_key);
  auto shard_json = BackupManifest::SerializeShard(shard_manifest);
  RETURN_IF_ERROR(rt.store->PutObject(full_shard_key, shard_json));
  result.shard_manifest_key = full_shard_key;
  result.bytes_uploaded += shard_json.size();

  return result;
}

absl::StatusOr<std::string> BackupManager::StartBackupSingleShard(
    const std::string& collection_name, core::CollectionId collection_id, core::Dimension dimension,
    core::MetricType metric, core::IndexType index_type, uint32_t num_shards,
    uint32_t replication_factor, core::ShardId shard_id, uint64_t primary_term,
    const BackupTarget& target, std::shared_ptr<ISegmentStore> segment_store,
    const std::string& requested_backup_id) {
  if (collection_name.empty()) {
    return absl::InvalidArgumentError("collection_name is required");
  }
  if (!segment_store) {
    return absl::InvalidArgumentError("segment_store is null");
  }
  // Pre-validate the target so the caller gets a synchronous error
  // instead of having to poll for FAILED with a stale-target message.
  auto rt = ResolveTarget(target);
  if (!rt.ok()) return rt.status();

  std::string id = requested_backup_id.empty() ? AllocateBackupId() : requested_backup_id;

  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(id);
    if (it != jobs_.end()) {
      auto s = it->second->state.load();
      if (s == BackupState::PENDING || s == BackupState::RUNNING || s == BackupState::COMPLETED) {
        return id;  // idempotent: existing run wins
      }
      // FAILED or CANCELLED: overwrite.
      jobs_.erase(it);
    }
  }

  auto job = std::make_shared<BackupJob>();
  job->id = id;
  job->collection_name = collection_name;
  job->collection_id = core::ToUInt32(collection_id);
  job->target = target;
  job->shards_total = 1;
  job->start_time = std::chrono::steady_clock::now();

  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    jobs_[id] = job;
  }

  (void)pool_->Enqueue([this, job, dimension, metric, index_type, num_shards, replication_factor,
                        shard_id, primary_term, segment_store]() {
    ExecuteSingleShardJob(job, dimension, metric, index_type, num_shards, replication_factor,
                          shard_id, primary_term, segment_store);
  });

  return id;
}

void BackupManager::ExecuteSingleShardJob(std::shared_ptr<BackupJob> job, core::Dimension dimension,
                                          core::MetricType metric, core::IndexType index_type,
                                          uint32_t num_shards, uint32_t replication_factor,
                                          core::ShardId shard_id, uint64_t primary_term,
                                          std::shared_ptr<ISegmentStore> segment_store) {
  job->state.store(BackupState::RUNNING);

  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }

  auto rt_or = ResolveTarget(job->target);
  if (!rt_or.ok()) {
    job->SetError(std::string(rt_or.status().message()));
    job->state.store(BackupState::FAILED);
    return;
  }
  auto& rt = *rt_or;

  auto shard_or = RunShardBackup(job->id, core::MakeCollectionId(job->collection_id), shard_id,
                                 primary_term, job->target, segment_store);
  if (!shard_or.ok()) {
    job->SetError(std::string(shard_or.status().message()));
    CleanupOnFailure(rt, job->id);
    job->state.store(BackupState::FAILED);
    return;
  }
  job->bytes_uploaded.fetch_add(shard_or->bytes_uploaded);

  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;
  for (const auto& seg : shard_or->segments) {
    vector_count += seg.vector_count;
    size_bytes += seg.size_bytes;
  }
  std::vector<BackupShardRef> shard_refs;
  shard_refs.push_back(BackupShardRef{static_cast<uint32_t>(core::ToUInt16(shard_id)),
                                      shard_or->shard_manifest_key});

  auto final_status = FinalizeTopManifest(job, rt, dimension, metric, index_type, num_shards,
                                          replication_factor, shard_refs, vector_count, size_bytes);
  if (!final_status.ok()) return;  // FinalizeTopManifest already set FAILED.
  job->shards_completed.store(1);
  job->state.store(BackupState::COMPLETED);
}

absl::Status BackupManager::FinalizeTopManifest(std::shared_ptr<BackupJob> job,
                                                const ResolvedTarget& rt, core::Dimension dimension,
                                                core::MetricType metric, core::IndexType index_type,
                                                uint32_t num_shards, uint32_t replication_factor,
                                                const std::vector<BackupShardRef>& shard_refs,
                                                uint64_t vector_count, uint64_t size_bytes) {
  BackupManifestV1 top;
  top.backup_id = job->id;
  top.collection.collection_id = job->collection_id;
  top.collection.collection_name = job->collection_name;
  top.collection.dimension = dimension;
  top.collection.metric = static_cast<int32_t>(metric);
  top.collection.index_type = static_cast<int32_t>(index_type);
  top.collection.num_shards = num_shards;
  top.collection.replication_factor = replication_factor;
  top.created_at_unix_ms = UnixMillisNow();
  top.vector_count = vector_count;
  top.size_bytes = size_bytes;
  top.shards = shard_refs;
  top.gvdb_version = opts_.gvdb_version;

  std::string s3_prefix;
  if (auto* s3 = std::get_if<S3BackupTarget>(&job->target)) {
    s3_prefix = s3->prefix;
  }
  auto rel_top_key = BackupManifest::TopManifestKey(job->id);
  auto full_top_key = BuildKey(s3_prefix, rel_top_key);
  auto top_json = BackupManifest::SerializeTop(top);
  auto put_status = rt.store->PutObject(full_top_key, top_json);
  if (!put_status.ok()) {
    job->SetError(std::string(put_status.message()));
    CleanupOnFailure(rt, job->id);
    job->state.store(BackupState::FAILED);
    return put_status;
  }
  job->bytes_uploaded.fetch_add(top_json.size());
  job->manifest_uri = absl::StrCat(rt.display_prefix, "/", rel_top_key);
  return absl::OkStatus();
}

// ============================================================================
// Distributed (multi-shard) backup
// ============================================================================

absl::StatusOr<std::string> BackupManager::StartBackupDistributed(
    const std::string& collection_name, core::CollectionId collection_id, core::Dimension dimension,
    core::MetricType metric, core::IndexType index_type, uint32_t replication_factor,
    const std::vector<core::ShardId>& shard_ids, const std::vector<uint64_t>& primary_terms,
    const BackupTarget& target, ShardBackupExecutor executor,
    const std::string& requested_backup_id) {
  if (collection_name.empty()) {
    return absl::InvalidArgumentError("collection_name is required");
  }
  if (shard_ids.empty()) {
    return absl::InvalidArgumentError("shard_ids is empty");
  }
  if (shard_ids.size() != primary_terms.size()) {
    return absl::InvalidArgumentError("shard_ids and primary_terms must have the same length");
  }
  if (!executor) {
    return absl::InvalidArgumentError("executor is required");
  }
  auto rt = ResolveTarget(target);
  if (!rt.ok()) return rt.status();

  std::string id = requested_backup_id.empty() ? AllocateBackupId() : requested_backup_id;
  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(id);
    if (it != jobs_.end()) {
      auto s = it->second->state.load();
      if (s == BackupState::PENDING || s == BackupState::RUNNING || s == BackupState::COMPLETED) {
        return id;
      }
      jobs_.erase(it);
    }
  }

  auto job = std::make_shared<BackupJob>();
  job->id = id;
  job->collection_name = collection_name;
  job->collection_id = core::ToUInt32(collection_id);
  job->target = target;
  job->shards_total = static_cast<uint32_t>(shard_ids.size());
  job->start_time = std::chrono::steady_clock::now();

  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    jobs_[id] = job;
  }

  (void)pool_->Enqueue([this, job, dimension, metric, index_type, replication_factor, shard_ids,
                        primary_terms, executor]() mutable {
    ExecuteDistributedJob(job, dimension, metric, index_type, replication_factor,
                          std::move(shard_ids), std::move(primary_terms), std::move(executor));
  });

  return id;
}

void BackupManager::ExecuteDistributedJob(std::shared_ptr<BackupJob> job, core::Dimension dimension,
                                          core::MetricType metric, core::IndexType index_type,
                                          uint32_t replication_factor,
                                          std::vector<core::ShardId> shard_ids,
                                          std::vector<uint64_t> primary_terms,
                                          ShardBackupExecutor executor) {
  job->state.store(BackupState::RUNNING);
  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }
  auto rt_or = ResolveTarget(job->target);
  if (!rt_or.ok()) {
    job->SetError(std::string(rt_or.status().message()));
    job->state.store(BackupState::FAILED);
    return;
  }
  auto& rt = *rt_or;

  std::vector<BackupShardRef> shard_refs;
  shard_refs.reserve(shard_ids.size());
  uint64_t vector_count = 0;
  uint64_t size_bytes = 0;

  for (size_t i = 0; i < shard_ids.size(); ++i) {
    if (job->cancelled.load()) {
      CleanupOnFailure(rt, job->id);
      job->state.store(BackupState::CANCELLED);
      return;
    }
    auto shard_result = executor(job->id, shard_ids[i], primary_terms[i], job->target);
    if (!shard_result.ok()) {
      job->SetError(std::string(shard_result.status().message()));
      CleanupOnFailure(rt, job->id);
      job->state.store(BackupState::FAILED);
      return;
    }
    for (const auto& seg : shard_result->segments) {
      vector_count += seg.vector_count;
      size_bytes += seg.size_bytes;
    }
    job->bytes_uploaded.fetch_add(shard_result->bytes_uploaded);
    shard_refs.push_back(BackupShardRef{static_cast<uint32_t>(core::ToUInt16(shard_ids[i])),
                                        shard_result->shard_manifest_key});
    job->shards_completed.fetch_add(1);
  }

  auto final_status = FinalizeTopManifest(job, rt, dimension, metric, index_type,
                                          static_cast<uint32_t>(shard_ids.size()),
                                          replication_factor, shard_refs, vector_count, size_bytes);
  if (!final_status.ok()) return;  // already FAILED inside helper
  job->state.store(BackupState::COMPLETED);
}

void BackupManager::CleanupOnFailure(const ResolvedTarget& rt, const std::string& backup_id) {
  std::string s3_prefix;
  // Best-effort: scan the backup prefix and delete everything under it.
  // We don't have the request target here so we use both relative and
  // s3-prefixed forms. For S3 targets the s3_prefix is needed; for local
  // it is empty.
  auto root = BackupManifest::BackupRootPrefix(backup_id);
  auto list = rt.store->ListObjects(root);
  if (!list.ok()) {
    list = rt.store->ListObjects(BuildKey(s3_prefix, root));
  }
  if (!list.ok()) return;
  for (const auto& key : *list) {
    (void)rt.store->DeleteObject(key);
  }
}

absl::StatusOr<BackupJobStatus> BackupManager::GetStatus(const std::string& backup_id) const {
  std::shared_ptr<BackupJob> job;
  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(backup_id);
    if (it == jobs_.end()) {
      return absl::NotFoundError(absl::StrCat("Backup not found: ", backup_id));
    }
    job = it->second;
  }
  BackupJobStatus s;
  s.backup_id = job->id;
  s.state = job->state.load();
  s.shards_total = job->shards_total.load();
  s.shards_completed = job->shards_completed.load();
  s.bytes_uploaded = job->bytes_uploaded.load();
  s.error_message = job->GetError();
  s.manifest_uri = job->manifest_uri;
  auto now = std::chrono::steady_clock::now();
  s.elapsed_seconds = std::chrono::duration<float>(now - job->start_time).count();
  return s;
}

absl::Status BackupManager::Cancel(const std::string& backup_id) {
  std::lock_guard<std::mutex> lk(jobs_mutex_);
  auto it = jobs_.find(backup_id);
  if (it == jobs_.end()) {
    return absl::NotFoundError(absl::StrCat("Backup not found: ", backup_id));
  }
  it->second->cancelled.store(true);
  return absl::OkStatus();
}

std::vector<std::string> BackupManager::JobIds() const {
  std::vector<std::string> ids;
  std::lock_guard<std::mutex> lk(jobs_mutex_);
  ids.reserve(jobs_.size());
  for (const auto& [id, _] : jobs_) ids.push_back(id);
  std::sort(ids.begin(), ids.end());
  return ids;
}

}  // namespace storage
}  // namespace gvdb
