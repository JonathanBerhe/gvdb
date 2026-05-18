// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/backup_manager.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <random>
#include <system_error>

#include "absl/strings/str_cat.h"
#include "storage/filesystem_object_store.h"
#include "storage/segment_export.h"

namespace gvdb {
namespace storage {

namespace {

// ============================================================================
// Helpers
// ============================================================================

// Compose object keys relative to the IObjectStore's namespace.
// For S3 targets we honor `request.prefix`; for local targets the
// FilesystemObjectStore is rooted at the request's path, so no prefix
// is needed. Either way every key passes through this helper so the
// shape stays uniform.
std::string BuildKey(const std::string& prefix, const std::string& suffix) {
  if (prefix.empty()) return suffix;
  // Strip any trailing slash on prefix; suffix always starts with a slash-free
  // path produced by BackupManifest::*Key().
  std::string p = prefix;
  while (!p.empty() && p.back() == '/') p.pop_back();
  return absl::StrCat(p, "/", suffix);
}

std::string RFC3339Now() {
  auto now = std::chrono::system_clock::now();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(
                  now.time_since_epoch())
                  .count();
  std::time_t t = static_cast<std::time_t>(secs);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
  return buf;
}

int64_t UnixMillisNow() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string RandomToken(size_t len = 6) {
  static const char kAlphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  thread_local std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(0, sizeof(kAlphabet) - 2);
  std::string s(len, 'a');
  for (auto& c : s) c = kAlphabet[dist(rng)];
  return s;
}

// True iff `child` resolves to a path under `root` (lexically).
bool PathUnderRoot(const std::string& root, const std::string& child) {
  if (root.empty()) return false;
  std::error_code ec;
  auto r = std::filesystem::weakly_canonical(
      std::filesystem::path(root), ec);
  if (ec) r = std::filesystem::path(root);
  auto c = std::filesystem::weakly_canonical(
      std::filesystem::path(child), ec);
  if (ec) c = std::filesystem::path(child);
  auto rs = r.string();
  auto cs = c.string();
  if (rs.empty() || cs.empty()) return false;
  if (cs.size() < rs.size()) return false;
  if (cs.compare(0, rs.size(), rs) != 0) return false;
  // Either equal, or the next character is a separator.
  return cs.size() == rs.size() || cs[rs.size()] == '/' ||
         cs[rs.size()] == '\\';
}

}  // namespace

// ============================================================================
// BackupManager
// ============================================================================

BackupManager::BackupManager(BackupManagerOptions opts)
    : opts_(std::move(opts)) {
  pool_ = std::make_unique<utils::ThreadPool>(
      static_cast<size_t>(std::max(1, opts_.max_concurrent_jobs)));
  std::error_code ec;
  std::filesystem::create_directories(opts_.tmp_dir, ec);
}

std::string BackupManager::AllocateBackupId() {
  return absl::StrCat("bk-", RFC3339Now(), "-", RandomToken());
}

core::StatusOr<BackupManager::ResolvedTarget> BackupManager::ResolveTarget(
    const BackupTarget& target) {
  if (std::holds_alternative<std::monostate>(target)) {
    return core::InvalidArgumentError(
        "BackupTarget is unset (must select S3 or Local)");
  }
  if (auto* s3 = std::get_if<S3BackupTarget>(&target)) {
    if (!opts_.default_object_store) {
      return core::FailedPreconditionError(
          "Server is not configured with an S3 object store; "
          "use a LocalBackupTarget or rebuild with -DGVDB_WITH_S3=ON");
    }
    if (!opts_.s3_bucket.empty() && s3->bucket != opts_.s3_bucket) {
      return core::InvalidArgumentError(absl::StrCat(
          "Requested S3 bucket '", s3->bucket,
          "' does not match server-configured bucket '", opts_.s3_bucket,
          "'"));
    }
    ResolvedTarget r;
    r.store = opts_.default_object_store;
    r.display_prefix =
        absl::StrCat("s3://", s3->bucket,
                     s3->prefix.empty() ? "" : absl::StrCat("/", s3->prefix));
    return r;
  }
  if (auto* loc = std::get_if<LocalBackupTarget>(&target)) {
    if (opts_.local_root_allowlist.empty()) {
      return core::FailedPreconditionError(
          "LocalBackupTarget is disabled on this server (no allowlist root)");
    }
    if (!PathUnderRoot(opts_.local_root_allowlist, loc->path)) {
      return core::InvalidArgumentError(absl::StrCat(
          "LocalBackupTarget path '", loc->path,
          "' is not under the configured allowlist root '",
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
  return core::InvalidArgumentError("Unknown BackupTarget variant");
}

// Per-shard executor: pure synchronous logic. Iterates the collection's
// segments on this node, uploads each, and writes the per-shard manifest.
core::StatusOr<ShardBackupResult> BackupManager::RunShardBackup(
    const std::string& backup_id,
    core::CollectionId collection_id,
    core::ShardId shard_id,
    uint64_t primary_term,
    const BackupTarget& target,
    std::shared_ptr<ISegmentStore> segment_store) {
  if (backup_id.empty()) {
    return core::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return core::InvalidArgumentError("segment_store is null");
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
  // Sort for deterministic manifest ordering — tests rely on it.
  std::sort(segment_ids.begin(), segment_ids.end(),
            [](core::SegmentId a, core::SegmentId b) {
              return core::ToUInt32(a) < core::ToUInt32(b);
            });

  for (auto seg_id : segment_ids) {
    Segment* seg = segment_store->GetSegment(seg_id);
    if (seg == nullptr) continue;

    auto export_or = SegmentExporter::Export(
        *seg, opts_.flushed_segments_root, opts_.tmp_dir);
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
      auto rel_key = BackupManifest::SegmentFileKey(
          backup_id, core::ToUInt16(shard_id), exp.segment_id, f.name);
      auto full_key = BuildKey(s3_prefix, rel_key);
      auto put_status = rt.store->PutObjectFromFile(
          full_key, f.source_path.string());
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
  auto rel_shard_key = BackupManifest::ShardManifestKey(
      backup_id, core::ToUInt16(shard_id));
  auto full_shard_key = BuildKey(s3_prefix, rel_shard_key);
  auto shard_json = BackupManifest::SerializeShard(shard_manifest);
  auto put_status = rt.store->PutObject(full_shard_key, shard_json);
  if (!put_status.ok()) return put_status;
  result.shard_manifest_key = full_shard_key;
  result.bytes_uploaded += shard_json.size();

  return result;
}

core::StatusOr<std::string> BackupManager::StartBackupSingleShard(
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
    const std::string& requested_backup_id) {
  if (collection_name.empty()) {
    return core::InvalidArgumentError("collection_name is required");
  }
  if (!segment_store) {
    return core::InvalidArgumentError("segment_store is null");
  }
  // Pre-validate the target so the caller gets a synchronous error
  // instead of having to poll for FAILED with a stale-target message.
  auto rt = ResolveTarget(target);
  if (!rt.ok()) return rt.status();

  std::string id =
      requested_backup_id.empty() ? AllocateBackupId() : requested_backup_id;

  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(id);
    if (it != jobs_.end()) {
      auto s = it->second->state.load();
      if (s == BackupState::PENDING || s == BackupState::RUNNING ||
          s == BackupState::COMPLETED) {
        return id;  // idempotent: existing run wins
      }
      // FAILED or CANCELLED — overwrite.
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

  (void)pool_->enqueue(
      [this, job, dimension, metric, index_type, num_shards,
       replication_factor, shard_id, primary_term, segment_store]() {
        ExecuteSingleShardJob(job, dimension, metric, index_type, num_shards,
                              replication_factor, shard_id, primary_term,
                              segment_store);
      });

  return id;
}

void BackupManager::ExecuteSingleShardJob(
    std::shared_ptr<BackupJob> job,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type,
    uint32_t num_shards,
    uint32_t replication_factor,
    core::ShardId shard_id,
    uint64_t primary_term,
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

  auto shard_or = RunShardBackup(job->id, core::MakeCollectionId(job->collection_id),
                                 shard_id, primary_term, job->target,
                                 segment_store);
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
  shard_refs.push_back(
      BackupShardRef{static_cast<uint32_t>(core::ToUInt16(shard_id)),
                     shard_or->shard_manifest_key});

  auto final_status = FinalizeTopManifest(
      job, rt, dimension, metric, index_type, num_shards, replication_factor,
      shard_refs, vector_count, size_bytes);
  if (!final_status.ok()) return;  // FinalizeTopManifest already set FAILED.
  job->shards_completed.store(1);
  job->state.store(BackupState::COMPLETED);
}

core::Status BackupManager::FinalizeTopManifest(
    std::shared_ptr<BackupJob> job,
    const ResolvedTarget& rt,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type,
    uint32_t num_shards,
    uint32_t replication_factor,
    const std::vector<BackupShardRef>& shard_refs,
    uint64_t vector_count,
    uint64_t size_bytes) {
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
  return core::OkStatus();
}

// ============================================================================
// Distributed (multi-shard) backup
// ============================================================================

core::StatusOr<std::string> BackupManager::StartBackupDistributed(
    const std::string& collection_name,
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type,
    uint32_t replication_factor,
    const std::vector<core::ShardId>& shard_ids,
    const std::vector<uint64_t>& primary_terms,
    const BackupTarget& target,
    ShardBackupExecutor executor,
    const std::string& requested_backup_id) {
  if (collection_name.empty()) {
    return core::InvalidArgumentError("collection_name is required");
  }
  if (shard_ids.empty()) {
    return core::InvalidArgumentError("shard_ids is empty");
  }
  if (shard_ids.size() != primary_terms.size()) {
    return core::InvalidArgumentError(
        "shard_ids and primary_terms must have the same length");
  }
  if (!executor) {
    return core::InvalidArgumentError("executor is required");
  }
  auto rt = ResolveTarget(target);
  if (!rt.ok()) return rt.status();

  std::string id =
      requested_backup_id.empty() ? AllocateBackupId() : requested_backup_id;
  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(id);
    if (it != jobs_.end()) {
      auto s = it->second->state.load();
      if (s == BackupState::PENDING || s == BackupState::RUNNING ||
          s == BackupState::COMPLETED) {
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

  (void)pool_->enqueue(
      [this, job, dimension, metric, index_type, replication_factor,
       shard_ids, primary_terms, executor]() mutable {
        ExecuteDistributedJob(job, dimension, metric, index_type,
                              replication_factor, std::move(shard_ids),
                              std::move(primary_terms), std::move(executor));
      });

  return id;
}

void BackupManager::ExecuteDistributedJob(
    std::shared_ptr<BackupJob> job,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type,
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
    auto shard_result = executor(job->id, shard_ids[i], primary_terms[i],
                                  job->target);
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
    shard_refs.push_back(BackupShardRef{
        static_cast<uint32_t>(core::ToUInt16(shard_ids[i])),
        shard_result->shard_manifest_key});
    job->shards_completed.fetch_add(1);
  }

  auto final_status = FinalizeTopManifest(
      job, rt, dimension, metric, index_type,
      static_cast<uint32_t>(shard_ids.size()), replication_factor,
      shard_refs, vector_count, size_bytes);
  if (!final_status.ok()) return;  // already FAILED inside helper
  job->state.store(BackupState::COMPLETED);
}

void BackupManager::CleanupOnFailure(const ResolvedTarget& rt,
                                     const std::string& backup_id) {
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

core::StatusOr<BackupJobStatus> BackupManager::GetStatus(
    const std::string& backup_id) const {
  std::shared_ptr<BackupJob> job;
  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(backup_id);
    if (it == jobs_.end()) {
      return core::NotFoundError(
          absl::StrCat("Backup not found: ", backup_id));
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
  s.elapsed_seconds =
      std::chrono::duration<float>(now - job->start_time).count();
  return s;
}

core::Status BackupManager::Cancel(const std::string& backup_id) {
  std::lock_guard<std::mutex> lk(jobs_mutex_);
  auto it = jobs_.find(backup_id);
  if (it == jobs_.end()) {
    return core::NotFoundError(absl::StrCat("Backup not found: ", backup_id));
  }
  it->second->cancelled.store(true);
  return core::OkStatus();
}

std::vector<std::string> BackupManager::JobIds() const {
  std::vector<std::string> ids;
  std::lock_guard<std::mutex> lk(jobs_mutex_);
  ids.reserve(jobs_.size());
  for (const auto& [id, _] : jobs_) ids.push_back(id);
  std::sort(ids.begin(), ids.end());
  return ids;
}

// ============================================================================
// RestoreManager
// ============================================================================

RestoreManager::RestoreManager(RestoreManagerOptions opts)
    : opts_(std::move(opts)) {
  pool_ = std::make_unique<utils::ThreadPool>(
      static_cast<size_t>(std::max(1, opts_.max_concurrent_jobs)));
  std::error_code ec;
  std::filesystem::create_directories(opts_.staging_dir, ec);
}

std::string RestoreManager::AllocateRestoreId() {
  return absl::StrCat("rs-", RFC3339Now(), "-", RandomToken());
}

core::StatusOr<RestoreManager::ResolvedTarget> RestoreManager::ResolveTarget(
    const BackupTarget& source) {
  if (std::holds_alternative<std::monostate>(source)) {
    return core::InvalidArgumentError("BackupTarget source is unset");
  }
  if (auto* s3 = std::get_if<S3BackupTarget>(&source)) {
    if (!opts_.default_object_store) {
      return core::FailedPreconditionError(
          "Server is not configured with an S3 object store");
    }
    if (!opts_.s3_bucket.empty() && s3->bucket != opts_.s3_bucket) {
      return core::InvalidArgumentError(absl::StrCat(
          "Requested S3 bucket '", s3->bucket,
          "' does not match server-configured bucket '", opts_.s3_bucket,
          "'"));
    }
    ResolvedTarget r;
    r.store = opts_.default_object_store;
    r.display_prefix =
        absl::StrCat("s3://", s3->bucket,
                     s3->prefix.empty() ? "" : absl::StrCat("/", s3->prefix));
    return r;
  }
  if (auto* loc = std::get_if<LocalBackupTarget>(&source)) {
    if (opts_.local_root_allowlist.empty()) {
      return core::FailedPreconditionError(
          "LocalBackupTarget restore is disabled on this server");
    }
    if (!PathUnderRoot(opts_.local_root_allowlist, loc->path)) {
      return core::InvalidArgumentError(absl::StrCat(
          "LocalBackupTarget path '", loc->path,
          "' is not under the configured allowlist root '",
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
  return core::InvalidArgumentError("Unknown BackupTarget variant");
}

core::StatusOr<BackupManifestV1> RestoreManager::ReadManifest(
    const BackupTarget& source, const std::string& backup_id) {
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

core::Status RestoreManager::RunShardRestore(
    const BackupTarget& source,
    const std::string& backup_id,
    core::CollectionId target_collection_id,
    core::ShardId shard_id,
    std::shared_ptr<ISegmentStore> segment_store) {
  if (backup_id.empty()) {
    return core::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return core::InvalidArgumentError("segment_store is null");
  }
  auto rt_or = ResolveTarget(source);
  if (!rt_or.ok()) return rt_or.status();
  auto& rt = *rt_or;

  std::string s3_prefix;
  if (auto* s3 = std::get_if<S3BackupTarget>(&source)) {
    s3_prefix = s3->prefix;
  }

  // Fetch shard manifest.
  auto shard_key = BuildKey(s3_prefix,
                            BackupManifest::ShardManifestKey(
                                backup_id, core::ToUInt16(shard_id)));
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
      absl::StrCat("restore-", backup_id, "-shard-",
                   core::ToUInt16(shard_id), "-", RandomToken());
  std::filesystem::create_directories(staging_root, ec);

  // Inner cleanup helper — Status return preserves the original error
  // while still removing the staging tree.
  auto cleanup = [&]() {
    std::error_code rec;
    std::filesystem::remove_all(staging_root, rec);
  };

  // Restoring into a fresh target collection (different collection_id
  // than the backup recorded) requires remapping segment_ids too — the
  // original ids may still be live under the source collection on the
  // same data-node. Mirrors the cluster::ShardSegmentId scheme
  // (collection_id * 1000 + shard_index) so restored segments live in
  // the same id space the resolver hands out for fresh segments;
  // inlined here to keep the storage layer free of cluster/ deps.
  // The remap is a no-op when the manifest's collection_id already
  // matches the target.
  auto shard_segment_base = [](core::CollectionId cid, uint16_t shard) {
    return core::MakeSegmentId(core::ToUInt32(cid) * 1000 + shard);
  };
  const bool remap_needed = !mani_or->segments.empty() &&
      mani_or->segments.front().segment_id !=
          core::ToUInt32(shard_segment_base(target_collection_id,
                                            core::ToUInt16(shard_id)));

  for (size_t seg_index = 0; seg_index < mani_or->segments.size();
       ++seg_index) {
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
      // eagerly created on CreateCollection — that's exactly what we
      // want for the common one-segment-per-shard restore.
      auto new_seg_id = core::MakeSegmentId(
          core::ToUInt32(shard_segment_base(target_collection_id,
                                            core::ToUInt16(shard_id))) +
          static_cast<uint32_t>(seg_index));
      (*seg_or)->SetSegmentIdForRestore(new_seg_id);
      // Drop any existing segment at this id (the empty
      // CreateCollection-allocated one) so AddReplicatedSegment doesn't
      // see AlreadyExists. Idempotent — NotFound is treated as OK.
      (void)segment_store->DropSegment(new_seg_id,
                                       /*delete_files=*/false);
    }

    auto add_status =
        segment_store->AddReplicatedSegment(std::move(*seg_or));
    if (!add_status.ok()) {
      cleanup();
      return add_status;
    }
  }

  cleanup();
  return core::OkStatus();
}

core::StatusOr<std::string> RestoreManager::StartRestoreSingleShard(
    const BackupTarget& source,
    const std::string& backup_id,
    core::CollectionId target_collection_id,
    core::ShardId shard_id,
    std::shared_ptr<ISegmentStore> segment_store) {
  if (backup_id.empty()) {
    return core::InvalidArgumentError("backup_id is required");
  }
  if (!segment_store) {
    return core::InvalidArgumentError("segment_store is null");
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
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    jobs_[id] = job;
  }

  (void)pool_->enqueue([this, job, shard_id, segment_store]() {
    ExecuteSingleShardJob(job, shard_id, segment_store);
  });

  return id;
}

void RestoreManager::ExecuteSingleShardJob(
    std::shared_ptr<RestoreJob> job,
    core::ShardId shard_id,
    std::shared_ptr<ISegmentStore> segment_store) {
  job->state.store(BackupState::RUNNING);

  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }
  auto status = RunShardRestore(job->source, job->backup_id,
                                core::MakeCollectionId(job->target_collection_id),
                                shard_id, segment_store);
  if (!status.ok()) {
    job->SetError(std::string(status.message()));
    job->state.store(BackupState::FAILED);
    return;
  }
  job->shards_completed.store(1);
  job->state.store(BackupState::COMPLETED);
}

core::StatusOr<std::string> RestoreManager::StartRestoreDistributed(
    const BackupTarget& source,
    const std::string& backup_id,
    core::CollectionId target_collection_id,
    const std::vector<core::ShardId>& shard_ids,
    ShardRestoreExecutor executor) {
  if (backup_id.empty()) {
    return core::InvalidArgumentError("backup_id is required");
  }
  if (shard_ids.empty()) {
    return core::InvalidArgumentError("shard_ids is empty");
  }
  if (!executor) {
    return core::InvalidArgumentError("executor is required");
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
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    jobs_[id] = job;
  }

  std::vector<core::ShardId> shards_copy = shard_ids;
  (void)pool_->enqueue(
      [this, job, shards_copy = std::move(shards_copy),
       executor = std::move(executor)]() mutable {
        ExecuteDistributedJob(job, std::move(shards_copy),
                              std::move(executor));
      });

  return id;
}

void RestoreManager::ExecuteDistributedJob(
    std::shared_ptr<RestoreJob> job,
    std::vector<core::ShardId> shard_ids,
    ShardRestoreExecutor executor) {
  job->state.store(BackupState::RUNNING);
  if (job->cancelled.load()) {
    job->state.store(BackupState::CANCELLED);
    return;
  }
  for (auto shard_id : shard_ids) {
    auto status = executor(
        job->source, job->backup_id,
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

core::StatusOr<RestoreJobStatus> RestoreManager::GetStatus(
    const std::string& restore_id) const {
  std::shared_ptr<RestoreJob> job;
  {
    std::lock_guard<std::mutex> lk(jobs_mutex_);
    auto it = jobs_.find(restore_id);
    if (it == jobs_.end()) {
      return core::NotFoundError(
          absl::StrCat("Restore not found: ", restore_id));
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
  s.elapsed_seconds =
      std::chrono::duration<float>(now - job->start_time).count();
  return s;
}

}  // namespace storage
}  // namespace gvdb
