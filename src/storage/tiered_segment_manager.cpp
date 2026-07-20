// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/tiered_segment_manager.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <mutex>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "utils/logger.h"
#include "utils/metrics.h"

namespace gvdb {
namespace storage {

// Files produced by Segment::Flush(), used for S3 upload/download/delete.
static constexpr std::array<const char*, 3> kSegmentFiles = {
    "metadata.txt", "vectors.bin", "index.faiss"
};

// Upload retry backoff cap. Failures retry forever with exponential backoff up
// to this ceiling so a long object-store outage does not spin the retry thread.
static constexpr auto kMaxRetryDelay = std::chrono::minutes(5);

// After this many consecutive failures a stuck upload is logged at Error rather
// than Warn so operators notice segments that are not reaching the cold tier.
static constexpr uint32_t kUploadErrorThreshold = 5;

TieredSegmentManager::TieredSegmentManager(
    std::unique_ptr<SegmentManager> local,
    std::unique_ptr<IObjectStore> object_store,
    std::unique_ptr<SegmentCache> cache,
    const std::string& prefix,
    int upload_threads,
    std::chrono::milliseconds retry_base_delay)
    : local_(std::move(local)),
      object_store_(std::move(object_store)),
      cache_(std::move(cache)),
      prefix_(prefix),
      upload_pool_(std::make_unique<utils::ThreadPool>(
          std::max(1, upload_threads))),
      retry_base_delay_(retry_base_delay) {
  retry_thread_ = std::thread([this]() { RetryLoop(); });
}

TieredSegmentManager::~TieredSegmentManager() {
  // Stop the retry thread before draining the pool: signal, wake, join. Once it
  // has exited no new upload tasks can be enqueued, so the pool can drain any
  // in-flight upload cleanly (ThreadPool destructor waits for pending tasks).
  {
    std::lock_guard lock(upload_mutex_);
    retry_stop_ = true;
  }
  retry_cv_.notify_all();
  if (retry_thread_.joinable()) retry_thread_.join();
  upload_pool_.reset();
}

// ============================================================================
// Segment Lifecycle — delegate to local, add S3 on top
// ============================================================================

core::StatusOr<core::SegmentId> TieredSegmentManager::CreateSegment(
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric) {
  return local_->CreateSegment(collection_id, dimension, metric);
}

core::Status TieredSegmentManager::CreateSegmentWithId(
    core::SegmentId segment_id,
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type) {
  return local_->CreateSegmentWithId(
      segment_id, collection_id, dimension, metric, index_type);
}

Segment* TieredSegmentManager::GetSegment(core::SegmentId id) {
  // Try local first
  auto* seg = local_->GetSegment(id);
  if (seg != nullptr) return seg;

  // Try lazy load from S3
  auto status = EnsureSegmentLoaded(id);
  if (!status.ok()) return nullptr;

  return local_->GetSegment(id);
}

const Segment* TieredSegmentManager::GetSegment(core::SegmentId id) const {
  const auto* seg = local_->GetSegment(id);
  if (seg != nullptr) return seg;

  auto status = EnsureSegmentLoaded(id);
  if (!status.ok()) return nullptr;

  return local_->GetSegment(id);
}

core::Status TieredSegmentManager::SealSegment(
    core::SegmentId id, const core::IndexConfig& index_config) {
  return local_->SealSegment(id, index_config);
}

core::Status TieredSegmentManager::FlushSegment(core::SegmentId id) {
  // 1. Flush to local disk
  auto status = local_->FlushSegment(id);
  if (!status.ok()) return status;

  // 2. Async upload to object storage. A failed upload is not dropped: RunUpload
  //    records it in failed_uploads_ and the retry thread re-drives it.
  auto seg_id = id;
  {
    std::lock_guard lock(upload_mutex_);
    uploading_.insert(core::ToUInt32(seg_id));
  }

  upload_pool_->enqueue([this, seg_id]() { RunUpload(seg_id, 0); });

  return core::OkStatus();
}

core::Status TieredSegmentManager::DropSegment(
    core::SegmentId id, bool delete_files) {
  auto status = local_->DropSegment(id, delete_files);

  // Copy remote info under lock, then do S3 I/O without holding the lock
  // to avoid blocking concurrent readers during potentially slow network calls.
  auto seg_key = core::ToUInt32(id);
  RemoteSegmentInfo info;
  bool is_remote = false;
  {
    std::shared_lock lock(remote_mutex_);
    auto it = remote_segments_.find(seg_key);
    if (it != remote_segments_.end()) {
      info = it->second;
      is_remote = true;
    }
  }

  if (is_remote) {
    auto col_id = core::ToUInt32(info.collection_id);

    // Delete files from S3 (best-effort, don't block on failure)
    for (const auto& filename : kSegmentFiles) {
      (void)object_store_->DeleteObject(MakeS3Key(col_id, seg_key, filename));
    }

    // Update manifest (best-effort)
    ManifestEntry dummy;
    dummy.segment_id = seg_key;
    (void)UpdateManifest(dummy, /*remove=*/true);
  }

  // Remove from remote tracking
  {
    std::unique_lock lock(remote_mutex_);
    remote_segments_.erase(seg_key);
  }

  return status;
}

core::Status TieredSegmentManager::LoadSegment(core::SegmentId id) {
  return EnsureSegmentLoaded(id);
}

core::Status TieredSegmentManager::AddReplicatedSegment(
    std::unique_ptr<Segment> segment) {
  return local_->AddReplicatedSegment(std::move(segment));
}

// ============================================================================
// Data Operations — delegate with lazy load fallback
// ============================================================================

core::Status TieredSegmentManager::WriteVectors(
    core::SegmentId segment_id,
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {
  return local_->WriteVectors(segment_id, vectors, ids);
}

core::StatusOr<std::vector<core::Vector>> TieredSegmentManager::ReadVectors(
    core::SegmentId segment_id,
    const std::vector<core::VectorId>& ids) const {
  auto result = local_->ReadVectors(segment_id, ids);
  if (result.ok()) return result;

  // Try lazy load from S3
  auto status = EnsureSegmentLoaded(segment_id);
  if (!status.ok()) return result;  // return original error

  return local_->ReadVectors(segment_id, ids);
}

core::StatusOr<core::SearchResult> TieredSegmentManager::SearchSegment(
    core::SegmentId segment_id,
    const core::Vector& query, int k) const {
  auto result = local_->SearchSegment(segment_id, query, k);
  if (result.ok()) return result;

  auto status = EnsureSegmentLoaded(segment_id);
  if (!status.ok()) return result;

  return local_->SearchSegment(segment_id, query, k);
}

core::StatusOr<core::SearchResult> TieredSegmentManager::SearchCollection(
    core::CollectionId collection_id,
    const core::Vector& query, int k) const {
  // Collect remote segment IDs under lock, then load outside the lock
  // to avoid recursive shared_mutex acquisition (UB per C++ standard).
  std::vector<core::SegmentId> to_load;
  {
    std::shared_lock lock(remote_mutex_);
    for (const auto& [seg_key, info] : remote_segments_) {
      if (info.collection_id == collection_id) {
        auto seg_id = static_cast<core::SegmentId>(seg_key);
        if (local_->GetSegment(seg_id) == nullptr) {
          to_load.push_back(seg_id);
        }
      }
    }
  }
  for (auto seg_id : to_load) {
    (void)EnsureSegmentLoaded(seg_id);
  }

  return local_->SearchCollection(collection_id, query, k);
}

// ============================================================================
// Active Segment Rotation — pure delegation
// ============================================================================

void TieredSegmentManager::SetSealCallback(SealCallback callback) {
  local_->SetSealCallback(std::move(callback));
}

Segment* TieredSegmentManager::GetWritableSegment(
    core::CollectionId collection_id, size_t required_bytes) {
  return local_->GetWritableSegment(collection_id, required_bytes);
}

std::vector<Segment*> TieredSegmentManager::GetQueryableSegments(
    core::CollectionId collection_id) const {
  // Ensure remote segments for this collection are loaded before querying.
  std::vector<core::SegmentId> to_load;
  {
    std::shared_lock lock(remote_mutex_);
    for (const auto& [seg_key, info] : remote_segments_) {
      if (info.collection_id == collection_id) {
        auto seg_id = static_cast<core::SegmentId>(seg_key);
        if (local_->GetSegment(seg_id) == nullptr) {
          to_load.push_back(seg_id);
        }
      }
    }
  }
  for (auto seg_id : to_load) {
    (void)EnsureSegmentLoaded(seg_id);
  }
  return local_->GetQueryableSegments(collection_id);
}

std::vector<core::SegmentId> TieredSegmentManager::GetAllSegmentIds() const {
  auto local_ids = local_->GetAllSegmentIds();

  // Add remote segment IDs not yet loaded locally (O(1) lookup via set)
  absl::flat_hash_set<uint32_t> local_set;
  local_set.reserve(local_ids.size());
  for (auto id : local_ids) local_set.insert(core::ToUInt32(id));

  std::shared_lock lock(remote_mutex_);
  for (const auto& [seg_key, _] : remote_segments_) {
    if (!local_set.contains(seg_key)) {
      local_ids.push_back(static_cast<core::SegmentId>(seg_key));
    }
  }
  return local_ids;
}

void TieredSegmentManager::RunTTLSweepLoop(const std::atomic<bool>& shutdown) {
  local_->RunTTLSweepLoop(shutdown);
}

// ============================================================================
// Management
// ============================================================================

absl::Status TieredSegmentManager::LoadAllSegments() {
  // 1. Load local segments
  auto status = local_->LoadAllSegments();
  if (!status.ok()) return status;

  // 2. Discover remote segments from S3 manifest
  auto s3_status = DiscoverSegmentsFromS3();
  if (!s3_status.ok()) {
    (void)s3_status;
    // Non-fatal, local segments still work
  }

  // 3. Re-drive segments that are FLUSHED locally but missing from the remote
  //    manifest: their original upload never reached the cold tier.
  RedriveUnuploadedSegments();

  return absl::OkStatus();
}

std::vector<core::SegmentId> TieredSegmentManager::GetCollectionSegments(
    core::CollectionId collection_id) const {
  auto local_segs = local_->GetCollectionSegments(collection_id);

  // Add remote segments for this collection (O(1) lookup via set)
  absl::flat_hash_set<uint32_t> local_set;
  local_set.reserve(local_segs.size());
  for (auto id : local_segs) local_set.insert(core::ToUInt32(id));

  std::shared_lock lock(remote_mutex_);
  for (const auto& [seg_key, info] : remote_segments_) {
    if (info.collection_id == collection_id && !local_set.contains(seg_key)) {
      local_segs.push_back(static_cast<core::SegmentId>(seg_key));
    }
  }
  return local_segs;
}

size_t TieredSegmentManager::GetSegmentCount() const {
  auto local_count = local_->GetSegmentCount();
  std::shared_lock lock(remote_mutex_);
  // Count remote segments not loaded locally
  size_t remote_only = 0;
  for (const auto& [seg_key, _] : remote_segments_) {
    if (local_->GetSegment(static_cast<core::SegmentId>(seg_key)) == nullptr) {
      ++remote_only;
    }
  }
  return local_count + remote_only;
}

size_t TieredSegmentManager::GetTotalMemoryUsage() const {
  return local_->GetTotalMemoryUsage();
}

void TieredSegmentManager::Clear() {
  local_->Clear();
  std::unique_lock lock(remote_mutex_);
  remote_segments_.clear();
}

// ============================================================================
// S3-specific accessors
// ============================================================================

size_t TieredSegmentManager::GetRemoteSegmentCount() const {
  std::shared_lock lock(remote_mutex_);
  return remote_segments_.size();
}

bool TieredSegmentManager::IsUploading(core::SegmentId id) const {
  std::lock_guard lock(upload_mutex_);
  return uploading_.count(core::ToUInt32(id)) > 0;
}

std::vector<core::SegmentId> TieredSegmentManager::GetFailedUploads() const {
  std::lock_guard lock(upload_mutex_);
  std::vector<core::SegmentId> result;
  result.reserve(failed_uploads_.size());
  for (const auto& [key, _] : failed_uploads_) {
    result.push_back(static_cast<core::SegmentId>(key));
  }
  return result;
}

// ============================================================================
// Internal S3 methods
// ============================================================================

core::Status TieredSegmentManager::UploadSegmentToS3(core::SegmentId id) {
  auto seg_key = core::ToUInt32(id);
  auto* seg = local_->GetSegment(id);
  if (seg == nullptr) {
    return core::NotFoundError(
        absl::StrCat("Segment not found for upload: ", seg_key));
  }

  auto col_id = core::ToUInt32(seg->GetCollectionId());
  auto seg_path = absl::StrCat(
      local_->GetBasePath(), "/segment_", seg_key);

  // Upload each file
  for (const auto& filename : kSegmentFiles) {
    auto local_file = absl::StrCat(seg_path, "/", filename);
    if (!std::filesystem::exists(local_file)) continue;

    auto s3_key = MakeS3Key(col_id, seg_key, filename);
    auto status = object_store_->PutObjectFromFile(s3_key, local_file);
    if (!status.ok()) return status;
  }

  // Update manifest
  ManifestEntry entry;
  entry.segment_id = seg_key;
  entry.collection_id = col_id;
  entry.dimension = seg->GetDimension();
  entry.metric = static_cast<int32_t>(seg->GetMetric());
  entry.index_type = static_cast<int32_t>(seg->GetIndexType());
  entry.vector_count = seg->GetVectorCount();
  // Approximate size
  entry.size_bytes = seg->GetVectorCount() * seg->GetDimension() * sizeof(float);

  auto manifest_status = UpdateManifest(entry);
  if (!manifest_status.ok()) {
    (void)manifest_status;
  }

  // Register in cache
  if (cache_) {
    (void)cache_->RegisterSegment(id, entry.size_bytes);
  }

  // Track as remote
  {
    std::unique_lock lock(remote_mutex_);
    remote_segments_[seg_key] = RemoteSegmentInfo{
        seg->GetCollectionId(),
        seg->GetDimension(),
        seg->GetMetric(),
        seg->GetIndexType(),
        seg->GetVectorCount(),
        entry.size_bytes};
  }

  // Segment uploaded successfully
  return core::OkStatus();
}

void TieredSegmentManager::RunUpload(
    core::SegmentId id, uint32_t prior_failures) {
  auto upload_status = UploadSegmentToS3(id);
  auto seg_key = core::ToUInt32(id);

  std::lock_guard lock(upload_mutex_);
  uploading_.erase(seg_key);

  if (upload_status.ok()) {
    if (prior_failures > 0) {
      utils::Logger::Instance().Info(
          "Object storage upload recovered for segment {} after {} failed "
          "attempt(s)", seg_key, prior_failures);
    }
    UpdateFailedUploadsMetricLocked();
    return;
  }

  // Record the failure with a fresh backoff so the retry thread re-drives it.
  uint32_t attempts = prior_failures + 1;
  failed_uploads_[seg_key] = FailedUploadState{
      attempts, std::chrono::steady_clock::now() + BackoffDelay(attempts)};

  if (attempts >= kUploadErrorThreshold) {
    utils::Logger::Instance().Error(
        "Object storage upload still failing for segment {} after {} "
        "attempts: {}", seg_key, attempts, upload_status.message());
  } else {
    utils::Logger::Instance().Warn(
        "Object storage upload failed for segment {} (attempt {}): {}",
        seg_key, attempts, upload_status.message());
  }

  UpdateFailedUploadsMetricLocked();
  retry_cv_.notify_one();
}

std::chrono::steady_clock::duration TieredSegmentManager::BackoffDelay(
    uint32_t attempts) const {
  // Exponential: base * 2^(attempts-1), capped at kMaxRetryDelay.
  auto delay = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
      retry_base_delay_);
  const auto cap = std::chrono::duration_cast<
      std::chrono::steady_clock::duration>(kMaxRetryDelay);
  for (uint32_t i = 1; i < attempts; ++i) {
    delay *= 2;
    if (delay >= cap) return cap;
  }
  return std::min(delay, cap);
}

void TieredSegmentManager::RetryLoop() {
  std::unique_lock<std::mutex> lock(upload_mutex_);
  while (!retry_stop_) {
    auto now = std::chrono::steady_clock::now();

    // Collect segments whose backoff has elapsed and find the next wake time.
    std::vector<std::pair<core::SegmentId, uint32_t>> ready;
    bool have_pending = false;
    auto next_wake = now + kMaxRetryDelay;
    for (const auto& [key, state] : failed_uploads_) {
      if (state.next_retry <= now) {
        ready.emplace_back(static_cast<core::SegmentId>(key), state.attempts);
      } else {
        have_pending = true;
        next_wake = std::min(next_wake, state.next_retry);
      }
    }

    if (!ready.empty()) {
      // Move each eligible segment back to in-flight and re-enqueue. The pool is
      // guaranteed alive here: the destructor joins this thread before resetting
      // it. RunUpload will re-add to failed_uploads_ if the attempt fails again.
      for (const auto& entry : ready) {
        core::SegmentId seg_id = entry.first;
        uint32_t attempts = entry.second;
        failed_uploads_.erase(core::ToUInt32(seg_id));
        uploading_.insert(core::ToUInt32(seg_id));
        upload_pool_->enqueue(
            [this, seg_id, attempts]() { RunUpload(seg_id, attempts); });
      }
      UpdateFailedUploadsMetricLocked();
      continue;  // re-evaluate immediately
    }

    if (have_pending) {
      retry_cv_.wait_until(lock, next_wake);
    } else {
      retry_cv_.wait(lock);
    }
  }
}

void TieredSegmentManager::RedriveUnuploadedSegments() {
  // Segments recovered from local disk report SEALED once their index is
  // rebuilt on load (or FLUSHED if the rebuild was skipped); either way their
  // flush files exist on disk. Any such segment that is not present in the
  // remote manifest never reached the cold tier (the process died or the
  // object store was unreachable during the original upload), so re-enqueue
  // it to restore durability. Only called from LoadAllSegments, where every
  // in-memory segment came from disk; a SEALED-but-never-flushed segment
  // cannot appear here.
  auto local_ids = local_->GetAllSegmentIds();

  std::vector<core::SegmentId> to_upload;
  {
    std::shared_lock rlock(remote_mutex_);
    for (auto id : local_ids) {
      if (remote_segments_.count(core::ToUInt32(id)) > 0) continue;
      auto* seg = local_->GetSegment(id);
      if (seg == nullptr) continue;
      auto state = seg->GetState();
      if (state != core::SegmentState::FLUSHED &&
          state != core::SegmentState::SEALED) {
        continue;
      }
      to_upload.push_back(id);
    }
  }

  for (auto id : to_upload) {
    auto seg_key = core::ToUInt32(id);
    {
      std::lock_guard lock(upload_mutex_);
      // Skip if already tracked to avoid enqueuing a segment twice.
      if (uploading_.count(seg_key) > 0 ||
          failed_uploads_.count(seg_key) > 0) {
        continue;
      }
      uploading_.insert(seg_key);
    }
    utils::Logger::Instance().Info(
        "Re-driving segment {} to object storage (flushed locally but missing "
        "from remote manifest)", seg_key);
    upload_pool_->enqueue([this, id]() { RunUpload(id, 0); });
  }
}

void TieredSegmentManager::UpdateFailedUploadsMetricLocked() const {
  utils::MetricsRegistry::Instance().SetPendingFailedUploads(
      failed_uploads_.size());
}

core::Status TieredSegmentManager::DownloadSegmentFromS3(
    core::SegmentId id) const {
  auto seg_key = core::ToUInt32(id);

  RemoteSegmentInfo info;
  {
    std::shared_lock lock(remote_mutex_);
    auto it = remote_segments_.find(seg_key);
    if (it == remote_segments_.end()) {
      return core::NotFoundError(
          absl::StrCat("Segment not in remote index: ", seg_key));
    }
    info = it->second;
  }

  auto col_id = core::ToUInt32(info.collection_id);
  auto local_seg_path = absl::StrCat(
      local_->GetBasePath(), "/segment_", seg_key);
  std::filesystem::create_directories(local_seg_path);

  // Download each file
  for (const auto& filename : kSegmentFiles) {
    auto s3_key = MakeS3Key(col_id, seg_key, filename);
    auto exists = object_store_->ObjectExists(s3_key);
    if (!exists.ok() || !*exists) continue;

    auto local_file = absl::StrCat(local_seg_path, "/", filename);
    auto status = object_store_->GetObjectToFile(s3_key, local_file);
    if (!status.ok()) return status;
  }

  // Register in cache
  if (cache_) {
    // Evict if needed before registering
    if (cache_->GetCachedSize() + info.size_bytes > cache_->GetMaxCacheBytes()) {
      auto evicted = cache_->Evict(info.size_bytes);
      if (evicted.ok()) {
        for (auto evicted_id : *evicted) {
          (void)local_->DropSegment(evicted_id, /*delete_files=*/true);
        }
      }
    }
    (void)cache_->RegisterSegment(id, info.size_bytes);
  }

  // Segment downloaded successfully
  return core::OkStatus();
}

core::Status TieredSegmentManager::DiscoverSegmentsFromS3() {
  auto manifest_key = absl::StrCat(prefix_, "/manifest.json");

  // Try reading manifest
  auto manifest_result = object_store_->GetObject(manifest_key);
  if (manifest_result.ok()) {
    auto entries = SegmentManifest::Deserialize(*manifest_result);
    if (entries.ok()) {
      std::unique_lock lock(remote_mutex_);
      for (const auto& e : *entries) {
        // Skip if already loaded locally
        if (local_->GetSegment(static_cast<core::SegmentId>(e.segment_id))
            != nullptr) {
          continue;
        }
        remote_segments_[e.segment_id] = RemoteSegmentInfo{
            static_cast<core::CollectionId>(e.collection_id),
            static_cast<core::Dimension>(e.dimension),
            static_cast<core::MetricType>(e.metric),
            static_cast<core::IndexType>(e.index_type),
            e.vector_count,
            e.size_bytes};
      }

      std::lock_guard mlock(manifest_mutex_);
      manifest_json_ = *manifest_result;

      // Manifest loaded successfully
      return core::OkStatus();
    }
  }

  // Fallback: list objects (slower but works if manifest is missing/corrupt)
  // Manifest not found or corrupt — fall back to ListObjects
  auto list_result = object_store_->ListObjects(
      absl::StrCat(prefix_, "/collections/"));
  if (!list_result.ok()) return list_result.status();

  // Parse segment IDs from keys like: prefix/collections/1/segments/5/metadata.txt
  // For now, just log that we'd need to download metadata.txt for each
  // This is a degraded path — the manifest is the primary discovery mechanism
  (void)list_result;
  return core::OkStatus();
}

core::Status TieredSegmentManager::UpdateManifest(
    const ManifestEntry& entry, bool remove) {
  std::lock_guard lock(manifest_mutex_);

  if (remove) {
    manifest_json_ = SegmentManifest::RemoveEntry(
        manifest_json_, entry.segment_id);
  } else {
    manifest_json_ = SegmentManifest::AddEntry(manifest_json_, entry);
  }

  auto manifest_key = absl::StrCat(prefix_, "/manifest.json");
  return object_store_->PutObject(manifest_key, manifest_json_);
}

core::Status TieredSegmentManager::EnsureSegmentLoaded(
    core::SegmentId id) const {
  // Already loaded locally?
  if (local_->GetSegment(id) != nullptr) return core::OkStatus();

  // Known remote segment?
  auto seg_key = core::ToUInt32(id);
  {
    std::shared_lock lock(remote_mutex_);
    if (remote_segments_.count(seg_key) == 0) {
      return core::NotFoundError(
          absl::StrCat("Segment not found locally or remotely: ", seg_key));
    }
  }

  // Check if already in local cache
  if (cache_ && cache_->HasSegment(id)) {
    cache_->Touch(id);
    return local_->LoadSegment(id);
  }

  // Download from S3
  auto status = DownloadSegmentFromS3(id);
  if (!status.ok()) return status;

  // Load into memory
  return local_->LoadSegment(id);
}

std::string TieredSegmentManager::MakeS3Key(
    uint32_t collection_id, uint32_t segment_id,
    const std::string& filename) const {
  return absl::StrCat(prefix_, "/collections/", collection_id,
                      "/segments/", segment_id, "/", filename);
}

const std::string& TieredSegmentManager::GetLocalBasePath() const {
  return local_->GetBasePath();
}

}  // namespace storage
}  // namespace gvdb
