// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// TieredSegmentManager, object store side: upload with retry and
// backoff, download, discovery and the manifest.

#include <algorithm>
#include <array>
#include <filesystem>
#include <mutex>
#include <vector>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "core/status.h"
#include "storage/segment/tiered_segment_manager.h"

namespace gvdb {
namespace storage {

namespace {

// Upload retry backoff cap. Failures retry forever with exponential backoff up
// to this ceiling so a long object-store outage does not spin the retry thread.
constexpr auto kMaxRetryDelay = std::chrono::minutes(5);

// After this many consecutive failures a stuck upload is logged at Error rather
// than Warn so operators notice segments that are not reaching the cold tier.
constexpr uint32_t kUploadErrorThreshold = 5;

}  // namespace

// ============================================================================
// S3-specific accessors
// ============================================================================

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

absl::Status TieredSegmentManager::UploadSegmentToS3(core::SegmentId id) {
  auto seg_key = core::ToUInt32(id);
  auto* seg = local_->GetSegment(id);
  if (seg == nullptr) {
    return absl::NotFoundError(absl::StrCat("Segment not found for upload: ", seg_key));
  }

  auto col_id = core::ToUInt32(seg->GetCollectionId());
  auto seg_path = absl::StrCat(local_->GetBasePath(), "/segment_", seg_key);

  // Upload each file
  for (const auto& filename : kSegmentFiles) {
    auto local_file = absl::StrCat(seg_path, "/", filename);
    if (!std::filesystem::exists(local_file)) continue;

    auto s3_key = MakeS3Key(col_id, seg_key, filename);
    RETURN_IF_ERROR(object_store_->PutObjectFromFile(s3_key, local_file));
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
    remote_segments_[seg_key] =
        RemoteSegmentInfo{seg->GetCollectionId(), seg->GetDimension(),   seg->GetMetric(),
                          seg->GetIndexType(),    seg->GetVectorCount(), entry.size_bytes};
  }

  // Segment uploaded successfully
  return absl::OkStatus();
}

void TieredSegmentManager::RunUpload(core::SegmentId id, uint32_t prior_failures) {
  auto upload_status = UploadSegmentToS3(id);
  auto seg_key = core::ToUInt32(id);

  std::lock_guard lock(upload_mutex_);
  uploading_.erase(seg_key);

  if (upload_status.ok()) {
    if (prior_failures > 0) {
      common::Logger::Instance().Info(
          "Object storage upload recovered for segment {} after {} failed "
          "attempt(s)",
          seg_key, prior_failures);
    }
    UpdateFailedUploadsMetricLocked();
    return;
  }

  // Record the failure with a fresh backoff so the retry thread re-drives it.
  uint32_t attempts = prior_failures + 1;
  failed_uploads_[seg_key] =
      FailedUploadState{attempts, std::chrono::steady_clock::now() + BackoffDelay(attempts)};

  if (attempts >= kUploadErrorThreshold) {
    common::Logger::Instance().Error(
        "Object storage upload still failing for segment {} after {} "
        "attempts: {}",
        seg_key, attempts, upload_status.message());
  } else {
    common::Logger::Instance().Warn("Object storage upload failed for segment {} (attempt {}): {}",
                                    seg_key, attempts, upload_status.message());
  }

  UpdateFailedUploadsMetricLocked();
  retry_cv_.notify_one();
}

std::chrono::steady_clock::duration TieredSegmentManager::BackoffDelay(uint32_t attempts) const {
  // Exponential: base * 2^(attempts-1), capped at kMaxRetryDelay.
  auto delay = std::chrono::duration_cast<std::chrono::steady_clock::duration>(retry_base_delay_);
  const auto cap = std::chrono::duration_cast<std::chrono::steady_clock::duration>(kMaxRetryDelay);
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
        upload_pool_->Enqueue([this, seg_id, attempts]() { RunUpload(seg_id, attempts); });
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
      if (state != core::SegmentState::FLUSHED && state != core::SegmentState::SEALED) {
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
      if (uploading_.count(seg_key) > 0 || failed_uploads_.count(seg_key) > 0) {
        continue;
      }
      uploading_.insert(seg_key);
    }
    common::Logger::Instance().Info(
        "Re-driving segment {} to object storage (flushed locally but missing "
        "from remote manifest)",
        seg_key);
    upload_pool_->Enqueue([this, id]() { RunUpload(id, 0); });
  }
}

void TieredSegmentManager::UpdateFailedUploadsMetricLocked() const {
  common::MetricsRegistry::Instance().SetPendingFailedUploads(failed_uploads_.size());
}

absl::Status TieredSegmentManager::DownloadSegmentFromS3(core::SegmentId id) const {
  auto seg_key = core::ToUInt32(id);

  RemoteSegmentInfo info;
  {
    std::shared_lock lock(remote_mutex_);
    auto it = remote_segments_.find(seg_key);
    if (it == remote_segments_.end()) {
      return absl::NotFoundError(absl::StrCat("Segment not in remote index: ", seg_key));
    }
    info = it->second;
  }

  auto col_id = core::ToUInt32(info.collection_id);
  auto local_seg_path = absl::StrCat(local_->GetBasePath(), "/segment_", seg_key);
  std::filesystem::create_directories(local_seg_path);

  // Download each file
  for (const auto& filename : kSegmentFiles) {
    auto s3_key = MakeS3Key(col_id, seg_key, filename);
    auto exists = object_store_->ObjectExists(s3_key);
    if (!exists.ok() || !*exists) continue;

    auto local_file = absl::StrCat(local_seg_path, "/", filename);
    RETURN_IF_ERROR(object_store_->GetObjectToFile(s3_key, local_file));
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
  return absl::OkStatus();
}

absl::Status TieredSegmentManager::DiscoverSegmentsFromS3() {
  auto manifest_key = absl::StrCat(prefix_, "/manifest.json");

  // Try reading manifest
  auto manifest_result = object_store_->GetObject(manifest_key);
  if (manifest_result.ok()) {
    auto entries = SegmentManifest::Deserialize(*manifest_result);
    if (entries.ok()) {
      std::unique_lock lock(remote_mutex_);
      for (const auto& e : *entries) {
        // Skip if already loaded locally
        if (local_->GetSegment(static_cast<core::SegmentId>(e.segment_id)) != nullptr) {
          continue;
        }
        remote_segments_[e.segment_id] =
            RemoteSegmentInfo{static_cast<core::CollectionId>(e.collection_id),
                              static_cast<core::Dimension>(e.dimension),
                              static_cast<core::MetricType>(e.metric),
                              static_cast<core::IndexType>(e.index_type),
                              e.vector_count,
                              e.size_bytes};
      }

      std::lock_guard mlock(manifest_mutex_);
      manifest_json_ = *manifest_result;

      // Manifest loaded successfully
      return absl::OkStatus();
    }
  }

  // Fallback: list objects (slower but works if manifest is missing/corrupt)
  // Manifest not found or corrupt: fall back to ListObjects
  auto list_result = object_store_->ListObjects(absl::StrCat(prefix_, "/collections/"));
  if (!list_result.ok()) return list_result.status();

  // Parse segment IDs from keys like: prefix/collections/1/segments/5/metadata.txt
  // For now, just log that we'd need to download metadata.txt for each
  // This is a degraded path: the manifest is the primary discovery mechanism
  (void)list_result;
  return absl::OkStatus();
}

absl::Status TieredSegmentManager::UpdateManifest(const ManifestEntry& entry, bool remove) {
  std::lock_guard lock(manifest_mutex_);

  if (remove) {
    manifest_json_ = SegmentManifest::RemoveEntry(manifest_json_, entry.segment_id);
  } else {
    manifest_json_ = SegmentManifest::AddEntry(manifest_json_, entry);
  }

  auto manifest_key = absl::StrCat(prefix_, "/manifest.json");
  return object_store_->PutObject(manifest_key, manifest_json_);
}

absl::Status TieredSegmentManager::EnsureSegmentLoaded(core::SegmentId id) const {
  // Already loaded locally?
  if (local_->GetSegment(id) != nullptr) return absl::OkStatus();

  // Known remote segment?
  auto seg_key = core::ToUInt32(id);
  {
    std::shared_lock lock(remote_mutex_);
    if (remote_segments_.count(seg_key) == 0) {
      return absl::NotFoundError(absl::StrCat("Segment not found locally or remotely: ", seg_key));
    }
  }

  // Check if already in local cache
  if (cache_ && cache_->HasSegment(id)) {
    cache_->Touch(id);
    return local_->LoadSegment(id);
  }

  // Download from S3
  RETURN_IF_ERROR(DownloadSegmentFromS3(id));

  // Load into memory
  return local_->LoadSegment(id);
}

std::string TieredSegmentManager::MakeS3Key(uint32_t collection_id, uint32_t segment_id,
                                            const std::string& filename) const {
  return absl::StrCat(prefix_, "/collections/", collection_id, "/segments/", segment_id, "/",
                      filename);
}

}  // namespace storage
}  // namespace gvdb
