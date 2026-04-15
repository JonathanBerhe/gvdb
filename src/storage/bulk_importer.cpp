// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/bulk_importer.h"

#include <chrono>
#include <filesystem>
#include <functional>
#include <random>

#include "absl/strings/str_cat.h"
#include "storage/npy_reader.h"
#include "utils/logger.h"

namespace gvdb {
namespace storage {

BulkImporter::BulkImporter(std::shared_ptr<ISegmentStore> segment_store,
                           IObjectStore* object_store,
                           const std::string& temp_dir,
                           int max_concurrent)
    : segment_store_(std::move(segment_store)),
      object_store_(object_store),
      temp_dir_(temp_dir),
      max_concurrent_(max_concurrent),
      active_imports_(0) {
  import_pool_ = std::make_unique<utils::ThreadPool>(
      static_cast<size_t>(max_concurrent));
  std::filesystem::create_directories(temp_dir_);
}

// ============================================================================
// Public API
// ============================================================================

core::StatusOr<std::string> BulkImporter::StartImport(
    const std::string& collection_name,
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType metric,
    core::IndexType index_type,
    const std::string& source_uri,
    ImportFormat format,
    const std::string& vector_column,
    const std::string& id_column) {

  if (!object_store_) {
    return core::FailedPreconditionError(
        "Bulk import requires object storage (S3/MinIO). "
        "Build with -DGVDB_WITH_S3=ON and configure storage.object_store");
  }

  if (source_uri.empty()) {
    return core::InvalidArgumentError("source_uri is required");
  }

  // Check idempotency
  auto fingerprint = ComputeFingerprint(source_uri, collection_name);
  {
    std::lock_guard lock(jobs_mutex_);
    auto fp_it = fingerprint_to_job_.find(fingerprint);
    if (fp_it != fingerprint_to_job_.end()) {
      auto job_it = jobs_.find(fp_it->second);
      if (job_it != jobs_.end()) {
        auto state = job_it->second->state.load();
        if (state == ImportState::COMPLETED || state == ImportState::RUNNING ||
            state == ImportState::PENDING) {
          // Return existing job
          return fp_it->second;
        }
        // FAILED or CANCELLED — allow re-import: remove old entries
        jobs_.erase(job_it);
        fingerprint_to_job_.erase(fp_it);
      }
    }
  }

  // Reserve a concurrency slot atomically (fixes TOCTOU race)
  int prev = active_imports_.fetch_add(1);
  if (prev >= max_concurrent_) {
    active_imports_.fetch_sub(1);
    return core::ResourceExhaustedError(
        absl::StrCat("Maximum concurrent imports reached (",
                      max_concurrent_, "). Try again later."));
  }

  // Validate URI format
  auto s3_key = ParseS3Uri(source_uri);
  if (!s3_key.ok()) {
    return s3_key.status();
  }

  // Create job
  auto job = std::make_shared<ImportJob>();
  job->id = GenerateImportId();
  job->collection_name = collection_name;
  job->source_uri = source_uri;
  job->fingerprint = fingerprint;
  job->start_time = std::chrono::steady_clock::now();

  {
    std::lock_guard lock(jobs_mutex_);
    jobs_[job->id] = job;
    fingerprint_to_job_[fingerprint] = job->id;
  }

  utils::Logger::Instance().Info(
      "Bulk import job {} created: collection='{}', uri='{}', format={}",
      job->id, collection_name, source_uri, static_cast<int>(format));

  // Submit to thread pool
  import_pool_->enqueue([this, job, collection_id, dimension, metric,
                         index_type, format, vector_column, id_column]() {
    ExecuteImport(job, collection_id, dimension, metric, index_type,
                  format, vector_column, id_column);
  });

  return job->id;
}

core::StatusOr<ImportJobStatus> BulkImporter::GetStatus(
    const std::string& import_id) const {
  std::lock_guard lock(jobs_mutex_);
  auto it = jobs_.find(import_id);
  if (it == jobs_.end()) {
    return core::NotFoundError(
        absl::StrCat("Import job not found: ", import_id));
  }

  auto& job = it->second;
  auto elapsed = std::chrono::steady_clock::now() - job->start_time;
  float elapsed_sec = std::chrono::duration<float>(elapsed).count();

  uint64_t total = job->total_vectors.load();
  uint64_t imported = job->imported_vectors.load();
  float progress = (total > 0) ? (static_cast<float>(imported) / static_cast<float>(total) * 100.0f) : 0.0f;

  return ImportJobStatus{
      .import_id = job->id,
      .collection_name = job->collection_name,
      .state = job->state.load(),
      .total_vectors = total,
      .imported_vectors = imported,
      .progress_percent = progress,
      .error_message = job->GetError(),
      .elapsed_seconds = elapsed_sec,
      .segments_created = job->segments_created.load(),
  };
}

core::Status BulkImporter::CancelImport(const std::string& import_id) {
  std::lock_guard lock(jobs_mutex_);
  auto it = jobs_.find(import_id);
  if (it == jobs_.end()) {
    return core::NotFoundError(
        absl::StrCat("Import job not found: ", import_id));
  }

  auto& job = it->second;
  auto state = job->state.load();
  if (state == ImportState::COMPLETED || state == ImportState::FAILED ||
      state == ImportState::CANCELLED) {
    return core::OkStatus();  // Already terminal — idempotent
  }

  job->cancelled.store(true);
  utils::Logger::Instance().Info("Bulk import job {} cancellation requested",
                                  import_id);
  return core::OkStatus();
}

// ============================================================================
// Internal execution
// ============================================================================

void BulkImporter::ExecuteImport(
    std::shared_ptr<ImportJob> job,
    core::CollectionId collection_id,
    core::Dimension dimension,
    core::MetricType /*metric*/,
    core::IndexType /*index_type*/,
    ImportFormat format,
    const std::string& /*vector_column*/,
    const std::string& /*id_column*/) {

  // active_imports_ already incremented in StartImport
  job->state.store(ImportState::RUNNING);

  std::string temp_path;
  core::Status status;

  // Download file from S3
  auto s3_key = ParseS3Uri(job->source_uri);
  if (!s3_key.ok()) {
    job->SetError(std::string(s3_key.status().message()));
    job->state.store(ImportState::FAILED);
    active_imports_.fetch_sub(1);
    return;
  }

  auto download_result = DownloadToTemp(*s3_key, job->id);
  if (!download_result.ok()) {
    job->SetError(std::string(download_result.status().message()));
    job->state.store(ImportState::FAILED);
    active_imports_.fetch_sub(1);
    return;
  }
  temp_path = *download_result;

  // Check cancellation after download
  if (job->cancelled.load()) {
    CleanupOnFailure(job, temp_path);
    job->state.store(ImportState::CANCELLED);
    active_imports_.fetch_sub(1);
    return;
  }

  // Execute format-specific import
  switch (format) {
    case ImportFormat::NUMPY:
      status = ImportNpy(job, temp_path, collection_id, dimension);
      break;
    case ImportFormat::PARQUET:
      status = core::UnimplementedError(
          "Parquet import is not yet available. Use NUMPY format.");
      break;
  }

  // Clean up temp file
  std::error_code ec;
  std::filesystem::remove(temp_path, ec);

  if (job->cancelled.load()) {
    CleanupOnFailure(job, "");  // temp already removed
    job->state.store(ImportState::CANCELLED);
  } else if (!status.ok()) {
    job->SetError(std::string(status.message()));
    CleanupOnFailure(job, "");
    job->state.store(ImportState::FAILED);
  } else {
    job->state.store(ImportState::COMPLETED);
    utils::Logger::Instance().Info(
        "Bulk import job {} completed: {} vectors in {} segments",
        job->id, job->imported_vectors.load(), job->segments_created.load());
  }

  active_imports_.fetch_sub(1);
}

core::Status BulkImporter::ImportNpy(
    std::shared_ptr<ImportJob> job,
    const std::string& local_path,
    core::CollectionId collection_id,
    core::Dimension expected_dim) {

  // Parse header
  auto header_result = NpyReader::ReadHeader(local_path);
  if (!header_result.ok()) {
    return header_result.status();
  }
  auto header = *header_result;

  // Validate dimension
  if (header.dimension != static_cast<size_t>(expected_dim)) {
    return core::InvalidArgumentError(
        absl::StrCat("Dimension mismatch: .npy file has dimension ",
                      header.dimension, " but collection expects ",
                      expected_dim));
  }

  job->total_vectors.store(header.num_vectors);

  // Import in batches
  size_t start_row = 0;
  uint64_t next_id = 1;

  while (start_row < header.num_vectors) {
    // Check cancellation between batches
    if (job->cancelled.load()) {
      return core::CancelledError("Import cancelled by user");
    }

    size_t batch_count = std::min(kImportBatchSize,
                                   header.num_vectors - start_row);

    // Read chunk from file
    auto chunk_result = NpyReader::ReadChunk(
        local_path, header, start_row, batch_count, next_id);
    if (!chunk_result.ok()) {
      return chunk_result.status();
    }
    auto& chunk = *chunk_result;

    if (chunk.empty()) break;

    // Separate IDs and vectors for AddVectors
    std::vector<core::VectorId> ids;
    std::vector<core::Vector> vectors;
    ids.reserve(chunk.size());
    vectors.reserve(chunk.size());
    for (auto& [id, vec] : chunk) {
      ids.push_back(id);
      vectors.push_back(std::move(vec));
    }

    // Get writable segment (auto-rotates when full)
    auto* segment = segment_store_->GetWritableSegment(collection_id);
    if (!segment) {
      return core::InternalError(
          "Failed to get writable segment for collection");
    }

    // Track new segments
    auto seg_id = segment->GetId();
    job->AddSegment(seg_id);

    // Add vectors to segment
    auto add_status = segment->AddVectors(vectors, ids);
    if (!add_status.ok()) {
      // Segment might be full — get a new one and retry
      segment = segment_store_->GetWritableSegment(
          collection_id, vectors.size() * expected_dim * sizeof(float));
      if (!segment) {
        return core::InternalError("Failed to get writable segment after rotation");
      }
      seg_id = segment->GetId();
      job->AddSegment(seg_id);
      add_status = segment->AddVectors(vectors, ids);
      if (!add_status.ok()) {
        return add_status;
      }
    }

    job->imported_vectors.fetch_add(chunk.size());
    start_row += chunk.size();
    next_id += chunk.size();
  }

  return core::OkStatus();
}

core::Status BulkImporter::ImportParquet(
    std::shared_ptr<ImportJob> /*job*/,
    const std::string& /*local_path*/,
    core::CollectionId /*collection_id*/,
    core::Dimension /*expected_dim*/) {
  return core::UnimplementedError(
      "Parquet import is not yet available. Use NUMPY format.");
}

// ============================================================================
// Helpers
// ============================================================================

core::StatusOr<std::string> BulkImporter::ParseS3Uri(const std::string& uri) {
  // Expected format: s3://bucket/key or s3://bucket/path/to/file
  if (uri.size() < 6 || uri.substr(0, 5) != "s3://") {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid S3 URI (must start with s3://): ", uri));
  }

  // Return the part after s3:// — the IObjectStore key
  // The bucket is configured at the object store level, so we strip just
  // the bucket name and return the key path.
  auto path = uri.substr(5);  // Remove "s3://"
  auto slash_pos = path.find('/');
  if (slash_pos == std::string::npos || slash_pos == path.size() - 1) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid S3 URI (no key path after bucket): ", uri));
  }

  // Return key (everything after bucket/)
  return path.substr(slash_pos + 1);
}

core::StatusOr<std::string> BulkImporter::DownloadToTemp(
    const std::string& s3_key, const std::string& job_id) {
  // Use job_id for unique temp file path (job IDs are already unique)
  auto temp_path = temp_dir_ + "/import_" + job_id;

  auto status = object_store_->GetObjectToFile(s3_key, temp_path);
  if (!status.ok()) {
    return core::InternalError(
        absl::StrCat("Failed to download from S3 key '", s3_key,
                      "': ", status.message()));
  }

  return temp_path;
}

std::string BulkImporter::GenerateImportId() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

  // thread_local avoids data race on concurrent StartImport calls
  static thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<uint32_t> dist(0, 0xFFFF);

  return absl::StrCat("imp_", ms, "_", dist(rng));
}

std::string BulkImporter::ComputeFingerprint(
    const std::string& source_uri, const std::string& collection_name) {
  // Simple FNV-1a hash of concatenated fields
  uint64_t hash = 14695981039346656037ULL;
  auto hash_bytes = [&hash](const std::string& s) {
    for (char c : s) {
      hash ^= static_cast<uint64_t>(static_cast<uint8_t>(c));
      hash *= 1099511628211ULL;
    }
  };
  hash_bytes(source_uri);
  hash_bytes("|");
  hash_bytes(collection_name);
  return std::to_string(hash);
}

void BulkImporter::CleanupOnFailure(std::shared_ptr<ImportJob> job,
                                     const std::string& temp_path) {
  // Remove temp file
  if (!temp_path.empty()) {
    std::error_code ec;
    std::filesystem::remove(temp_path, ec);
  }

  // Drop partially-created segments
  std::vector<core::SegmentId> segments;
  {
    std::lock_guard lock(job->segments_mutex);
    segments = job->created_segment_ids;
  }

  for (auto seg_id : segments) {
    auto status = segment_store_->DropSegment(seg_id, true);
    if (!status.ok()) {
      utils::Logger::Instance().Warn(
          "Failed to cleanup segment {} during import failure: {}",
          core::ToUInt32(seg_id), status.message());
    }
  }

  if (!segments.empty()) {
    utils::Logger::Instance().Info(
        "Cleaned up {} segments for failed import job {}",
        segments.size(), job->id);
  }
}

}  // namespace storage
}  // namespace gvdb
