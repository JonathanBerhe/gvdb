// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_BULK_IMPORTER_H_
#define GVDB_STORAGE_BULK_IMPORTER_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "storage/object_store.h"
#include "storage/segment_store.h"
#include "utils/thread_pool.h"

namespace gvdb {
namespace storage {

// ============================================================================
// Import job state (mirrors proto ImportState)
// ============================================================================
enum class ImportState {
  PENDING = 0,
  RUNNING = 1,
  COMPLETED = 2,
  FAILED = 3,
  CANCELLED = 4,
};

// ============================================================================
// Import format (mirrors proto ImportFormat)
// ============================================================================
enum class ImportFormat {
  PARQUET = 0,
  NUMPY = 1,
};

// ============================================================================
// ImportJob - Tracks the lifecycle of a single bulk import operation
// ============================================================================
struct ImportJob {
  std::string id;
  std::string collection_name;
  std::string source_uri;
  std::string fingerprint;

  std::atomic<ImportState> state{ImportState::PENDING};
  std::atomic<uint64_t> total_vectors{0};
  std::atomic<uint64_t> imported_vectors{0};
  std::atomic<uint32_t> segments_created{0};

  std::mutex error_mutex;
  std::string error_message;

  std::atomic<bool> cancelled{false};

  std::chrono::steady_clock::time_point start_time;

  // Track created segments for cleanup on failure
  std::mutex segments_mutex;
  std::vector<core::SegmentId> created_segment_ids;

  void SetError(const std::string& msg) {
    std::lock_guard lock(error_mutex);
    error_message = msg;
  }

  std::string GetError() const {
    std::lock_guard lock(const_cast<std::mutex&>(error_mutex));
    return error_message;
  }

  void AddSegment(core::SegmentId id) {
    std::lock_guard lock(segments_mutex);
    if (std::find(created_segment_ids.begin(), created_segment_ids.end(), id) ==
        created_segment_ids.end()) {
      created_segment_ids.push_back(id);
      segments_created.fetch_add(1);
    }
  }
};

// ============================================================================
// ImportJobStatus - Snapshot of job state for RPC responses
// ============================================================================
struct ImportJobStatus {
  std::string import_id;
  ImportState state;
  uint64_t total_vectors;
  uint64_t imported_vectors;
  float progress_percent;
  std::string error_message;
  float elapsed_seconds;
  uint32_t segments_created;
};

// ============================================================================
// BulkImporter - Async engine for server-side bulk import from object storage
// ============================================================================
// Downloads files from S3/MinIO, parses them (NumPy .npy or Parquet), and
// creates segments via ISegmentStore. Jobs run asynchronously on a thread pool
// with cancellation support and failure cleanup.
//
// Thread-safety: All public methods are thread-safe.
class BulkImporter {
 public:
  // object_store may be nullptr (import will fail with UNIMPLEMENTED).
  // max_concurrent controls how many imports can run simultaneously.
  BulkImporter(std::shared_ptr<ISegmentStore> segment_store,
               IObjectStore* object_store,
               const std::string& temp_dir,
               int max_concurrent = 2);

  ~BulkImporter() = default;

  // Start an async import job. Returns job_id immediately.
  // The collection must already exist — caller resolves metadata.
  [[nodiscard]] core::StatusOr<std::string> StartImport(
      const std::string& collection_name,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type,
      const std::string& source_uri,
      ImportFormat format,
      const std::string& vector_column = "vector",
      const std::string& id_column = "id");

  // Get current job status snapshot.
  [[nodiscard]] core::StatusOr<ImportJobStatus> GetStatus(
      const std::string& import_id) const;

  // Cancel a running or pending import. Idempotent.
  [[nodiscard]] core::Status CancelImport(const std::string& import_id);

  // Number of batch vectors to add per AddVectorsWithMetadata call
  static constexpr size_t kImportBatchSize = 10000;

 private:
  // Execute the import (runs on thread pool)
  void ExecuteImport(std::shared_ptr<ImportJob> job,
                     core::CollectionId collection_id,
                     core::Dimension dimension,
                     core::MetricType metric,
                     core::IndexType index_type,
                     ImportFormat format,
                     const std::string& vector_column,
                     const std::string& id_column);

  // Import a NumPy .npy file
  core::Status ImportNpy(std::shared_ptr<ImportJob> job,
                         const std::string& local_path,
                         core::CollectionId collection_id,
                         core::Dimension expected_dim);

  // Import a Parquet file (requires GVDB_HAS_ARROW)
  core::Status ImportParquet(std::shared_ptr<ImportJob> job,
                              const std::string& local_path,
                              core::CollectionId collection_id,
                              core::Dimension expected_dim);

  // Parse s3://bucket/key URI into object store key
  static core::StatusOr<std::string> ParseS3Uri(const std::string& uri);

  // Download file from object store to local temp path
  core::StatusOr<std::string> DownloadToTemp(const std::string& s3_key);

  // Generate a unique import ID
  static std::string GenerateImportId();

  // Compute fingerprint for idempotency
  static std::string ComputeFingerprint(const std::string& source_uri,
                                         const std::string& collection_name);

  // Cleanup partially-created segments and temp file on failure
  void CleanupOnFailure(std::shared_ptr<ImportJob> job,
                        const std::string& temp_path);

  std::shared_ptr<ISegmentStore> segment_store_;
  IObjectStore* object_store_;
  std::string temp_dir_;

  // Job tracking
  mutable std::mutex jobs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<ImportJob>> jobs_;
  std::unordered_map<std::string, std::string> fingerprint_to_job_;

  // Concurrency control
  std::unique_ptr<utils::ThreadPool> import_pool_;
  int max_concurrent_;
  std::atomic<int> active_imports_{0};
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_BULK_IMPORTER_H_
