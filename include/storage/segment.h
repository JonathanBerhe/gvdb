#ifndef GVDB_STORAGE_SEGMENT_H_
#define GVDB_STORAGE_SEGMENT_H_

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/interfaces.h"
#include "core/metadata.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace storage {

// Forward declarations
class Segment;

// ============================================================================
// Segment - Manages vector data for a single segment
// ============================================================================
// A segment represents a unit of data storage that goes through lifecycle:
// GROWING -> SEALING -> SEALED -> (optionally) FLUSHED
//
// Thread-safety: All operations are thread-safe using shared_mutex
class Segment {
 public:
  // Constructor
  Segment(core::SegmentId id, core::CollectionId collection_id,
          core::Dimension dimension, core::MetricType metric);

  // Disable copy and move (contains mutex)
  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;
  Segment(Segment&&) = delete;
  Segment& operator=(Segment&&) = delete;

  ~Segment() = default;

  // ========== Data Operations ==========

  // Add vectors to segment (only valid for GROWING state)
  [[nodiscard]] core::Status AddVectors(const std::vector<core::Vector>& vectors,
                                         const std::vector<core::VectorId>& ids);

  // Add vectors with metadata to segment (only valid for GROWING state)
  [[nodiscard]] core::Status AddVectorsWithMetadata(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids,
      const std::vector<core::Metadata>& metadata);

  // Read specific vectors by ID
  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      const std::vector<core::VectorId>& ids) const;

  // Search for nearest neighbors (requires index to be built)
  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const core::Vector& query, int k) const;

  // Search with metadata filtering (requires index to be built)
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchWithFilter(
      const core::Vector& query, int k, const std::string& filter_expr) const;

  // Get metadata for a vector
  [[nodiscard]] core::StatusOr<core::Metadata> GetMetadata(core::VectorId id) const;

  // ========== State Management ==========

  // Seal the segment (transition from GROWING to SEALED)
  // This builds the index and makes the segment immutable
  [[nodiscard]] core::Status Seal(core::IVectorIndex* index);

  // Flush segment data to persistent storage
  [[nodiscard]] core::Status Flush(const std::string& base_path);

  // Load segment from persistent storage
  [[nodiscard]] static core::StatusOr<std::unique_ptr<Segment>> Load(
      const std::string& base_path, core::SegmentId id);

  // ========== Accessors ==========

  [[nodiscard]] core::SegmentId GetId() const { return id_; }
  [[nodiscard]] core::CollectionId GetCollectionId() const {
    return collection_id_;
  }
  [[nodiscard]] core::SegmentState GetState() const;
  [[nodiscard]] core::Dimension GetDimension() const { return dimension_; }
  [[nodiscard]] core::MetricType GetMetric() const { return metric_; }
  [[nodiscard]] size_t GetVectorCount() const;
  [[nodiscard]] size_t GetMemoryUsage() const;

  // Check if segment can accept more vectors
  [[nodiscard]] bool CanAcceptWrites() const;

  // Get maximum segment size (512 MB default)
  static constexpr size_t kMaxSegmentSize = 512 * 1024 * 1024;  // 512 MB

 private:
  // Segment identification
  core::SegmentId id_;
  core::CollectionId collection_id_;
  core::Dimension dimension_;
  core::MetricType metric_;

  // State management
  mutable std::shared_mutex mutex_;
  core::SegmentState state_;

  // Vector storage (for GROWING state)
  std::vector<core::Vector> vectors_;
  std::vector<core::VectorId> vector_ids_;
  size_t memory_usage_;

  // Metadata storage (maps VectorId to Metadata)
  std::unordered_map<uint64_t, core::Metadata> metadata_map_;

  // Index (for SEALED state)
  std::unique_ptr<core::IVectorIndex> index_;

  // Helper methods
  [[nodiscard]] bool IsFull() const;
  [[nodiscard]] core::Status ValidateVectors(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) const;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_H_
