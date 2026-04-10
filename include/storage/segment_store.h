// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_STORE_H_
#define GVDB_STORAGE_SEGMENT_STORE_H_

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "storage/segment.h"

namespace gvdb {
namespace storage {

// ============================================================================
// ISegmentStore - Abstract interface for segment storage backends
// ============================================================================
// All segment operations go through this interface. Implementations:
//   - SegmentManager: local disk only (existing)
//   - TieredSegmentManager: local disk + S3/MinIO object storage (future)
//
// Thread-safety: Implementations must be thread-safe.
class ISegmentStore {
 public:
  virtual ~ISegmentStore() = default;

  // Disable copy and move
  ISegmentStore(const ISegmentStore&) = delete;
  ISegmentStore& operator=(const ISegmentStore&) = delete;
  ISegmentStore(ISegmentStore&&) = delete;
  ISegmentStore& operator=(ISegmentStore&&) = delete;

  // ========== Segment Lifecycle ==========

  // Create a new growing segment (auto-assigns segment_id)
  [[nodiscard]] virtual core::StatusOr<core::SegmentId> CreateSegment(
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric) = 0;

  // Create segment with specific ID (for distributed mode - called via RPC)
  [[nodiscard]] virtual core::Status CreateSegmentWithId(
      core::SegmentId segment_id,
      core::CollectionId collection_id,
      core::Dimension dimension,
      core::MetricType metric,
      core::IndexType index_type) = 0;

  // Get a segment by ID (returns nullptr if not found)
  [[nodiscard]] virtual Segment* GetSegment(core::SegmentId id) = 0;
  [[nodiscard]] virtual const Segment* GetSegment(core::SegmentId id) const = 0;

  // Seal a segment (build index, make immutable)
  [[nodiscard]] virtual core::Status SealSegment(
      core::SegmentId id,
      const core::IndexConfig& index_config) = 0;

  // Flush a segment to persistent storage
  [[nodiscard]] virtual core::Status FlushSegment(core::SegmentId id) = 0;

  // Drop a segment (remove from memory and optionally delete from disk)
  [[nodiscard]] virtual core::Status DropSegment(
      core::SegmentId id, bool delete_files = false) = 0;

  // Load segment from disk
  [[nodiscard]] virtual core::Status LoadSegment(core::SegmentId id) = 0;

  // Add replicated segment (from network transfer)
  [[nodiscard]] virtual core::Status AddReplicatedSegment(
      std::unique_ptr<Segment> segment) = 0;

  // ========== Data Operations ==========

  // Write vectors to a segment
  [[nodiscard]] virtual core::Status WriteVectors(
      core::SegmentId segment_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) = 0;

  // Read vectors from a segment
  [[nodiscard]] virtual core::StatusOr<std::vector<core::Vector>> ReadVectors(
      core::SegmentId segment_id,
      const std::vector<core::VectorId>& ids) const = 0;

  // Search in a specific segment
  [[nodiscard]] virtual core::StatusOr<core::SearchResult> SearchSegment(
      core::SegmentId segment_id,
      const core::Vector& query,
      int k) const = 0;

  // Search across all segments in a collection
  [[nodiscard]] virtual core::StatusOr<core::SearchResult> SearchCollection(
      core::CollectionId collection_id,
      const core::Vector& query,
      int k) const = 0;

  // ========== Active Segment Rotation ==========

  // Callback invoked when a segment is rotated out (full).
  // Parameters: (old_segment_id, index_type_to_build)
  using SealCallback = std::function<void(core::SegmentId, core::IndexType)>;

  // Register a callback for segment rotation events.
  virtual void SetSealCallback(SealCallback callback) = 0;

  // Get a writable GROWING segment for a collection.
  // If the active segment is full, rotates: creates a new segment,
  // invokes the seal callback for the old one, and returns the new one.
  [[nodiscard]] virtual Segment* GetWritableSegment(
      core::CollectionId collection_id,
      size_t required_bytes = 0) = 0;

  // Get all queryable segments for a collection (GROWING + SEALED).
  [[nodiscard]] virtual std::vector<Segment*> GetQueryableSegments(
      core::CollectionId collection_id) const = 0;

  // Get all segment IDs across all collections
  [[nodiscard]] virtual std::vector<core::SegmentId> GetAllSegmentIds() const = 0;

  // Run TTL sweep loop — blocks until shutdown. Sweeps expired vectors from
  // GROWING segments at periodic interval.
  virtual void RunTTLSweepLoop(const std::atomic<bool>& shutdown) = 0;

  // ========== Management ==========

  // Load all previously-flushed segments from persistent storage (startup)
  [[nodiscard]] virtual absl::Status LoadAllSegments() = 0;

  // Get all segment IDs for a collection
  [[nodiscard]] virtual std::vector<core::SegmentId> GetCollectionSegments(
      core::CollectionId collection_id) const = 0;

  // Get segment count
  [[nodiscard]] virtual size_t GetSegmentCount() const = 0;

  // Get total memory usage
  [[nodiscard]] virtual size_t GetTotalMemoryUsage() const = 0;

  // Clear all segments
  virtual void Clear() = 0;

 protected:
  ISegmentStore() = default;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_STORE_H_
