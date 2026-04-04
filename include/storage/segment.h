// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

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
#include "index/text_index.h"
#include "storage/scalar_index.h"

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

  // Read specific vectors by ID (fails if any ID not found)
  [[nodiscard]] core::StatusOr<std::vector<core::Vector>> ReadVectors(
      const std::vector<core::VectorId>& ids) const;

  // Get vectors by ID with metadata (returns partial results)
  struct GetVectorsResult {
    std::vector<core::VectorId> found_ids;
    std::vector<core::Vector> found_vectors;
    std::vector<core::Metadata> found_metadata;
    std::vector<core::VectorId> not_found_ids;
  };
  [[nodiscard]] GetVectorsResult GetVectors(
      const std::vector<core::VectorId>& ids, bool include_metadata) const;

  // Delete vectors by ID (only valid for GROWING state)
  struct DeleteVectorsResult {
    uint64_t deleted_count;
    std::vector<core::VectorId> not_found_ids;
  };
  [[nodiscard]] core::StatusOr<DeleteVectorsResult> DeleteVectors(
      const std::vector<core::VectorId>& ids);

  // Update metadata for a vector (only valid for GROWING state)
  [[nodiscard]] core::Status UpdateMetadata(
      core::VectorId id, const core::Metadata& metadata, bool merge);

  // Search for nearest neighbors (requires index to be built)
  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const core::Vector& query, int k) const;

  // Search with metadata filtering (requires index to be built)
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchWithFilter(
      const core::Vector& query, int k, const std::string& filter_expr) const;

  // Hybrid search combining vector similarity and BM25 text relevance
  // Uses Reciprocal Rank Fusion to combine ranked results
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchHybrid(
      const core::Vector& query_vector,
      const std::string& text_query,
      int k,
      float vector_weight = 0.5f,
      float text_weight = 0.5f,
      const std::string& text_field = "text") const;

  // Upsert: insert or replace vector (only valid for GROWING state)
  struct UpsertResult {
    uint64_t inserted_count;
    uint64_t updated_count;
  };
  [[nodiscard]] core::StatusOr<UpsertResult> UpsertVectors(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids,
      const std::vector<core::Metadata>& metadata);

  // Range search: find all vectors within distance radius
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchRange(
      const core::Vector& query, float radius, int max_results = 1000) const;

  // Range search with metadata filter
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchRangeWithFilter(
      const core::Vector& query, float radius, const std::string& filter_expr,
      int max_results = 1000) const;

  // Get metadata for a vector
  [[nodiscard]] core::StatusOr<core::Metadata> GetMetadata(core::VectorId id) const;

  // ========== State Management ==========

  // Set the text index for hybrid search (call after seal with metadata text field)
  void SetTextIndex(std::unique_ptr<index::ITextIndex> text_index);

  // Build a BM25 text index from a metadata field across all vectors in this segment
  core::Status BuildTextIndex(std::unique_ptr<index::ITextIndex> text_index,
                               const std::string& text_field = "text");

  // Seal the segment (transition from GROWING to SEALED)
  // This builds the index and makes the segment immutable
  [[nodiscard]] core::Status Seal(core::IVectorIndex* index);

  // Flush segment data to persistent storage
  [[nodiscard]] core::Status Flush(const std::string& base_path);

  // Load segment from persistent storage
  [[nodiscard]] static core::StatusOr<std::unique_ptr<Segment>> Load(
      const std::string& base_path, core::SegmentId id);

  // ========== Serialization for Replication ==========

  // Serialize segment data to bytes (for network transfer)
  [[nodiscard]] core::StatusOr<std::string> SerializeToBytes() const;

  // Deserialize segment data from bytes and create segment
  [[nodiscard]] static core::StatusOr<std::unique_ptr<Segment>> DeserializeFromBytes(
      const std::string& bytes_data);

  // Get all vector IDs in this segment
  [[nodiscard]] std::vector<core::VectorId> GetAllVectorIds() const;

  // ========== Accessors ==========

  [[nodiscard]] core::SegmentId GetId() const { return id_; }
  [[nodiscard]] core::CollectionId GetCollectionId() const {
    return collection_id_;
  }
  [[nodiscard]] core::SegmentState GetState() const;
  [[nodiscard]] core::Dimension GetDimension() const { return dimension_; }
  [[nodiscard]] core::MetricType GetMetric() const { return metric_; }
  [[nodiscard]] core::IndexType GetIndexType() const { return index_type_; }
  [[nodiscard]] size_t GetVectorCount() const;
  [[nodiscard]] size_t GetMemoryUsage() const;

  // Check if segment can accept more vectors
  [[nodiscard]] bool CanAcceptWrites() const;

  // Get maximum segment size (512 MB default)
  static constexpr size_t kMaxSegmentSize = 512 * 1024 * 1024;  // 512 MB

  friend class SegmentManager;

 private:
  // Segment identification
  core::SegmentId id_;
  core::CollectionId collection_id_;
  core::Dimension dimension_;
  core::MetricType metric_;
  core::IndexType index_type_ = core::IndexType::FLAT;

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

  // Text index for BM25 hybrid search (built during seal from metadata text fields)
  std::unique_ptr<index::ITextIndex> text_index_;

  // Scalar indexes on metadata fields (built incrementally + during seal)
  ScalarIndexSet scalar_indexes_;

  // Helper methods
  [[nodiscard]] bool IsFull() const;
  [[nodiscard]] core::Status ValidateVectors(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) const;

  // Brute-force search over in-memory vectors. Caller must hold the lock.
  [[nodiscard]] core::StatusOr<core::SearchResult> BruteForceSearchUnlocked(
      const core::Vector& query, int k) const;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_H_