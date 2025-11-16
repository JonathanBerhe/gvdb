#ifndef GVDB_CORE_INTERFACES_H_
#define GVDB_CORE_INTERFACES_H_

#include <memory>
#include <string>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace core {

// Forward declarations
struct IndexConfig;
struct StorageConfig;

// ============================================================================
// IVectorIndex - Abstract interface for all vector index implementations
// ============================================================================
class IVectorIndex {
 public:
  virtual ~IVectorIndex() = default;

  // Building and modification operations

  // Build index from a batch of vectors
  [[nodiscard]] virtual Status Build(const std::vector<Vector>& vectors,
                                       const std::vector<VectorId>& ids) = 0;

  // Add a single vector to the index
  [[nodiscard]] virtual Status Add(const Vector& vector, VectorId id) = 0;

  // Add multiple vectors to the index (batch operation)
  [[nodiscard]] virtual Status AddBatch(const std::vector<Vector>& vectors,
                                         const std::vector<VectorId>& ids) = 0;

  // Remove a vector from the index
  [[nodiscard]] virtual Status Remove(VectorId id) = 0;

  // Search operations

  // Search for k nearest neighbors
  [[nodiscard]] virtual StatusOr<SearchResult> Search(
      const Vector& query, int k) = 0;

  // Range search: find all vectors within a radius
  [[nodiscard]] virtual StatusOr<SearchResult> SearchRange(
      const Vector& query, float radius) = 0;

  // Batch search: multiple queries at once
  [[nodiscard]] virtual StatusOr<std::vector<SearchResult>> SearchBatch(
      const std::vector<Vector>& queries, int k) = 0;

  // Index management

  // Get memory usage in bytes
  [[nodiscard]] virtual size_t GetMemoryUsage() const = 0;

  // Get number of vectors in the index
  [[nodiscard]] virtual size_t GetVectorCount() const = 0;

  // Get index dimension
  [[nodiscard]] virtual Dimension GetDimension() const = 0;

  // Get metric type
  [[nodiscard]] virtual MetricType GetMetricType() const = 0;

  // Train the index (for indexes that require training like IVF)
  [[nodiscard]] virtual Status Train(const std::vector<Vector>& training_data) = 0;

  // Check if index is trained
  [[nodiscard]] virtual bool IsTrained() const = 0;

  // Serialization

  // Serialize index to file
  [[nodiscard]] virtual Status Serialize(const std::string& path) const = 0;

  // Deserialize index from file
  [[nodiscard]] virtual Status Deserialize(const std::string& path) = 0;

  // Get index type
  [[nodiscard]] virtual IndexType GetIndexType() const = 0;
};

// ============================================================================
// IStorage - Abstract interface for storage backends
// ============================================================================
class IStorage {
 public:
  virtual ~IStorage() = default;

  // Segment operations

  // Create a new segment
  [[nodiscard]] virtual StatusOr<SegmentId> CreateSegment(
      CollectionId collection_id) = 0;

  // Write vectors to a segment
  [[nodiscard]] virtual Status WriteVectors(
      SegmentId segment_id,
      const std::vector<Vector>& vectors,
      const std::vector<VectorId>& ids) = 0;

  // Read vectors from a segment
  [[nodiscard]] virtual StatusOr<std::vector<Vector>> ReadVectors(
      SegmentId segment_id,
      const std::vector<VectorId>& ids) = 0;

  // Seal a segment (make it read-only)
  [[nodiscard]] virtual Status SealSegment(SegmentId segment_id) = 0;

  // Flush segment to persistent storage
  [[nodiscard]] virtual Status FlushSegment(SegmentId segment_id) = 0;

  // Drop a segment
  [[nodiscard]] virtual Status DropSegment(SegmentId segment_id) = 0;

  // Get segment state
  [[nodiscard]] virtual StatusOr<SegmentState> GetSegmentState(
      SegmentId segment_id) const = 0;

  // Metadata operations

  // Store metadata
  [[nodiscard]] virtual Status PutMetadata(
      const std::string& key, const std::string& value) = 0;

  // Retrieve metadata
  [[nodiscard]] virtual StatusOr<std::string> GetMetadata(
      const std::string& key) const = 0;

  // Delete metadata
  [[nodiscard]] virtual Status DeleteMetadata(const std::string& key) = 0;

  // Storage management

  // Get storage size in bytes
  [[nodiscard]] virtual size_t GetStorageSize() const = 0;

  // Compact storage (remove deleted data)
  [[nodiscard]] virtual Status Compact() = 0;

  // Close storage
  [[nodiscard]] virtual Status Close() = 0;
};

// ============================================================================
// IIndexFactory - Factory interface for creating index instances
// ============================================================================
class IIndexFactory {
 public:
  virtual ~IIndexFactory() = default;

  // Create an index instance based on configuration
  [[nodiscard]] virtual StatusOr<std::unique_ptr<IVectorIndex>> CreateIndex(
      const IndexConfig& config) = 0;
};

// ============================================================================
// IStorageFactory - Factory interface for creating storage instances
// ============================================================================
class IStorageFactory {
 public:
  virtual ~IStorageFactory() = default;

  // Create a storage instance based on configuration
  [[nodiscard]] virtual StatusOr<std::unique_ptr<IStorage>> CreateStorage(
      const StorageConfig& config) = 0;
};

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_INTERFACES_H_
