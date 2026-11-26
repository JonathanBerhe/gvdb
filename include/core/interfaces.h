// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

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
// IIndexFactory - Factory interface for creating index instances
// ============================================================================
class IIndexFactory {
 public:
  virtual ~IIndexFactory() = default;

  // Create an index instance based on configuration
  [[nodiscard]] virtual StatusOr<std::unique_ptr<IVectorIndex>> CreateIndex(
      const IndexConfig& config) = 0;
};

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_INTERFACES_H_