// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb::core {

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
  [[nodiscard]] virtual absl::Status Build(const std::vector<Vector>& vectors,
                                           const std::vector<VectorId>& ids) = 0;

  // Add a single vector to the index
  [[nodiscard]] virtual absl::Status Add(const Vector& vector, VectorId id) = 0;

  // Add multiple vectors to the index (batch operation)
  [[nodiscard]] virtual absl::Status AddBatch(const std::vector<Vector>& vectors,
                                              const std::vector<VectorId>& ids) = 0;

  // Remove a vector from the index
  [[nodiscard]] virtual absl::Status Remove(VectorId id) = 0;

  // Search operations

  // Search for k nearest neighbors
  [[nodiscard]] virtual absl::StatusOr<SearchResult> Search(const Vector& query, int k) = 0;

  // Range search: find all vectors within a radius
  [[nodiscard]] virtual absl::StatusOr<SearchResult> SearchRange(const Vector& query,
                                                                 float radius) = 0;

  // Batch search: multiple queries at once
  [[nodiscard]] virtual absl::StatusOr<std::vector<SearchResult>> SearchBatch(
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
  [[nodiscard]] virtual absl::Status Train(const std::vector<Vector>& training_data) = 0;

  // Check if index is trained
  [[nodiscard]] virtual bool IsTrained() const = 0;

  // Serialization

  // Serialize index to file
  [[nodiscard]] virtual absl::Status Serialize(const std::string& path) const = 0;

  // Deserialize index from file
  [[nodiscard]] virtual absl::Status Deserialize(const std::string& path) = 0;

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
  [[nodiscard]] virtual absl::StatusOr<std::unique_ptr<IVectorIndex>> CreateIndex(
      const IndexConfig& config) = 0;
};

}  // namespace gvdb::core
