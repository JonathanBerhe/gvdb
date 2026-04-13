// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#ifdef GVDB_HAS_METAL

#include <shared_mutex>
#include <string>
#include <vector>

namespace MTL { class Buffer; }

#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace index {
namespace metal {

// FLAT index with Metal GPU-accelerated distance computation.
// Stores vectors in CPU memory, dispatches brute-force distance
// computation to the GPU via Metal compute kernels.
// API-compatible drop-in for FaissFlatIndex.
class MetalFlatIndex : public core::IVectorIndex {
 public:
  MetalFlatIndex(core::Dimension dimension, core::MetricType metric);
  ~MetalFlatIndex() override = default;

  // IVectorIndex implementation
  [[nodiscard]] core::Status Build(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::Status Add(const core::Vector& vector,
                                  core::VectorId id) override;

  [[nodiscard]] core::Status AddBatch(
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] core::Status Remove(core::VectorId id) override;

  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const core::Vector& query, int k) override;

  [[nodiscard]] core::StatusOr<core::SearchResult> SearchRange(
      const core::Vector& query, float radius) override;

  [[nodiscard]] core::StatusOr<std::vector<core::SearchResult>> SearchBatch(
      const std::vector<core::Vector>& queries, int k) override;

  [[nodiscard]] size_t GetMemoryUsage() const override;
  [[nodiscard]] size_t GetVectorCount() const override;
  [[nodiscard]] core::Dimension GetDimension() const override;
  [[nodiscard]] core::MetricType GetMetricType() const override;

  [[nodiscard]] core::Status Train(
      const std::vector<core::Vector>& training_data) override;

  [[nodiscard]] bool IsTrained() const override;

  [[nodiscard]] core::Status Serialize(const std::string& path) const override;
  [[nodiscard]] core::Status Deserialize(const std::string& path) override;

  [[nodiscard]] core::IndexType GetIndexType() const override;

 private:
  // Get the Metal kernel name for the configured metric type
  std::string KernelName() const;

  // Extract top-k results from a distance array
  core::SearchResult TopK(const float* distances, size_t nb, int k) const;

  // Rebuild the persistent GPU buffer from vectors_
  void RebuildGpuBuffer();

  core::Dimension dimension_;
  core::MetricType metric_type_;

  // Contiguous vector storage: [n * dimension_] floats
  std::vector<float> vectors_;
  std::vector<core::VectorId> ids_;

  // Persistent Metal buffer — rebuilt on Build/Add/AddBatch, reused on Search
  MTL::Buffer* gpu_vectors_ = nullptr;

  mutable std::shared_mutex mutex_;
};

}  // namespace metal
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_HAS_METAL
