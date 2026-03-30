// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_FAISS_BASE_H_
#define GVDB_INDEX_FAISS_BASE_H_

#include <memory>
#include <shared_mutex>
#include <vector>

#include <faiss/Index.h>

#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace index {

// Helper function to execute Faiss operations with exception handling
template <typename F>
core::Status ExecuteFaissOperation(F&& operation, const std::string& context) {
  try {
    operation();
    return core::OkStatus();
  } catch (const std::exception& e) {
    return core::InternalError(
        absl::StrCat("Faiss error in ", context, ": ", e.what()));
  }
}

// Helper function to execute Faiss operations that return StatusOr
template <typename T, typename F>
core::StatusOr<T> ExecuteFaissOperationWithReturn(F&& operation,
                                                   const std::string& context) {
  try {
    return operation();
  } catch (const std::exception& e) {
    return core::InternalError(
        absl::StrCat("Faiss error in ", context, ": ", e.what()));
  }
}

// Base class for all Faiss index wrappers
// Provides thread-safe wrapper around Faiss indexes
class FaissIndexBase : public core::IVectorIndex {
 public:
  explicit FaissIndexBase(std::unique_ptr<faiss::Index> index,
                          core::MetricType metric);
  virtual ~FaissIndexBase() = default;

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

  [[nodiscard]] core::IndexType GetIndexType() const override = 0;

 protected:
  // Convert Vector to float array for Faiss
  [[nodiscard]] static std::vector<float> VectorToFloatArray(
      const core::Vector& vec);

  // Convert multiple vectors to contiguous float array
  [[nodiscard]] static std::vector<float> VectorsToFloatArray(
      const std::vector<core::Vector>& vectors);

  // Convert VectorId to faiss idx_t
  [[nodiscard]] static faiss::idx_t VectorIdToIdx(core::VectorId id);

  // Convert faiss idx_t to VectorId
  [[nodiscard]] static core::VectorId IdxToVectorId(faiss::idx_t idx);

  // Underlying Faiss index
  std::unique_ptr<faiss::Index> index_;

  // Metric type
  core::MetricType metric_type_;

  // Thread safety: multiple readers, single writer
  mutable std::shared_mutex mutex_;

  // Track if index is trained (for IVF indexes)
  bool is_trained_;
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_FAISS_BASE_H_