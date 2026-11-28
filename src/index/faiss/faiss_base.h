// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <shared_mutex>
#include <vector>

#include <faiss/Index.h>

#ifdef GVDB_HAS_CUDA
namespace faiss {
namespace gpu {
class StandardGpuResources;
}  // namespace gpu
}  // namespace faiss
#endif

#include "absl/strings/str_cat.h"
#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"

namespace gvdb::index {

// Helper function to execute Faiss operations with exception handling
template <typename F>
absl::Status ExecuteFaissOperation(F&& operation, const std::string& context) {
  try {
    operation();
    return absl::OkStatus();
  } catch (const std::exception& e) {
    return absl::InternalError(absl::StrCat("Faiss error in ", context, ": ", e.what()));
  }
}

// Helper function to execute Faiss operations that return StatusOr
template <typename T, typename F>
absl::StatusOr<T> ExecuteFaissOperationWithReturn(F&& operation, const std::string& context) {
  try {
    return operation();
  } catch (const std::exception& e) {
    return absl::InternalError(absl::StrCat("Faiss error in ", context, ": ", e.what()));
  }
}

// Base class for all Faiss index wrappers
// Provides thread-safe wrapper around Faiss indexes. The wrapper returns the
// canonical distance of core/distance.h: FAISS computes squared L2 for L2,
// and the dot product for INNER_PRODUCT and COSINE (on unit vectors).
class FaissIndexBase : public core::IVectorIndex {
 public:
  // The FAISS metric of a GVDB metric. COSINE is the inner product of unit
  // vectors, so the wrapper normalizes every vector it passes to FAISS.
  [[nodiscard]] static faiss::MetricType ToFaissMetric(core::MetricType metric);

  explicit FaissIndexBase(std::unique_ptr<faiss::Index> index, core::MetricType metric);
  // Out-of-line so unique_ptr<StandardGpuResources>'s deleter sees the
  // complete type only in faiss_base.cpp (which includes the faiss-gpu
  // header under GVDB_HAS_CUDA).
  ~FaissIndexBase() override;

  // IVectorIndex implementation

  [[nodiscard]] absl::Status Build(const std::vector<core::Vector>& vectors,
                                   const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] absl::Status Add(const core::Vector& vector, core::VectorId id) override;

  [[nodiscard]] absl::Status AddBatch(const std::vector<core::Vector>& vectors,
                                      const std::vector<core::VectorId>& ids) override;

  [[nodiscard]] absl::Status Remove(core::VectorId id) override;

  [[nodiscard]] absl::StatusOr<core::SearchResult> Search(const core::Vector& query,
                                                          int k) override;

  [[nodiscard]] absl::StatusOr<core::SearchResult> SearchRange(const core::Vector& query,
                                                               float radius) override;

  [[nodiscard]] absl::StatusOr<std::vector<core::SearchResult>> SearchBatch(
      const std::vector<core::Vector>& queries, int k) override;

  [[nodiscard]] size_t GetMemoryUsage() const override;
  [[nodiscard]] size_t GetVectorCount() const override;
  [[nodiscard]] core::Dimension GetDimension() const override;
  [[nodiscard]] core::MetricType GetMetricType() const override;

  [[nodiscard]] absl::Status Train(const std::vector<core::Vector>& training_data) override;

  [[nodiscard]] bool IsTrained() const override;

  [[nodiscard]] absl::Status Serialize(const std::string& path) const override;
  [[nodiscard]] absl::Status Deserialize(const std::string& path) override;

  [[nodiscard]] core::IndexType GetIndexType() const override = 0;

 protected:
  // Convert Vector to float array for Faiss, normalized for COSINE
  [[nodiscard]] std::vector<float> VectorToFloatArray(const core::Vector& vec) const;

  // Convert multiple vectors to contiguous float array, normalized for COSINE
  [[nodiscard]] std::vector<float> VectorsToFloatArray(
      const std::vector<core::Vector>& vectors) const;

  // The canonical distance of a FAISS distance or dot product.
  [[nodiscard]] float ToCanonical(float faiss_value) const;

  // The FAISS range_search radius of a canonical radius: a squared distance
  // for L2, the smallest dot product for INNER_PRODUCT and COSINE.
  [[nodiscard]] virtual float ToFaissRadius(float radius) const;

  // Convert VectorId to faiss idx_t
  [[nodiscard]] static faiss::idx_t VectorIdToIdx(core::VectorId id);

  // Convert faiss idx_t to VectorId
  [[nodiscard]] static core::VectorId IdxToVectorId(faiss::idx_t idx);

  // Derived classes opt into the GPU clone path. Default: CPU only.
  // FLAT stays CPU even when CUDA is available because faiss::gpu::GpuIndexFlat
  // does not honor the add_with_ids contract this base class relies on; IVF
  // variants override and return true.
  [[nodiscard]] virtual bool SupportsGpu() const { return false; }

  // Clone index_ from CPU to GPU when SupportsGpu() and a CUDA device is
  // available. No-op without GVDB_HAS_CUDA. Safe to call multiple times.
  void MaybeCloneToGpu();

#ifdef GVDB_HAS_CUDA
  // Owns the CUDA scratch resources backing index_ when gpu_resident_.
  // Declared before index_ so it destructs last, because the GPU index references
  // these resources during its own destruction.
  std::unique_ptr<faiss::gpu::StandardGpuResources> gpu_res_;
  bool gpu_resident_ = false;
#endif

  // Underlying Faiss index. Holds either a CPU or GPU-resident faiss::Index
  // depending on whether MaybeCloneToGpu() ran.
  std::unique_ptr<faiss::Index> index_;

  // Metric type
  core::MetricType metric_type_;

  // Thread safety: multiple readers, single writer
  mutable std::shared_mutex mutex_;

  // Track if index is trained (for IVF indexes)
  bool is_trained_{false};
};

}  // namespace gvdb::index
