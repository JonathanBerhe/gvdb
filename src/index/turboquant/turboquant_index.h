// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_TURBOQUANT_INDEX_H_
#define GVDB_INDEX_TURBOQUANT_INDEX_H_

#include <shared_mutex>
#include <vector>

#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "turboquant_codec.h"

namespace gvdb {
namespace index {
namespace turboquant {

// TurboQuant vector index: data-oblivious quantized search.
//
// Unlike Faiss-backed indexes, this is a custom implementation of the
// TurboQuant algorithm (arXiv:2504.19874). Key properties:
//   - No training required (data-oblivious)
//   - Vectors can be added one at a time with instant quantization
//   - Near-optimal inner-product preservation at 1-8 bits per dimension
class TurboQuantIndex : public core::IVectorIndex {
 public:
  TurboQuantIndex(core::Dimension dimension, core::MetricType metric,
                  int bit_width = 4, bool use_qjl = true, int qjl_dim = 0);
  ~TurboQuantIndex() override = default;

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
  // Encode and append a single vector to storage
  core::Status EncodeAndStore(const core::Vector& vector, core::VectorId id);

  // Convert estimated inner product to metric-appropriate distance
  float IPToDistance(float estimated_ip, float x_norm,
                     const PreprocessedQuery& query) const;

  TurboQuantCodec codec_;
  core::MetricType metric_type_;

  // Contiguous storage for encoded vectors
  std::vector<float> norms_;
  std::vector<float> residual_norms_;
  std::vector<uint8_t> codes_;       // size = num_vectors * codec_.CodeBytesPerVector()
  std::vector<uint8_t> qjl_bits_;    // size = num_vectors * codec_.QJLBytesPerVector()
  std::vector<core::VectorId> ids_;
  std::vector<bool> deleted_;

  size_t num_vectors_ = 0;

  mutable std::shared_mutex mutex_;
};

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_TURBOQUANT_INDEX_H_
