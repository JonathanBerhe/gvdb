// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_IVF_TURBOQUANT_INDEX_H_
#define GVDB_INDEX_IVF_TURBOQUANT_INDEX_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"
#include "core/vector.h"
#include "turboquant_codec.h"

#include <faiss/Clustering.h>
#include <faiss/IndexFlat.h>

namespace gvdb {
namespace index {
namespace turboquant {

// IVF + TurboQuant: sub-linear search with extreme compression.
//
// Uses Faiss IndexFlat as coarse quantizer (k-means centroids) for IVF
// partitioning, and TurboQuantCodec for per-cluster vector compression.
//
// Build: Train k-means centroids, assign vectors to clusters, encode with TQ.
// Search: Find nprobe nearest clusters, scan TQ-encoded vectors in each.
//
// Memory per vector (768D, 4-bit): ~400 bytes vs 3072 bytes for float32 (7.5x).
// Search complexity: O(nprobe * cluster_size) vs O(N) for brute-force.
class IVFTurboQuantIndex : public core::IVectorIndex {
 public:
  IVFTurboQuantIndex(core::Dimension dimension, core::MetricType metric,
                     int nlist, int nprobe, int bit_width,
                     bool use_qjl = true, int qjl_dim = 0);
  ~IVFTurboQuantIndex() override = default;

  // IVectorIndex interface
  [[nodiscard]] core::Status Build(const std::vector<core::Vector>& vectors,
                                   const std::vector<core::VectorId>& ids) override;
  [[nodiscard]] core::Status Add(const core::Vector& vector, core::VectorId id) override;
  [[nodiscard]] core::Status AddBatch(const std::vector<core::Vector>& vectors,
                                      const std::vector<core::VectorId>& ids) override;
  [[nodiscard]] core::Status Remove(core::VectorId id) override;

  [[nodiscard]] core::StatusOr<core::SearchResult> Search(
      const core::Vector& query, int k) override;
  [[nodiscard]] core::StatusOr<core::SearchResult> SearchRange(
      const core::Vector& query, float radius) override;
  [[nodiscard]] core::StatusOr<std::vector<core::SearchResult>> SearchBatch(
      const std::vector<core::Vector>& queries, int k) override;

  [[nodiscard]] core::Status Train(const std::vector<core::Vector>& training_data) override;
  [[nodiscard]] bool IsTrained() const override;

  [[nodiscard]] core::Status Serialize(const std::string& path) const override;
  [[nodiscard]] core::Status Deserialize(const std::string& path) override;

  [[nodiscard]] size_t GetMemoryUsage() const override;
  [[nodiscard]] size_t GetVectorCount() const override;
  [[nodiscard]] core::Dimension GetDimension() const override { return dimension_; }
  [[nodiscard]] core::MetricType GetMetricType() const override { return metric_type_; }
  [[nodiscard]] core::IndexType GetIndexType() const override { return core::IndexType::IVF_TURBOQUANT; }

 private:
  // Per-cluster storage of TurboQuant-encoded vectors
  struct ClusterData {
    std::vector<uint8_t> codes;
    std::vector<float> norms;
    std::vector<float> residual_norms;
    std::vector<uint8_t> qjl_bits;
    std::vector<core::VectorId> ids;
    size_t num_vectors = 0;
  };

  // Encode a vector and store in the given cluster
  void EncodeAndStore(ClusterData& cluster, const core::Vector& vector, core::VectorId id);

  // Convert estimated inner product to distance metric
  float IPToDistance(float estimated_ip, float x_norm,
                    const PreprocessedQuery& query) const;

  core::Dimension dimension_;
  core::MetricType metric_type_;
  int nlist_;
  int nprobe_;

  // Faiss coarse quantizer for cluster assignment
  std::unique_ptr<faiss::IndexFlat> coarse_quantizer_;

  // TurboQuant codec (shared across all clusters)
  std::unique_ptr<TurboQuantCodec> codec_;

  // Per-cluster encoded data
  std::vector<ClusterData> clusters_;

  mutable std::shared_mutex mutex_;
  bool trained_ = false;
  size_t total_vectors_ = 0;
};

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_IVF_TURBOQUANT_INDEX_H_
