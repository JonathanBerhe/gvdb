// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_CORE_CONFIG_H_
#define GVDB_CORE_CONFIG_H_

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>

#include "core/types.h"

namespace gvdb {
namespace core {

// ============================================================================
// IndexConfig - Configuration for vector index creation
// ============================================================================
struct IndexConfig {
  // Index type (FLAT, HNSW, IVF_FLAT, etc.)
  IndexType index_type = IndexType::FLAT;

  // Vector dimension
  Dimension dimension = 0;

  // Distance metric
  MetricType metric_type = MetricType::L2;

  // HNSW-specific parameters
  struct HNSWParams {
    int M = 16;                // Number of connections per layer
    int ef_construction = 200; // Size of dynamic candidate list during construction
    int ef_search = 100;       // Size of dynamic candidate list during search
  } hnsw_params;

  // IVF-specific parameters
  struct IVFParams {
    int nlist = 100;           // Number of clusters/cells
    int nprobe = 10;           // Number of clusters to search
    int pq_m = 8;              // Number of subquantizers for PQ
    int pq_nbits = 8;          // Number of bits per subquantizer
  } ivf_params;

  // TurboQuant-specific parameters
  struct TurboQuantParams {
    int bit_width = 4;           // Bits per dimension (1, 2, 4, or 8)
    bool use_qjl = true;        // Enable QJL bias correction (Stage 2)
    int qjl_projection_dim = 0; // QJL projection dimensions (0 = auto: padded_dim/4)
  } turboquant_params;

  // IVF + TurboQuant parameters
  struct IVFTurboQuantParams {
    int nlist = 100;             // Number of IVF clusters
    int nprobe = 10;             // Clusters to search at query time
    int bit_width = 4;           // TurboQuant bits per dimension (1, 2, 4, 8)
    bool use_qjl = true;        // Enable QJL bias correction
    int qjl_dim = 0;            // QJL projection dim (0 = auto)
  } ivf_turboquant_params;

  // General parameters
  bool use_gpu = false;        // Use GPU acceleration if available

  // Validation
  [[nodiscard]] bool IsValid() const {
    return dimension > 0;
  }
};

// ============================================================================
// Adaptive index parameter constants
// ============================================================================

// Sub-tier boundary within HNSW range
constexpr size_t kAutoTierSmallHnswMax = 100'000;  // 10K–100K → small HNSW

// HNSW parameters — small graphs (10K–100K vectors)
constexpr int kSmallHnswM = 12;
constexpr int kSmallHnswEfConstruction = 128;
constexpr int kSmallHnswEfSearch = 64;

// HNSW parameters — large graphs (100K–500K vectors)
constexpr int kLargeHnswM = 16;
constexpr int kLargeHnswEfConstruction = 200;
constexpr int kLargeHnswEfSearch = 100;

// High-dimensional vectors need more graph edges to maintain recall
constexpr Dimension kHighDimThreshold = 512;
constexpr int kHighDimHnswM = 20;

// ============================================================================
// ResolveAutoIndexConfig - Adaptive index parameters based on segment profile
// ============================================================================
// Returns a complete IndexConfig with parameters tuned for the given vector
// count and dimension. For non-AUTO types, returns an IndexConfig with only
// index_type/dimension/metric_type populated — callers wanting explicit tuning
// should construct IndexConfig directly.
// Tier boundaries from core/types.h: kAutoTierFlatMax, kAutoTierHnswMax, etc.

struct IvfAdaptiveParams {
  int nlist;
  int nprobe;
};

// Compute IVF nlist (≈sqrt(n)) and nprobe (≈sqrt(nlist)) for a given vector count.
// nlist and nprobe are each floored at 1; sqrt is clamped to INT_MAX to guard
// against overflow on pathologically large counts.
inline IvfAdaptiveParams ComputeIvfParams(size_t vector_count) {
  constexpr double kIntMax = static_cast<double>(std::numeric_limits<int>::max());
  const double raw_nlist = std::sqrt(static_cast<double>(vector_count));
  const int nlist = std::max(1, static_cast<int>(std::min(raw_nlist, kIntMax)));
  const int nprobe = std::max(
      1, static_cast<int>(std::sqrt(static_cast<double>(nlist))));
  return {nlist, nprobe};
}

inline IndexConfig ResolveAutoIndexConfig(IndexType type, size_t vector_count,
                                          Dimension dimension, MetricType metric) {
  IndexConfig config;
  config.index_type = ResolveAutoIndexType(type, vector_count);
  config.dimension = dimension;
  config.metric_type = metric;

  if (type != IndexType::AUTO) return config;

  if (vector_count < kAutoTierFlatMax) {
    // FLAT — brute force, no parameters to tune

  } else if (vector_count < kAutoTierSmallHnswMax) {
    // Small HNSW: lower connectivity sufficient for small graphs
    config.hnsw_params.M = kSmallHnswM;
    config.hnsw_params.ef_construction = kSmallHnswEfConstruction;
    config.hnsw_params.ef_search = kSmallHnswEfSearch;
    if (dimension > kHighDimThreshold) {
      config.hnsw_params.M = kHighDimHnswM;
    }

  } else if (vector_count < kAutoTierHnswMax) {
    // Large HNSW: full connectivity for recall in denser graphs
    config.hnsw_params.M = kLargeHnswM;
    config.hnsw_params.ef_construction = kLargeHnswEfConstruction;
    config.hnsw_params.ef_search = kLargeHnswEfSearch;
    if (dimension > kHighDimThreshold) {
      config.hnsw_params.M = kHighDimHnswM;
    }

  } else if (vector_count < kAutoTierSqMax) {
    // IVF_SQ: scalar quantization (float32→int8), 4x memory savings
    const auto ivf = ComputeIvfParams(vector_count);
    config.ivf_params.nlist = ivf.nlist;
    config.ivf_params.nprobe = ivf.nprobe;

  } else if (vector_count < kAutoTierTQ4Max) {
    // IVF_TURBOQUANT 4-bit
    const auto ivf = ComputeIvfParams(vector_count);
    config.ivf_turboquant_params.nlist = ivf.nlist;
    config.ivf_turboquant_params.nprobe = ivf.nprobe;
    config.ivf_turboquant_params.bit_width = 4;

  } else {
    // IVF_TURBOQUANT 2-bit (≥100M vectors) — aggressive compression, lowest
    // recall per the TurboQuant paper; reserved for the largest tier.
    const auto ivf = ComputeIvfParams(vector_count);
    config.ivf_turboquant_params.nlist = ivf.nlist;
    config.ivf_turboquant_params.nprobe = ivf.nprobe;
    config.ivf_turboquant_params.bit_width = 2;
  }

  return config;
}

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_CONFIG_H_