// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "index/index_factory.h"

#include "faiss_flat.h"
#include "faiss_hnsw.h"
#include "faiss_ivf.h"
#include "turboquant/turboquant_index.h"
#include "turboquant/ivf_turboquant_index.h"
#ifdef GVDB_HAS_METAL
#include "metal/metal_compute.h"
#include "metal/metal_flat_index.h"
#endif

namespace gvdb {
namespace index {

core::StatusOr<std::unique_ptr<core::IVectorIndex>> IndexFactory::CreateIndex(
    const core::IndexConfig& config) {
  RETURN_IF_ERROR(ValidateConfig(config));

  switch (config.index_type) {
    case core::IndexType::FLAT:
      return CreateFlatIndex(config.dimension, config.metric_type);

    case core::IndexType::HNSW:
      return CreateHNSWIndex(config.dimension, config.metric_type,
                            config.hnsw_params.M,
                            config.hnsw_params.ef_construction,
                            config.hnsw_params.ef_search);

    case core::IndexType::IVF_FLAT:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::NONE,
                           config.ivf_params.pq_m, config.ivf_params.pq_nbits,
                           config.ivf_params.nprobe);

    case core::IndexType::IVF_PQ:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::PQ,
                           config.ivf_params.pq_m, config.ivf_params.pq_nbits,
                           config.ivf_params.nprobe);

    case core::IndexType::IVF_SQ:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::SQ,
                           config.ivf_params.pq_m, config.ivf_params.pq_nbits,
                           config.ivf_params.nprobe);

    case core::IndexType::TURBOQUANT:
      return CreateTurboQuantIndex(config.dimension, config.metric_type,
                                   config.turboquant_params.bit_width,
                                   config.turboquant_params.use_qjl,
                                   config.turboquant_params.qjl_projection_dim);

    case core::IndexType::IVF_TURBOQUANT:
      return CreateIVFTurboQuantIndex(config);

    case core::IndexType::AUTO:
      return core::InvalidArgumentError(
          "AUTO index type must be resolved before reaching IndexFactory");

    default:
      return core::UnimplementedError(
          "Index type not yet implemented");
  }
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateFlatIndex(core::Dimension dimension,
                               core::MetricType metric) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

#ifdef GVDB_HAS_METAL
  if (metal::MetalCompute::IsAvailable()) {
    return std::make_unique<metal::MetalFlatIndex>(dimension, metric);
  }
#endif
  return std::make_unique<FaissFlatIndex>(dimension, metric);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateHNSWIndex(core::Dimension dimension, core::MetricType metric,
                               int M, int ef_construction, int ef_search) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  if (M <= 0 || M > 64) {
    return core::InvalidArgumentError("M must be in range (0, 64]");
  }

  if (ef_construction <= 0) {
    return core::InvalidArgumentError("ef_construction must be positive");
  }

  if (ef_search <= 0) {
    return core::InvalidArgumentError("ef_search must be positive");
  }

  return std::make_unique<FaissHNSWIndex>(dimension, metric, M, ef_construction,
                                          ef_search);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateIVFIndex(core::Dimension dimension, core::MetricType metric,
                              int nlist, IVFQuantizationType quantization,
                              int pq_m, int pq_nbits, int nprobe) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  if (nlist <= 0) {
    return core::InvalidArgumentError("nlist must be positive");
  }

  // Faiss tolerates nprobe > nlist by probing all clusters, so we only
  // require nprobe > 0.
  if (nprobe <= 0) {
    return core::InvalidArgumentError("nprobe must be positive");
  }

  return std::make_unique<FaissIVFIndex>(dimension, metric, nlist, nprobe,
                                         quantization, pq_m, pq_nbits);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateTurboQuantIndex(core::Dimension dimension,
                                     core::MetricType metric,
                                     int bit_width, bool use_qjl,
                                     int qjl_dim) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  if (bit_width != 1 && bit_width != 2 && bit_width != 4 && bit_width != 8) {
    return core::InvalidArgumentError(
        "TurboQuant bit_width must be 1, 2, 4, or 8");
  }

  return std::make_unique<turboquant::TurboQuantIndex>(
      dimension, metric, bit_width, use_qjl, qjl_dim);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateIVFTurboQuantIndex(const core::IndexConfig& config) {
  if (config.dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  const auto& p = config.ivf_turboquant_params;
  if (p.nlist <= 0) {
    return core::InvalidArgumentError("nlist must be positive");
  }
  if (p.bit_width != 1 && p.bit_width != 2 && p.bit_width != 4 && p.bit_width != 8) {
    return core::InvalidArgumentError(
        "IVF_TURBOQUANT bit_width must be 1, 2, 4, or 8");
  }

  return std::make_unique<turboquant::IVFTurboQuantIndex>(
      config.dimension, config.metric_type,
      p.nlist, p.nprobe, p.bit_width, p.use_qjl, p.qjl_dim);
}

bool IndexFactory::IsGPUAvailable() {
  // For now, GPU support is disabled
  // Can be implemented with CUDA availability check
  return false;
}

core::Status IndexFactory::ValidateConfig(const core::IndexConfig& config) {
  if (!config.IsValid()) {
    return core::InvalidArgumentError("Invalid index configuration");
  }

  if (config.use_gpu && !IsGPUAvailable()) {
    return core::UnavailableError("GPU requested but not available");
  }

  return core::OkStatus();
}

}  // namespace index
}  // namespace gvdb