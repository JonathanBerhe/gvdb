#include "index/index_factory.h"

#include "faiss_flat.h"
#include "faiss_hnsw.h"
#include "faiss_ivf.h"

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
                            config.hnsw_params.ef_construction);

    case core::IndexType::IVF_FLAT:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::NONE);

    case core::IndexType::IVF_PQ:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::PQ);

    case core::IndexType::IVF_SQ:
      return CreateIVFIndex(config.dimension, config.metric_type,
                           config.ivf_params.nlist,
                           IVFQuantizationType::SQ);

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

  return std::make_unique<FaissFlatIndex>(dimension, metric);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateHNSWIndex(core::Dimension dimension, core::MetricType metric,
                               int M, int ef_construction) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  if (M <= 0 || M > 64) {
    return core::InvalidArgumentError("M must be in range (0, 64]");
  }

  if (ef_construction <= 0) {
    return core::InvalidArgumentError("ef_construction must be positive");
  }

  return std::make_unique<FaissHNSWIndex>(dimension, metric, M, ef_construction);
}

core::StatusOr<std::unique_ptr<core::IVectorIndex>>
IndexFactory::CreateIVFIndex(core::Dimension dimension, core::MetricType metric,
                              int nlist, IVFQuantizationType quantization,
                              int pq_m, int pq_nbits) {
  if (dimension <= 0) {
    return core::InvalidArgumentError("Dimension must be positive");
  }

  if (nlist <= 0) {
    return core::InvalidArgumentError("nlist must be positive");
  }

  return std::make_unique<FaissIVFIndex>(dimension, metric, nlist, 10,
                                         quantization, pq_m, pq_nbits);
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
