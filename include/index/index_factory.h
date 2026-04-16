// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_INDEX_FACTORY_H_
#define GVDB_INDEX_INDEX_FACTORY_H_

#include <memory>

#include "core/config.h"
#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"

namespace gvdb {
namespace index {

// Quantization types for IVF indexes
enum class IVFQuantizationType {
  NONE,  // No quantization (IVF_FLAT)
  PQ,    // Product Quantization (IVF_PQ)
  SQ     // Scalar Quantization (IVF_SQ)
};

// IndexFactory creates vector index instances based on configuration
// Implements the Factory pattern for IVectorIndex
class IndexFactory : public core::IIndexFactory {
 public:
  IndexFactory() = default;
  ~IndexFactory() override = default;

  // Create an index based on configuration
  [[nodiscard]] core::StatusOr<std::unique_ptr<core::IVectorIndex>> CreateIndex(
      const core::IndexConfig& config) override;

  // Create a specific index type with dimension and metric
  [[nodiscard]] static core::StatusOr<std::unique_ptr<core::IVectorIndex>>
  CreateFlatIndex(core::Dimension dimension, core::MetricType metric);

  [[nodiscard]] static core::StatusOr<std::unique_ptr<core::IVectorIndex>>
  CreateHNSWIndex(core::Dimension dimension, core::MetricType metric,
                  int M = 32, int ef_construction = 200, int ef_search = 50);

  [[nodiscard]] static core::StatusOr<std::unique_ptr<core::IVectorIndex>>
  CreateIVFIndex(core::Dimension dimension, core::MetricType metric,
                 int nlist = 100,
                 IVFQuantizationType quantization = IVFQuantizationType::NONE,
                 int pq_m = 8, int pq_nbits = 8, int nprobe = 10);

  [[nodiscard]] static core::StatusOr<std::unique_ptr<core::IVectorIndex>>
  CreateTurboQuantIndex(core::Dimension dimension, core::MetricType metric,
                        int bit_width = 4, bool use_qjl = true,
                        int qjl_dim = 0);

  [[nodiscard]] static core::StatusOr<std::unique_ptr<core::IVectorIndex>>
  CreateIVFTurboQuantIndex(const core::IndexConfig& config);

  // Check if GPU is available for GPU indexes
  [[nodiscard]] static bool IsGPUAvailable();

 private:
  // Helper to validate config
  [[nodiscard]] static core::Status ValidateConfig(
      const core::IndexConfig& config);
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_INDEX_FACTORY_H_