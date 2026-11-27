// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "index/faiss/faiss_base.h"

namespace gvdb {
namespace index {

// FLAT index: brute-force exact search
// Best for small datasets (< 10,000 vectors)
// Guarantees 100% recall but slower for large datasets
class FaissFlatIndex : public FaissIndexBase {
 public:
  FaissFlatIndex(core::Dimension dimension, core::MetricType metric);
  ~FaissFlatIndex() override = default;

  [[nodiscard]] core::IndexType GetIndexType() const override;

 private:
  static std::unique_ptr<faiss::Index> CreateFlatIndex(core::Dimension dimension,
                                                       core::MetricType metric);
};

}  // namespace index
}  // namespace gvdb
