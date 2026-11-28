// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "index/faiss/faiss_flat.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>

namespace gvdb::index {

FaissFlatIndex::FaissFlatIndex(core::Dimension dimension, core::MetricType metric)
    : FaissIndexBase(CreateFlatIndex(dimension, metric), metric) {}

std::unique_ptr<faiss::Index> FaissFlatIndex::CreateFlatIndex(core::Dimension dimension,
                                                              core::MetricType metric) {
  faiss::MetricType faiss_metric = ToFaissMetric(metric);

  // Create base flat index
  auto flat_index = new faiss::IndexFlat(dimension, faiss_metric);

  // Wrap with IDMap to support add_with_ids
  return std::make_unique<faiss::IndexIDMap>(flat_index);
}

core::IndexType FaissFlatIndex::GetIndexType() const { return core::IndexType::FLAT; }

}  // namespace gvdb::index
