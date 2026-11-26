// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Test vectors and ids.

#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "core/types.h"
#include "core/vector.h"

namespace gvdb {
namespace test {

// `count` vectors with values uniform in [-1, 1]. The same seed gives the
// same vectors, so a failing case can be run again with the same data.
inline std::vector<core::Vector> RandomVectors(size_t count, core::Dimension dimension,
                                               std::uint32_t seed = 42) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<core::Vector> vectors;
  vectors.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    std::vector<float> values(dimension);
    for (auto& v : values) v = dist(rng);
    vectors.emplace_back(std::move(values));
  }
  return vectors;
}

// Ids first, first + 1, ..., first + count - 1.
inline std::vector<core::VectorId> SequentialIds(size_t count, std::uint64_t first = 1) {
  std::vector<core::VectorId> ids;
  ids.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    ids.push_back(core::MakeVectorId(first + i));
  }
  return ids;
}

}  // namespace test
}  // namespace gvdb
