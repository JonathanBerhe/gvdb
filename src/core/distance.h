// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The canonical distance of each metric. Every index and every search path
// returns it, sorted ascending, because lower is better for every metric:
//
//   L2             the Euclidean distance (the square root of the sum of
//                  squares)
//   INNER_PRODUCT  minus the dot product
//   COSINE         1 minus the cosine similarity, in [0, 2]; 1 when either
//                  vector is zero
//
// A range search radius is the largest canonical distance that a result can
// have. The functions below convert between the canonical distance and the
// values that the index backends compute.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>

#include "core/types.h"

namespace gvdb::core {

// The smallest norm of a vector that has a direction. A COSINE distance with a
// shorter vector is 1, and a COSINE collection rejects such a vector.
inline constexpr float kMinCosineNorm = 1e-8F;

// The canonical L2 distance from a squared Euclidean distance. A small
// negative estimate (TurboQuant) counts as 0.
[[nodiscard]] inline float CanonicalFromSquaredL2(float squared) {
  return std::sqrt(std::max(0.0F, squared));
}

// The squared Euclidean radius of a canonical L2 radius. A negative radius
// matches nothing, so it stays negative.
[[nodiscard]] inline float SquaredL2Radius(float radius) {
  return radius < 0.0F ? radius : radius * radius;
}

// The canonical distance from a dot product: -dot for INNER_PRODUCT, and
// 1 - dot for COSINE, where the dot product is of unit vectors.
[[nodiscard]] inline float CanonicalFromDot(MetricType metric, float dot) {
  return metric == MetricType::COSINE ? 1.0F - dot : -dot;
}

// The smallest dot product that a canonical radius allows, for INNER_PRODUCT
// and COSINE: the inverse of CanonicalFromDot.
[[nodiscard]] inline float DotThreshold(MetricType metric, float radius) {
  return metric == MetricType::COSINE ? 1.0F - radius : -radius;
}

// Scales `dim` floats to unit length. A zero vector stays zero, so its
// cosine distance to any vector is 1.
inline void NormalizeInPlace(float* data, size_t dim) {
  float sum = 0.0F;
  for (size_t i = 0; i < dim; ++i) sum += data[i] * data[i];
  if (sum <= 0.0F) return;
  const float inv = 1.0F / std::sqrt(sum);
  for (size_t i = 0; i < dim; ++i) data[i] *= inv;
}

}  // namespace gvdb::core
