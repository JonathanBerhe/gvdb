// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "turboquant_codec.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <random>

namespace gvdb {
namespace index {
namespace turboquant {

namespace {

int NextPowerOf2(int n) {
  if (n <= 1) return 1;
  int p = 1;
  while (p < n) p <<= 1;
  return p;
}

// ============================================================================
// Precomputed Lloyd-Max quantizer tables for N(0,1)
// ============================================================================

// 1-bit (2 levels): threshold at 0, centroids at ±sqrt(2/π)
static const float kLM1Boundaries[] = {0.0f};
static const float kLM1Centroids[] = {-0.7978845608f, 0.7978845608f};

// 2-bit (4 levels)
static const float kLM2Boundaries[] = {-0.9816f, 0.0f, 0.9816f};
static const float kLM2Centroids[] = {-1.510f, -0.4528f, 0.4528f, 1.510f};

// 4-bit (16 levels)
static const float kLM4Boundaries[] = {
    -2.4008f, -1.8436f, -1.4371f, -1.0993f, -0.7975f, -0.5224f, -0.2582f,
    0.0f, 0.2582f, 0.5224f, 0.7975f, 1.0993f, 1.4371f, 1.8436f, 2.4008f};
static const float kLM4Centroids[] = {
    -2.7326f, -2.0690f, -1.6180f, -1.2562f, -0.9424f, -0.6568f, -0.3881f,
    -0.1284f, 0.1284f, 0.3881f, 0.6568f, 0.9424f, 1.2562f, 1.6180f,
    2.0690f, 2.7326f};

// 8-bit (256 levels): uniform quantizer with clip at ±3.5
// Generated programmatically in CreateLloydMaxTable

// Quantize a scalar value to a code using the table's decision boundaries
uint8_t QuantizeScalar(float value, const LloydMaxTable& table) {
  for (int i = 0; i < static_cast<int>(table.boundaries.size()); i++) {
    if (value < table.boundaries[i]) {
      return static_cast<uint8_t>(i);
    }
  }
  return static_cast<uint8_t>(table.num_levels - 1);
}

}  // namespace

// ============================================================================
// LloydMaxTable construction
// ============================================================================

LloydMaxTable CreateLloydMaxTable(int bit_width) {
  LloydMaxTable table;
  table.bit_width = bit_width;
  table.num_levels = 1 << bit_width;

  switch (bit_width) {
    case 1:
      table.boundaries.assign(kLM1Boundaries, kLM1Boundaries + 1);
      table.centroids.assign(kLM1Centroids, kLM1Centroids + 2);
      break;
    case 2:
      table.boundaries.assign(kLM2Boundaries, kLM2Boundaries + 3);
      table.centroids.assign(kLM2Centroids, kLM2Centroids + 4);
      break;
    case 4:
      table.boundaries.assign(kLM4Boundaries, kLM4Boundaries + 15);
      table.centroids.assign(kLM4Centroids, kLM4Centroids + 16);
      break;
    case 8: {
      const float clip = 3.5f;
      const float step = 2.0f * clip / table.num_levels;
      table.boundaries.resize(table.num_levels - 1);
      table.centroids.resize(table.num_levels);
      for (int i = 0; i < table.num_levels - 1; i++) {
        table.boundaries[i] = -clip + static_cast<float>(i + 1) * step;
      }
      for (int i = 0; i < table.num_levels; i++) {
        table.centroids[i] = -clip + (static_cast<float>(i) + 0.5f) * step;
      }
      break;
    }
    default:
      break;
  }
  return table;
}

// ============================================================================
// Walsh-Hadamard Transform
// ============================================================================

void TurboQuantCodec::ApplyWHT(float* data, int n) {
  for (int len = 1; len < n; len <<= 1) {
    for (int i = 0; i < n; i += len << 1) {
      for (int j = 0; j < len; j++) {
        float u = data[i + j];
        float v = data[i + j + len];
        data[i + j] = u + v;
        data[i + j + len] = u - v;
      }
    }
  }
  float inv_sqrt_n = 1.0f / std::sqrt(static_cast<float>(n));
  for (int i = 0; i < n; i++) {
    data[i] *= inv_sqrt_n;
  }
}

void TurboQuantCodec::ApplyForwardRWHT(
    float* data, int n, const std::vector<int8_t>& signs) const {
  // Forward: random sign flip, then WHT
  for (int i = 0; i < n; i++) {
    data[i] *= signs[i];
  }
  ApplyWHT(data, n);
}

void TurboQuantCodec::ApplyInverseRWHT(
    float* data, int n, const std::vector<int8_t>& signs) const {
  // Inverse: WHT, then sign flip (WHT is self-inverse when normalized)
  ApplyWHT(data, n);
  for (int i = 0; i < n; i++) {
    data[i] *= signs[i];
  }
}

// ============================================================================
// Bit packing
// ============================================================================

void TurboQuantCodec::PackCode(uint8_t value, int index,
                                uint8_t* packed) const {
  int values_per_byte = 8 / bit_width_;
  int byte_idx = index / values_per_byte;
  int shift = (index % values_per_byte) * bit_width_;
  uint8_t mask = static_cast<uint8_t>((1 << bit_width_) - 1);
  packed[byte_idx] =
      static_cast<uint8_t>((packed[byte_idx] & ~(mask << shift)) |
                            ((value & mask) << shift));
}

uint8_t TurboQuantCodec::UnpackCode(int index, const uint8_t* packed) const {
  int values_per_byte = 8 / bit_width_;
  int byte_idx = index / values_per_byte;
  int shift = (index % values_per_byte) * bit_width_;
  uint8_t mask = static_cast<uint8_t>((1 << bit_width_) - 1);
  return (packed[byte_idx] >> shift) & mask;
}

void TurboQuantCodec::PackBit(bool value, int index, uint8_t* packed) {
  int byte_idx = index / 8;
  int bit_idx = index % 8;
  if (value) {
    packed[byte_idx] |= static_cast<uint8_t>(1 << bit_idx);
  } else {
    packed[byte_idx] &= static_cast<uint8_t>(~(1 << bit_idx));
  }
}

bool TurboQuantCodec::UnpackBit(int index, const uint8_t* packed) {
  int byte_idx = index / 8;
  int bit_idx = index % 8;
  return (packed[byte_idx] >> bit_idx) & 1;
}

// ============================================================================
// TurboQuantCodec
// ============================================================================

TurboQuantCodec::TurboQuantCodec(core::Dimension dimension, int bit_width,
                                   bool use_qjl, int qjl_dim, uint64_t seed)
    : dimension_(dimension),
      padded_dim_(NextPowerOf2(dimension)),
      bit_width_(bit_width),
      use_qjl_(use_qjl),
      qjl_dim_(qjl_dim > 0 ? qjl_dim : padded_dim_ / 4),
      seed_(seed),
      table_(CreateLloydMaxTable(bit_width)) {
  code_bytes_ = (static_cast<size_t>(padded_dim_) * bit_width_ + 7) / 8;
  qjl_bytes_ = use_qjl_ ? (static_cast<size_t>(qjl_dim_) + 7) / 8 : 0;
  scale_ = std::sqrt(static_cast<float>(padded_dim_));

  // Generate deterministic random signs for primary WHT
  std::mt19937_64 rng(seed_);
  std::uniform_int_distribution<int> dist(0, 1);
  wht_signs_.resize(padded_dim_);
  for (int i = 0; i < padded_dim_; i++) {
    wht_signs_[i] = dist(rng) ? 1 : -1;
  }

  // Generate separate random signs for QJL WHT
  if (use_qjl_) {
    qjl_wht_signs_.resize(padded_dim_);
    for (int i = 0; i < padded_dim_; i++) {
      qjl_wht_signs_[i] = dist(rng) ? 1 : -1;
    }
  }
}

void TurboQuantCodec::Encode(const float* data, core::Dimension dim,
                               std::vector<uint8_t>& codes_out,
                               float& norm_out,
                               float& residual_norm_out,
                               std::vector<uint8_t>& qjl_bits_out) const {
  // Step 1: Compute norm
  float norm_sq = 0.0f;
  for (int i = 0; i < dim; i++) {
    norm_sq += data[i] * data[i];
  }
  norm_out = std::sqrt(norm_sq);

  // Step 2: Normalize and zero-pad to power-of-2 dimension
  std::vector<float> rotated(padded_dim_, 0.0f);
  if (norm_out > 1e-30f) {
    float inv_norm = 1.0f / norm_out;
    for (int i = 0; i < dim; i++) {
      rotated[i] = data[i] * inv_norm;
    }
  }

  // Step 3: Apply randomized WHT (forward rotation)
  ApplyForwardRWHT(rotated.data(), padded_dim_, wht_signs_);

  // Step 4: Scale so each coordinate ≈ N(0,1), then quantize
  codes_out.assign(code_bytes_, 0);
  for (int i = 0; i < padded_dim_; i++) {
    float scaled = rotated[i] * scale_;
    uint8_t code = QuantizeScalar(scaled, table_);
    PackCode(code, i, codes_out.data());
  }

  // Step 5: QJL stage (optional bias correction)
  residual_norm_out = 0.0f;
  qjl_bits_out.clear();
  if (use_qjl_) {
    // Compute residual in rotated space: rotated - dequantized_rotated
    std::vector<float> residual(padded_dim_);
    float res_norm_sq = 0.0f;
    for (int i = 0; i < padded_dim_; i++) {
      float recon = table_.centroids[UnpackCode(i, codes_out.data())] / scale_;
      residual[i] = rotated[i] - recon;
      res_norm_sq += residual[i] * residual[i];
    }
    residual_norm_out = std::sqrt(res_norm_sq);

    // Project residual with second randomized WHT, store sign bits
    ApplyForwardRWHT(residual.data(), padded_dim_, qjl_wht_signs_);
    qjl_bits_out.assign(qjl_bytes_, 0);
    for (int j = 0; j < qjl_dim_; j++) {
      PackBit(residual[j] >= 0.0f, j, qjl_bits_out.data());
    }
  }
}

PreprocessedQuery TurboQuantCodec::PreprocessQuery(
    const float* data, core::Dimension dim) const {
  PreprocessedQuery pq;

  // Compute query norm
  float norm_sq = 0.0f;
  for (int i = 0; i < dim; i++) {
    norm_sq += data[i] * data[i];
  }
  pq.norm = std::sqrt(norm_sq);

  // Normalize and pad
  std::vector<float> rotated(padded_dim_, 0.0f);
  if (pq.norm > 1e-30f) {
    float inv_norm = 1.0f / pq.norm;
    for (int i = 0; i < dim; i++) {
      rotated[i] = data[i] * inv_norm;
    }
  }

  // Apply primary randomized WHT
  ApplyForwardRWHT(rotated.data(), padded_dim_, wht_signs_);

  // Save scaled version for Stage 1 dot product with centroids
  pq.rotated_scaled.resize(padded_dim_);
  for (int i = 0; i < padded_dim_; i++) {
    pq.rotated_scaled[i] = rotated[i] * scale_;
  }

  // Project for QJL correction (second WHT on rotated query)
  if (use_qjl_) {
    ApplyForwardRWHT(rotated.data(), padded_dim_, qjl_wht_signs_);
    pq.qjl_projected.assign(rotated.begin(),
                             rotated.begin() + qjl_dim_);
  }

  return pq;
}

float TurboQuantCodec::EstimateIP(const PreprocessedQuery& query,
                                    const uint8_t* codes, float norm,
                                    float residual_norm,
                                    const uint8_t* qjl_bits) const {
  // Stage 1: dot(centroid_values, query_scaled) * norm * ||q|| / padded_dim
  float dot = 0.0f;
  for (int i = 0; i < padded_dim_; i++) {
    dot += table_.centroids[UnpackCode(i, codes)] * query.rotated_scaled[i];
  }
  float ip = norm * query.norm * dot / static_cast<float>(padded_dim_);

  // Stage 2: QJL correction for residual inner product
  if (use_qjl_ && qjl_bits != nullptr && residual_norm > 1e-30f) {
    float qjl_dot = 0.0f;
    for (int j = 0; j < qjl_dim_; j++) {
      float sign = UnpackBit(j, qjl_bits) ? 1.0f : -1.0f;
      qjl_dot += sign * query.qjl_projected[j];
    }
    // Correction: norm * ||q|| * res_norm * sqrt(2*d/π) / m * qjl_dot
    static constexpr float kPi = 3.14159265358979323846f;
    float factor = std::sqrt(2.0f * static_cast<float>(padded_dim_) / kPi) /
                   static_cast<float>(qjl_dim_);
    ip += norm * query.norm * residual_norm * factor * qjl_dot;
  }

  return ip;
}

void TurboQuantCodec::Decode(const uint8_t* codes, float norm,
                               float* out, core::Dimension dim) const {
  // Dequantize to rotated space (centroid / scale)
  std::vector<float> rotated(padded_dim_);
  for (int i = 0; i < padded_dim_; i++) {
    rotated[i] = table_.centroids[UnpackCode(i, codes)] / scale_;
  }

  // Inverse randomized WHT: WHT then sign flip
  ApplyInverseRWHT(rotated.data(), padded_dim_, wht_signs_);

  // Truncate to original dimension and re-apply norm
  for (int i = 0; i < dim; i++) {
    out[i] = rotated[i] * norm;
  }
}

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb
