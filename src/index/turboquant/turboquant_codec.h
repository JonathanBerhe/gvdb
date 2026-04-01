// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_TURBOQUANT_CODEC_H_
#define GVDB_INDEX_TURBOQUANT_CODEC_H_

#include <cmath>
#include <cstdint>
#include <vector>

#include "core/types.h"

namespace gvdb {
namespace index {
namespace turboquant {

// Precomputed Lloyd-Max quantizer table for Gaussian N(0,1)
struct LloydMaxTable {
  int bit_width;
  int num_levels;                  // 2^bit_width
  std::vector<float> boundaries;   // num_levels - 1 decision boundaries
  std::vector<float> centroids;    // num_levels reconstruction values
};

// Create a Lloyd-Max table for the given bit width
LloydMaxTable CreateLloydMaxTable(int bit_width);

// Preprocessed query data for efficient batch search
struct PreprocessedQuery {
  float norm;
  std::vector<float> rotated_scaled;   // WHT(q/||q||) * sqrt(padded_dim)
  std::vector<float> qjl_projected;    // WHT2(WHT1(q/||q||)), first qjl_dim entries
};

// Core TurboQuant codec: encoding, decoding, and distance estimation.
//
// Implements the two-stage TurboQuant algorithm:
//   Stage 1 (PolarQuant): Randomized Walsh-Hadamard rotation + Lloyd-Max scalar quantization
//   Stage 2 (QJL):        1-bit sign projections of residual for inner-product debiasing
class TurboQuantCodec {
 public:
  TurboQuantCodec(core::Dimension dimension, int bit_width,
                  bool use_qjl, int qjl_dim = 0, uint64_t seed = 42);

  // Encode a raw vector into packed codes, norm, residual info, and QJL bits.
  void Encode(const float* data, core::Dimension dim,
              std::vector<uint8_t>& codes_out,
              float& norm_out,
              float& residual_norm_out,
              std::vector<uint8_t>& qjl_bits_out) const;

  // Preprocess a query once before scanning all encoded vectors.
  PreprocessedQuery PreprocessQuery(const float* data, core::Dimension dim) const;

  // Estimate inner product between preprocessed query and an encoded vector.
  float EstimateIP(const PreprocessedQuery& query,
                   const uint8_t* codes, float norm,
                   float residual_norm, const uint8_t* qjl_bits) const;

  // Reconstruct a vector from its codes and norm.
  void Decode(const uint8_t* codes, float norm,
              float* out, core::Dimension dim) const;

  // Bytes of packed storage per vector
  size_t CodeBytesPerVector() const { return code_bytes_; }
  size_t QJLBytesPerVector() const { return qjl_bytes_; }

  core::Dimension dimension() const { return dimension_; }
  int padded_dim() const { return padded_dim_; }
  int bit_width() const { return bit_width_; }
  bool use_qjl() const { return use_qjl_; }
  int qjl_dim() const { return qjl_dim_; }
  uint64_t seed() const { return seed_; }

 private:
  // Pure normalized Walsh-Hadamard Transform (in-place, O(d log d))
  static void ApplyWHT(float* data, int n);

  // Randomized WHT: random sign flip then WHT
  void ApplyForwardRWHT(float* data, int n,
                        const std::vector<int8_t>& signs) const;

  // Inverse randomized WHT: WHT then sign flip
  void ApplyInverseRWHT(float* data, int n,
                        const std::vector<int8_t>& signs) const;

  // Bit packing for quantized codes
  void PackCode(uint8_t value, int index, uint8_t* packed) const;
  uint8_t UnpackCode(int index, const uint8_t* packed) const;

  // Single-bit packing for QJL
  static void PackBit(bool value, int index, uint8_t* packed);
  static bool UnpackBit(int index, const uint8_t* packed);

  core::Dimension dimension_;
  int padded_dim_;
  int bit_width_;
  bool use_qjl_;
  int qjl_dim_;
  uint64_t seed_;

  size_t code_bytes_;
  size_t qjl_bytes_;

  LloydMaxTable table_;
  float scale_;  // sqrt(padded_dim)

  std::vector<int8_t> wht_signs_;      // Random signs for primary WHT
  std::vector<int8_t> qjl_wht_signs_;  // Random signs for QJL WHT
};

}  // namespace turboquant
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_TURBOQUANT_CODEC_H_
