// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstdint>
#include <utility>
#include <vector>
#include "core/status.h"

namespace gvdb {
namespace core {

// Sparse vector representation for learned sparse retrieval (SPLADE, etc.)
// Entries are stored sorted by dimension index with no duplicates.
class SparseVector {
 public:
  using Entry = std::pair<uint32_t, float>;  // (dimension_index, value)

  // Default: empty sparse vector
  SparseVector() = default;

  // Construct from entries. Entries MUST be sorted by index with no duplicates.
  explicit SparseVector(std::vector<Entry> entries);

  // Construct from separate index/value arrays (proto-friendly)
  static SparseVector FromArrays(const std::vector<uint32_t>& indices,
                                 const std::vector<float>& values);

  // Copy and move
  SparseVector(const SparseVector&) = default;
  SparseVector& operator=(const SparseVector&) = default;
  SparseVector(SparseVector&&) noexcept = default;
  SparseVector& operator=(SparseVector&&) noexcept = default;

  // Accessors
  [[nodiscard]] const std::vector<Entry>& entries() const noexcept;
  [[nodiscard]] size_t nnz() const noexcept;
  [[nodiscard]] size_t byte_size() const noexcept;
  [[nodiscard]] bool empty() const noexcept;

  // Validation
  [[nodiscard]] absl::Status Validate() const;

 private:
  std::vector<Entry> entries_;
};

}  // namespace core
}  // namespace gvdb
