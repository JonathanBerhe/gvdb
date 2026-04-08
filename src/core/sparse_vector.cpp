// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/sparse_vector.h"
#include "absl/strings/str_cat.h"
#include <algorithm>
#include <cmath>

namespace gvdb {
namespace core {

SparseVector::SparseVector(std::vector<Entry> entries)
    : entries_(std::move(entries)) {}

SparseVector SparseVector::FromArrays(const std::vector<uint32_t>& indices,
                                      const std::vector<float>& values) {
  std::vector<Entry> entries;
  entries.reserve(indices.size());
  for (size_t i = 0; i < indices.size() && i < values.size(); ++i) {
    entries.emplace_back(indices[i], values[i]);
  }
  return SparseVector(std::move(entries));
}

SparseVector SparseVector::FromUnsorted(std::vector<Entry> entries) {
  std::sort(entries.begin(), entries.end(),
            [](const Entry& a, const Entry& b) { return a.first < b.first; });

  // Remove duplicates (keep last value for each index)
  auto it = std::unique(entries.begin(), entries.end(),
                        [](const Entry& a, const Entry& b) {
                          return a.first == b.first;
                        });
  entries.erase(it, entries.end());

  return SparseVector(std::move(entries));
}

const std::vector<SparseVector::Entry>& SparseVector::entries() const noexcept {
  return entries_;
}

size_t SparseVector::nnz() const noexcept {
  return entries_.size();
}

size_t SparseVector::byte_size() const noexcept {
  return entries_.size() * sizeof(Entry);
}

bool SparseVector::empty() const noexcept {
  return entries_.empty();
}

float SparseVector::DotProduct(const SparseVector& other) const {
  float result = 0.0f;
  size_t i = 0, j = 0;
  const auto& a = entries_;
  const auto& b = other.entries_;

  while (i < a.size() && j < b.size()) {
    if (a[i].first == b[j].first) {
      result += a[i].second * b[j].second;
      ++i;
      ++j;
    } else if (a[i].first < b[j].first) {
      ++i;
    } else {
      ++j;
    }
  }
  return result;
}

float SparseVector::L2Norm() const {
  float sum = 0.0f;
  for (const auto& [idx, val] : entries_) {
    sum += val * val;
  }
  return std::sqrt(sum);
}

float SparseVector::CosineSimilarity(const SparseVector& other) const {
  float dot = DotProduct(other);
  float norm_a = L2Norm();
  float norm_b = other.L2Norm();
  if (norm_a < 1e-8f || norm_b < 1e-8f) return 0.0f;
  return dot / (norm_a * norm_b);
}

bool SparseVector::IsValid() const {
  for (size_t i = 0; i < entries_.size(); ++i) {
    if (std::isnan(entries_[i].second)) return false;
    if (i > 0 && entries_[i].first <= entries_[i - 1].first) return false;
  }
  return true;
}

Status SparseVector::Validate() const {
  for (size_t i = 0; i < entries_.size(); ++i) {
    if (std::isnan(entries_[i].second)) {
      return InvalidArgumentError(
          absl::StrCat("NaN value at index ", entries_[i].first));
    }
    if (i > 0 && entries_[i].first <= entries_[i - 1].first) {
      return InvalidArgumentError(
          absl::StrCat("Entries not sorted or contain duplicates at position ", i));
    }
  }
  return OkStatus();
}

}  // namespace core
}  // namespace gvdb
