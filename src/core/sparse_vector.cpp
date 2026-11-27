// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/sparse_vector.h"
#include <cmath>
#include "absl/strings/str_cat.h"

namespace gvdb::core {

SparseVector::SparseVector(std::vector<Entry> entries) : entries_(std::move(entries)) {}

SparseVector SparseVector::FromArrays(const std::vector<uint32_t>& indices,
                                      const std::vector<float>& values) {
  std::vector<Entry> entries;
  entries.reserve(indices.size());
  for (size_t i = 0; i < indices.size() && i < values.size(); ++i) {
    entries.emplace_back(indices[i], values[i]);
  }
  return SparseVector(std::move(entries));
}

const std::vector<SparseVector::Entry>& SparseVector::entries() const noexcept { return entries_; }

size_t SparseVector::nnz() const noexcept { return entries_.size(); }

size_t SparseVector::byte_size() const noexcept { return entries_.size() * sizeof(Entry); }

bool SparseVector::empty() const noexcept { return entries_.empty(); }

absl::Status SparseVector::Validate() const {
  for (size_t i = 0; i < entries_.size(); ++i) {
    if (std::isnan(entries_[i].second)) {
      return absl::InvalidArgumentError(absl::StrCat("NaN value at index ", entries_[i].first));
    }
    if (i > 0 && entries_[i].first <= entries_[i - 1].first) {
      return absl::InvalidArgumentError(
          absl::StrCat("Entries not sorted or contain duplicates at position ", i));
    }
  }
  return absl::OkStatus();
}

}  // namespace gvdb::core
