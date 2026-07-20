// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/batch_splitter.h"

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

core::StatusOr<std::vector<std::pair<size_t, size_t>>> SplitBatchBySize(
    const std::vector<size_t>& item_costs, size_t max_bytes) {
  std::vector<std::pair<size_t, size_t>> ranges;
  if (item_costs.empty()) {
    return ranges;
  }

  size_t begin = 0;
  size_t range_cost = 0;
  for (size_t i = 0; i < item_costs.size(); ++i) {
    if (item_costs[i] > max_bytes) {
      return core::InvalidArgumentError(absl::StrCat(
          "Item at index ", i, " requires ", item_costs[i],
          " bytes, which exceeds the maximum segment size of ", max_bytes,
          " bytes; raise storage.segment_max_size_mb or shrink the item"));
    }
    if (range_cost + item_costs[i] > max_bytes) {
      ranges.emplace_back(begin, i);
      begin = i;
      range_cost = 0;
    }
    range_cost += item_costs[i];
  }
  ranges.emplace_back(begin, item_costs.size());

  return ranges;
}

}  // namespace storage
}  // namespace gvdb
