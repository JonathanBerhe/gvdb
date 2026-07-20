// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_BATCH_SPLITTER_H_
#define GVDB_STORAGE_BATCH_SPLITTER_H_

#include <cstddef>
#include <utility>
#include <vector>

#include "core/status.h"

namespace gvdb {
namespace storage {

// Splits a batch of items into contiguous [begin, end) ranges such that the
// summed cost of each range is at most max_bytes. item_costs[i] must be the
// same figure the target segment's capacity check charges for item i, or a
// range that "fits" here can still be rejected by the segment.
//
// Returns InvalidArgumentError if any single item exceeds max_bytes: such an
// item can never fit a segment, so no amount of rotation helps.
core::StatusOr<std::vector<std::pair<size_t, size_t>>> SplitBatchBySize(
    const std::vector<size_t>& item_costs, size_t max_bytes);

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_BATCH_SPLITTER_H_
