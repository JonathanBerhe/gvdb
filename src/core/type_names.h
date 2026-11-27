// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Names of the core enums as they appear in configuration files, internal
// RPC fields and log messages, and the parsers back to the enums.

#pragma once

#include <string>

#include "absl/status/statusor.h"
#include "core/types.h"

namespace gvdb::core {

// "L2", "INNER_PRODUCT" or "COSINE"; "UNKNOWN" for any other value.
std::string ToString(MetricType metric);

// "FLAT", "HNSW", "IVF_FLAT", "IVF_PQ", "IVF_SQ", "TURBOQUANT",
// "IVF_TURBOQUANT" or "AUTO"; "UNKNOWN" for any other value.
std::string ToString(IndexType index);

// The inverse of ToString(MetricType). InvalidArgument for an unknown name.
absl::StatusOr<MetricType> MetricTypeFromString(const std::string& metric_str);

// The inverse of ToString(IndexType). InvalidArgument for an unknown name.
absl::StatusOr<IndexType> IndexTypeFromString(const std::string& index_str);

}  // namespace gvdb::core
