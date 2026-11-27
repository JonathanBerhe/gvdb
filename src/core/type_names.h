// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Names of the core enums as they appear in configuration files, internal
// RPC fields and log messages, and the parsers back to the enums.

#pragma once

#include <string>

#include "absl/status/statusor.h"
#include "core/types.h"

namespace gvdb {
namespace core {

// "L2", "INNER_PRODUCT" or "COSINE"; "UNKNOWN" for any other value.
std::string toString(MetricType metric);

// "FLAT", "HNSW", "IVF_FLAT", "IVF_PQ", "IVF_SQ", "TURBOQUANT",
// "IVF_TURBOQUANT" or "AUTO"; "UNKNOWN" for any other value.
std::string toString(IndexType index);

// The inverse of toString(MetricType). InvalidArgument for an unknown name.
absl::StatusOr<MetricType> metricTypeFromString(const std::string& metric_str);

// The inverse of toString(IndexType). InvalidArgument for an unknown name.
absl::StatusOr<IndexType> indexTypeFromString(const std::string& index_str);

}  // namespace core
}  // namespace gvdb
