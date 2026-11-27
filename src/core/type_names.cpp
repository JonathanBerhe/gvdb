// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/type_names.h"

#include "absl/strings/str_format.h"

namespace gvdb {
namespace core {

std::string ToString(core::MetricType metric) {
  switch (metric) {
    case core::MetricType::L2:
      return "L2";
    case core::MetricType::INNER_PRODUCT:
      return "INNER_PRODUCT";
    case core::MetricType::COSINE:
      return "COSINE";
    default:
      return "UNKNOWN";
  }
}

std::string ToString(core::IndexType index) {
  switch (index) {
    case core::IndexType::FLAT:
      return "FLAT";
    case core::IndexType::HNSW:
      return "HNSW";
    case core::IndexType::IVF_FLAT:
      return "IVF_FLAT";
    case core::IndexType::IVF_PQ:
      return "IVF_PQ";
    case core::IndexType::IVF_SQ:
      return "IVF_SQ";
    case core::IndexType::TURBOQUANT:
      return "TURBOQUANT";
    case core::IndexType::IVF_TURBOQUANT:
      return "IVF_TURBOQUANT";
    case core::IndexType::AUTO:
      return "AUTO";
    default:
      return "UNKNOWN";
  }
}

absl::StatusOr<core::MetricType> MetricTypeFromString(const std::string& metric_str) {
  if (metric_str == "L2") {
    return core::MetricType::L2;
  } else if (metric_str == "INNER_PRODUCT") {
    return core::MetricType::INNER_PRODUCT;
  } else if (metric_str == "COSINE") {
    return core::MetricType::COSINE;
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("Unknown metric type: %s", metric_str));
  }
}

absl::StatusOr<core::IndexType> IndexTypeFromString(const std::string& index_str) {
  if (index_str == "FLAT") {
    return core::IndexType::FLAT;
  } else if (index_str == "HNSW") {
    return core::IndexType::HNSW;
  } else if (index_str == "IVF_FLAT") {
    return core::IndexType::IVF_FLAT;
  } else if (index_str == "IVF_PQ") {
    return core::IndexType::IVF_PQ;
  } else if (index_str == "IVF_SQ") {
    return core::IndexType::IVF_SQ;
  } else if (index_str == "TURBOQUANT") {
    return core::IndexType::TURBOQUANT;
  } else if (index_str == "IVF_TURBOQUANT") {
    return core::IndexType::IVF_TURBOQUANT;
  } else if (index_str == "AUTO") {
    return core::IndexType::AUTO;
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("Unknown index type: %s", index_str));
  }
}

}  // namespace core
}  // namespace gvdb
