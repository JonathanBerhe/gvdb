// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/proto_conversions.h"
#include "absl/strings/str_format.h"

namespace gvdb {
namespace network {

// ============================================================================
// Proto to Core Conversions
// ============================================================================

absl::StatusOr<core::Vector> fromProto(const proto::Vector& proto_vector) {
  if (proto_vector.values().empty()) {
    return absl::InvalidArgumentError("Vector values cannot be empty");
  }

  if (proto_vector.dimension() != static_cast<uint32_t>(proto_vector.values().size())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Dimension mismatch: declared %d, got %d values",
                        proto_vector.dimension(), proto_vector.values().size()));
  }

  std::vector<float> data(proto_vector.values().begin(), proto_vector.values().end());
  return core::Vector(std::move(data));
}

absl::StatusOr<std::pair<core::VectorId, core::Vector>>
fromProto(const proto::VectorWithId& proto_vector_with_id) {
  auto vector_result = fromProto(proto_vector_with_id.vector());
  if (!vector_result.ok()) {
    return vector_result.status();
  }

  return std::make_pair(static_cast<core::VectorId>(proto_vector_with_id.id()),
                        std::move(*vector_result));
}

absl::StatusOr<core::MetricType>
fromProto(proto::CreateCollectionRequest::MetricType metric) {
  switch (metric) {
    case proto::CreateCollectionRequest::L2:
      return core::MetricType::L2;
    case proto::CreateCollectionRequest::INNER_PRODUCT:
      return core::MetricType::INNER_PRODUCT;
    case proto::CreateCollectionRequest::COSINE:
      return core::MetricType::COSINE;
    default:
      return absl::InvalidArgumentError(
          absl::StrFormat("Unknown metric type: %d", static_cast<int>(metric)));
  }
}

absl::StatusOr<core::IndexType>
fromProto(proto::CreateCollectionRequest::IndexType index_type) {
  switch (index_type) {
    case proto::CreateCollectionRequest::FLAT:
      return core::IndexType::FLAT;
    case proto::CreateCollectionRequest::HNSW:
      return core::IndexType::HNSW;
    case proto::CreateCollectionRequest::IVF_FLAT:
      return core::IndexType::IVF_FLAT;
    case proto::CreateCollectionRequest::IVF_PQ:
      return core::IndexType::IVF_PQ;
    case proto::CreateCollectionRequest::IVF_SQ:
      return core::IndexType::IVF_SQ;
    default:
      return absl::InvalidArgumentError(
          absl::StrFormat("Unknown index type: %d", static_cast<int>(index_type)));
  }
}

absl::StatusOr<core::Metadata> fromProto(const proto::Metadata& proto_metadata) {
  core::Metadata metadata;

  for (const auto& [key, proto_value] : proto_metadata.fields()) {
    // Validate key
    auto key_validation = core::validate_metadata_key(key);
    if (!key_validation.ok()) {
      return key_validation;
    }

    // Convert value based on which field is set
    core::MetadataValue value;
    if (proto_value.has_int_value()) {
      value = proto_value.int_value();
    } else if (proto_value.has_double_value()) {
      value = proto_value.double_value();
    } else if (proto_value.has_string_value()) {
      value = proto_value.string_value();
    } else if (proto_value.has_bool_value()) {
      value = proto_value.bool_value();
    } else {
      return absl::InvalidArgumentError(
          absl::StrFormat("Metadata field '%s' has no value set", key));
    }

    // Validate value
    auto value_validation = core::validate_metadata_value(value);
    if (!value_validation.ok()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Invalid metadata value for key '%s': %s",
                          key, value_validation.message()));
    }

    metadata[key] = std::move(value);
  }

  // Validate complete metadata
  auto metadata_validation = core::validate_metadata(metadata);
  if (!metadata_validation.ok()) {
    return metadata_validation;
  }

  return metadata;
}

absl::StatusOr<core::MetricType> metricTypeFromString(const std::string& metric_str) {
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

absl::StatusOr<core::IndexType> indexTypeFromString(const std::string& index_str) {
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
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("Unknown index type: %s", index_str));
  }
}

// ============================================================================
// Core to Proto Conversions
// ============================================================================

void toProto(const core::Vector& vector, proto::Vector* proto_vector) {
  proto_vector->set_dimension(vector.dimension());
  for (size_t i = 0; i < vector.dimension(); ++i) {
    proto_vector->add_values(vector.data()[i]);
  }
}

void toProto(const core::SearchResultEntry& entry, proto::SearchResultEntry* proto_entry) {
  proto_entry->set_id(core::ToUInt64(entry.id));
  proto_entry->set_distance(entry.distance);
}

std::string toString(core::MetricType metric) {
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

std::string toString(core::IndexType index) {
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
    default:
      return "UNKNOWN";
  }
}

void toProto(const core::Metadata& metadata, proto::Metadata* proto_metadata) {
  for (const auto& [key, value] : metadata) {
    proto::MetadataValue* proto_value = &(*proto_metadata->mutable_fields())[key];

    if (std::holds_alternative<int64_t>(value)) {
      proto_value->set_int_value(std::get<int64_t>(value));
    } else if (std::holds_alternative<double>(value)) {
      proto_value->set_double_value(std::get<double>(value));
    } else if (std::holds_alternative<std::string>(value)) {
      proto_value->set_string_value(std::get<std::string>(value));
    } else if (std::holds_alternative<bool>(value)) {
      proto_value->set_bool_value(std::get<bool>(value));
    }
  }
}

// ============================================================================
// Status Conversions
// ============================================================================

grpc::Status toGrpcStatus(const absl::Status& status) {
  if (status.ok()) {
    return grpc::Status::OK;
  }

  grpc::StatusCode grpc_code;
  switch (status.code()) {
    case absl::StatusCode::kInvalidArgument:
      grpc_code = grpc::StatusCode::INVALID_ARGUMENT;
      break;
    case absl::StatusCode::kNotFound:
      grpc_code = grpc::StatusCode::NOT_FOUND;
      break;
    case absl::StatusCode::kAlreadyExists:
      grpc_code = grpc::StatusCode::ALREADY_EXISTS;
      break;
    case absl::StatusCode::kResourceExhausted:
      grpc_code = grpc::StatusCode::RESOURCE_EXHAUSTED;
      break;
    case absl::StatusCode::kUnimplemented:
      grpc_code = grpc::StatusCode::UNIMPLEMENTED;
      break;
    case absl::StatusCode::kInternal:
    case absl::StatusCode::kUnknown:
    default:
      grpc_code = grpc::StatusCode::INTERNAL;
      break;
  }

  return grpc::Status(grpc_code, std::string(status.message()));
}

absl::Status fromGrpcStatus(const grpc::Status& grpc_status) {
  if (grpc_status.ok()) {
    return absl::OkStatus();
  }

  absl::StatusCode absl_code;
  switch (grpc_status.error_code()) {
    case grpc::StatusCode::INVALID_ARGUMENT:
      absl_code = absl::StatusCode::kInvalidArgument;
      break;
    case grpc::StatusCode::NOT_FOUND:
      absl_code = absl::StatusCode::kNotFound;
      break;
    case grpc::StatusCode::ALREADY_EXISTS:
      absl_code = absl::StatusCode::kAlreadyExists;
      break;
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      absl_code = absl::StatusCode::kResourceExhausted;
      break;
    case grpc::StatusCode::UNIMPLEMENTED:
      absl_code = absl::StatusCode::kUnimplemented;
      break;
    case grpc::StatusCode::UNAVAILABLE:
      absl_code = absl::StatusCode::kUnavailable;
      break;
    case grpc::StatusCode::INTERNAL:
    case grpc::StatusCode::UNKNOWN:
    default:
      absl_code = absl::StatusCode::kInternal;
      break;
  }

  return absl::Status(absl_code, grpc_status.error_message());
}

} // namespace network
} // namespace gvdb