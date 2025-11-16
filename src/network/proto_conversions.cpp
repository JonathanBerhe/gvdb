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
    default:
      return absl::InvalidArgumentError(
          absl::StrFormat("Unknown index type: %d", static_cast<int>(index_type)));
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

} // namespace network
} // namespace gvdb
