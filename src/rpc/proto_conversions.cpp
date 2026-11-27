// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "rpc/proto_conversions.h"
#include "core/status.h"
#include "absl/strings/str_format.h"

namespace gvdb {
namespace rpc {

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
    case proto::CreateCollectionRequest::IVF_TURBOQUANT:
      return core::IndexType::IVF_TURBOQUANT;
    case proto::CreateCollectionRequest::TURBOQUANT:
      return core::IndexType::TURBOQUANT;
    case proto::CreateCollectionRequest::AUTO:
      return core::IndexType::AUTO;
    default:
      return absl::InvalidArgumentError(
          absl::StrFormat("Unknown index type: %d", static_cast<int>(index_type)));
  }
}

absl::StatusOr<core::Metadata> fromProto(const proto::Metadata& proto_metadata) {
  core::Metadata metadata;

  for (const auto& [key, proto_value] : proto_metadata.fields()) {
    // Validate key
    RETURN_IF_ERROR(core::validate_metadata_key(key));

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
  RETURN_IF_ERROR(core::validate_metadata(metadata));

  return metadata;
}

// ============================================================================
// Core to Proto Conversions
// ============================================================================

void toProto(const core::Vector& vector, proto::Vector* proto_vector) {
  proto_vector->set_dimension(vector.dimension());
  for (size_t i = 0; i < static_cast<size_t>(vector.dimension()); ++i) {
    proto_vector->add_values(vector.data()[i]);
  }
}

void toProto(const core::SearchResultEntry& entry, proto::SearchResultEntry* proto_entry) {
  proto_entry->set_id(core::ToUInt64(entry.id));
  proto_entry->set_distance(entry.distance);
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
// Sparse Vector Conversions
// ============================================================================

absl::StatusOr<core::SparseVector> fromProto(const proto::SparseVector& proto_sparse) {
  if (proto_sparse.indices_size() != proto_sparse.values_size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Sparse vector indices/values size mismatch: %d indices, %d values",
                        proto_sparse.indices_size(), proto_sparse.values_size()));
  }

  std::vector<uint32_t> indices(proto_sparse.indices().begin(),
                                proto_sparse.indices().end());
  std::vector<float> values(proto_sparse.values().begin(),
                            proto_sparse.values().end());

  auto sparse = core::SparseVector::FromArrays(indices, values);
  RETURN_IF_ERROR(sparse.Validate());

  return sparse;
}

// ============================================================================
// Backup / Restore
// ============================================================================

absl::StatusOr<storage::BackupTarget> fromProto(
    const proto::BackupTarget& proto_target) {
  switch (proto_target.target_case()) {
    case proto::BackupTarget::kS3: {
      const auto& s3 = proto_target.s3();
      if (s3.bucket().empty()) {
        return absl::InvalidArgumentError("S3 BackupTarget missing 'bucket'");
      }
      return storage::BackupTarget{
          storage::S3BackupTarget{s3.bucket(), s3.prefix()}};
    }
    case proto::BackupTarget::kLocal: {
      const auto& local = proto_target.local();
      if (local.path().empty()) {
        return absl::InvalidArgumentError("Local BackupTarget missing 'path'");
      }
      return storage::BackupTarget{storage::LocalBackupTarget{local.path()}};
    }
    case proto::BackupTarget::TARGET_NOT_SET:
    default:
      return absl::InvalidArgumentError(
          "BackupTarget is unset (must select s3 or local)");
  }
}

absl::Status toProto(const storage::BackupTarget& target,
                     proto::BackupTarget* proto_target) {
  if (proto_target == nullptr) {
    return absl::InvalidArgumentError("proto_target is null");
  }
  if (std::holds_alternative<std::monostate>(target)) {
    return absl::InvalidArgumentError("BackupTarget variant is unset");
  }
  if (auto* s3 = std::get_if<storage::S3BackupTarget>(&target)) {
    auto* p = proto_target->mutable_s3();
    p->set_bucket(s3->bucket);
    p->set_prefix(s3->prefix);
    return absl::OkStatus();
  }
  if (auto* loc = std::get_if<storage::LocalBackupTarget>(&target)) {
    auto* p = proto_target->mutable_local();
    p->set_path(loc->path);
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError("Unknown BackupTarget variant");
}

proto::BackupState toProto(storage::BackupState state) {
  switch (state) {
    case storage::BackupState::PENDING:   return proto::BACKUP_PENDING;
    case storage::BackupState::RUNNING:   return proto::BACKUP_RUNNING;
    case storage::BackupState::COMPLETED: return proto::BACKUP_COMPLETED;
    case storage::BackupState::FAILED:    return proto::BACKUP_FAILED;
    case storage::BackupState::CANCELLED: return proto::BACKUP_CANCELLED;
  }
  return proto::BACKUP_PENDING;
}

// ============================================================================
// Node Conversions
// ============================================================================

meta::NodeInfo fromProto(const proto::internal::NodeInfo& proto_info) {
  meta::NodeInfo info;
  info.node_id = core::MakeNodeId(proto_info.node_id());

  // Convert node type
  switch (proto_info.node_type()) {
    case proto::internal::NodeType::NODE_TYPE_COORDINATOR:
      info.type = meta::NodeType::COORDINATOR;
      break;
    case proto::internal::NodeType::NODE_TYPE_DATA_NODE:
      info.type = meta::NodeType::DATA_NODE;
      break;
    case proto::internal::NodeType::NODE_TYPE_QUERY_NODE:
      info.type = meta::NodeType::QUERY_NODE;
      break;
    case proto::internal::NodeType::NODE_TYPE_PROXY:
      info.type = meta::NodeType::PROXY;
      break;
    default:
      info.type = meta::NodeType::QUERY_NODE;  // Default
      break;
  }

  // Convert status - map proto READY to HEALTHY
  switch (proto_info.status()) {
    case proto::internal::NodeStatus::NODE_STATUS_STARTING:
      info.status = meta::NodeStatus::STARTING;
      break;
    case proto::internal::NodeStatus::NODE_STATUS_READY:
      info.status = meta::NodeStatus::HEALTHY;  // Map READY to HEALTHY
      break;
    case proto::internal::NodeStatus::NODE_STATUS_DEGRADED:
      info.status = meta::NodeStatus::DEGRADED;
      break;
    case proto::internal::NodeStatus::NODE_STATUS_DOWN:
      info.status = meta::NodeStatus::FAILED;  // Map DOWN to FAILED
      break;
    case proto::internal::NodeStatus::NODE_STATUS_DRAINING:
      info.status = meta::NodeStatus::LEAVING;  // Graceful shutdown in progress
      break;
    default:
      info.status = meta::NodeStatus::HEALTHY;  // Default
      break;
  }

  info.address = proto_info.grpc_address();
  info.memory_capacity_bytes = proto_info.memory_total_bytes();
  info.memory_used_bytes = proto_info.memory_used_bytes();
  info.disk_capacity_bytes = proto_info.disk_total_bytes();
  info.disk_used_bytes = proto_info.disk_used_bytes();
  info.cpu_usage_percent = 0.0f;  // Not in proto, leave at 0
  info.last_heartbeat_ts = 0;  // Not available from proto, handled by NodeRegistry

  return info;
}

void toProto(const meta::NodeInfo& info, proto::internal::NodeInfo* proto_info) {
  proto_info->set_node_id(core::ToUInt32(info.node_id));

  switch (info.type) {
    case meta::NodeType::COORDINATOR:
      proto_info->set_node_type(proto::internal::NodeType::NODE_TYPE_COORDINATOR);
      break;
    case meta::NodeType::DATA_NODE:
      proto_info->set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
      break;
    case meta::NodeType::QUERY_NODE:
      proto_info->set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
      break;
    case meta::NodeType::PROXY:
      proto_info->set_node_type(proto::internal::NodeType::NODE_TYPE_PROXY);
      break;
  }

  switch (info.status) {
    case meta::NodeStatus::STARTING:
      proto_info->set_status(proto::internal::NodeStatus::NODE_STATUS_STARTING);
      break;
    case meta::NodeStatus::HEALTHY:
      proto_info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
      break;
    case meta::NodeStatus::DEGRADED:
      proto_info->set_status(proto::internal::NodeStatus::NODE_STATUS_DEGRADED);
      break;
    case meta::NodeStatus::FAILED:
      proto_info->set_status(proto::internal::NodeStatus::NODE_STATUS_DOWN);
      break;
    case meta::NodeStatus::LEAVING:
      proto_info->set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
      break;
  }

  proto_info->set_grpc_address(info.address);
  proto_info->set_memory_total_bytes(info.memory_capacity_bytes);
  proto_info->set_memory_used_bytes(info.memory_used_bytes);
  proto_info->set_disk_total_bytes(info.disk_capacity_bytes);
  proto_info->set_disk_used_bytes(info.disk_used_bytes);
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

}  // namespace rpc
} // namespace gvdb
