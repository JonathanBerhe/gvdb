// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "core/metadata.h"
#include "core/sparse_vector.h"
#include "core/types.h"
#include "core/vector.h"
#include "internal.pb.h"
#include "meta/node_info.h"
#include "storage/backup/backup_target.h"
#include "vectordb.grpc.pb.h"
#include "vectordb.pb.h"

namespace gvdb::rpc {

// ============================================================================
// Proto to Core Conversions
// ============================================================================

// Convert proto Vector to core Vector
absl::StatusOr<core::Vector> FromProto(const proto::Vector& proto_vector);

// Convert proto VectorWithId to core Vector with ID
absl::StatusOr<std::pair<core::VectorId, core::Vector>> FromProto(
    const proto::VectorWithId& proto_vector_with_id);

// Convert proto MetricType to core MetricType
absl::StatusOr<core::MetricType> FromProto(proto::CreateCollectionRequest::MetricType metric);

// Convert proto IndexType to core IndexType
absl::StatusOr<core::IndexType> FromProto(proto::CreateCollectionRequest::IndexType index_type);

// Convert proto Metadata to core Metadata
absl::StatusOr<core::Metadata> FromProto(const proto::Metadata& proto_metadata);

// ============================================================================
// Core to Proto Conversions
// ============================================================================

// Convert core Vector to proto Vector
void ToProto(const core::Vector& vector, proto::Vector* proto_vector);

// Convert core SearchResultEntry to proto SearchResultEntry
void ToProto(const core::SearchResultEntry& entry, proto::SearchResultEntry* proto_entry);

// Convert core Metadata to proto Metadata
void ToProto(const core::Metadata& metadata, proto::Metadata* proto_metadata);

// Sparse vector conversions
absl::StatusOr<core::SparseVector> FromProto(const proto::SparseVector& proto_sparse);

// ============================================================================
// Backup / Restore conversions
// ============================================================================

// Convert proto BackupTarget oneof to storage::BackupTarget variant.
// Returns InvalidArgumentError if the proto oneof is unset.
absl::StatusOr<storage::BackupTarget> FromProto(const proto::BackupTarget& proto_target);

// Convert storage::BackupTarget variant back to proto. Used by the
// coordinator's per-shard executor to pass the same target down to
// each data-node's BackupShard handler. Returns InvalidArgumentError
// if the variant is monostate (caller bug).
absl::Status ToProto(const storage::BackupTarget& target, proto::BackupTarget* proto_target);

// Convert storage::BackupState to proto BackupState.
proto::BackupState ToProto(storage::BackupState state);

// ============================================================================
// Node Conversions
// ============================================================================

// Convert the node info that a node sends with its heartbeat to the
// coordinator's view of the node. READY maps to HEALTHY, DOWN to FAILED and
// DRAINING to LEAVING. An unknown type becomes QUERY_NODE and an unknown
// status HEALTHY. The proto has no heartbeat time or CPU usage, so both are 0.
meta::NodeInfo FromProto(const proto::internal::NodeInfo& proto_info);

// Convert the coordinator's view of a node back to the proto node info.
void ToProto(const meta::NodeInfo& info, proto::internal::NodeInfo* proto_info);

// ============================================================================
// Status Conversions
// ============================================================================

// Convert absl::Status to grpc::Status
grpc::Status ToGrpcStatus(const absl::Status& status);

// Convert grpc::Status to absl::Status
absl::Status FromGrpcStatus(const grpc::Status& grpc_status);

}  // namespace gvdb::rpc
