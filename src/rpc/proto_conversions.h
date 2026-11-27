// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/metadata.h"
#include "core/sparse_vector.h"
#include "core/types.h"
#include "core/vector.h"
#include "meta/node_info.h"
#include "storage/backup/backup_target.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "internal.pb.h"
#include "vectordb.pb.h"
#include "vectordb.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace rpc {

// ============================================================================
// Proto to Core Conversions
// ============================================================================

// Convert proto Vector to core Vector
absl::StatusOr<core::Vector> fromProto(const proto::Vector& proto_vector);

// Convert proto VectorWithId to core Vector with ID
absl::StatusOr<std::pair<core::VectorId, core::Vector>>
fromProto(const proto::VectorWithId& proto_vector_with_id);

// Convert proto MetricType to core MetricType
absl::StatusOr<core::MetricType>
fromProto(proto::CreateCollectionRequest::MetricType metric);

// Convert proto IndexType to core IndexType
absl::StatusOr<core::IndexType>
fromProto(proto::CreateCollectionRequest::IndexType index_type);

// Convert proto Metadata to core Metadata
absl::StatusOr<core::Metadata> fromProto(const proto::Metadata& proto_metadata);

// ============================================================================
// Core to Proto Conversions
// ============================================================================

// Convert core Vector to proto Vector
void toProto(const core::Vector& vector, proto::Vector* proto_vector);

// Convert core SearchResultEntry to proto SearchResultEntry
void toProto(const core::SearchResultEntry& entry, proto::SearchResultEntry* proto_entry);

// Convert core Metadata to proto Metadata
void toProto(const core::Metadata& metadata, proto::Metadata* proto_metadata);

// Sparse vector conversions
absl::StatusOr<core::SparseVector> fromProto(const proto::SparseVector& proto_sparse);

// ============================================================================
// Backup / Restore conversions
// ============================================================================

// Convert proto BackupTarget oneof to storage::BackupTarget variant.
// Returns InvalidArgumentError if the proto oneof is unset.
absl::StatusOr<storage::BackupTarget> fromProto(
    const proto::BackupTarget& proto_target);

// Convert storage::BackupTarget variant back to proto. Used by the
// coordinator's per-shard executor to pass the same target down to
// each data-node's BackupShard handler. Returns InvalidArgumentError
// if the variant is monostate (caller bug).
absl::Status toProto(const storage::BackupTarget& target,
                     proto::BackupTarget* proto_target);

// Convert storage::BackupState to proto BackupState.
proto::BackupState toProto(storage::BackupState state);

// ============================================================================
// Node Conversions
// ============================================================================

// Convert the node info that a node sends with its heartbeat to the
// coordinator's view of the node. READY maps to HEALTHY, DOWN to FAILED and
// DRAINING to LEAVING. An unknown type becomes QUERY_NODE and an unknown
// status HEALTHY. The proto has no heartbeat time or CPU usage, so both are 0.
meta::NodeInfo fromProto(const proto::internal::NodeInfo& proto_info);

// Convert the coordinator's view of a node back to the proto node info.
void toProto(const meta::NodeInfo& info, proto::internal::NodeInfo* proto_info);

// ============================================================================
// Status Conversions
// ============================================================================

// Convert absl::Status to grpc::Status
grpc::Status toGrpcStatus(const absl::Status& status);

// Convert grpc::Status to absl::Status
absl::Status fromGrpcStatus(const grpc::Status& grpc_status);

}  // namespace rpc
} // namespace gvdb
