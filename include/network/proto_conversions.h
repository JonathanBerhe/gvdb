// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/metadata.h"
#include "core/sparse_vector.h"
#include "core/types.h"
#include "core/vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "vectordb.pb.h"
#include "vectordb.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

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

// Convert string to core MetricType (for internal.proto string fields)
absl::StatusOr<core::MetricType> metricTypeFromString(const std::string& metric_str);

// Convert string to core IndexType (for internal.proto string fields)
absl::StatusOr<core::IndexType> indexTypeFromString(const std::string& index_str);

// ============================================================================
// Core to Proto Conversions
// ============================================================================

// Convert core Vector to proto Vector
void toProto(const core::Vector& vector, proto::Vector* proto_vector);

// Convert core SearchResultEntry to proto SearchResultEntry
void toProto(const core::SearchResultEntry& entry, proto::SearchResultEntry* proto_entry);

// Convert core MetricType to string representation
std::string toString(core::MetricType metric);

// Convert core IndexType to string representation
std::string toString(core::IndexType index);

// Convert core Metadata to proto Metadata
void toProto(const core::Metadata& metadata, proto::Metadata* proto_metadata);

// Sparse vector conversions
absl::StatusOr<core::SparseVector> fromProto(const proto::SparseVector& proto_sparse);
void toProto(const core::SparseVector& sparse, proto::SparseVector* proto_sparse);

// ============================================================================
// Status Conversions
// ============================================================================

// Convert absl::Status to grpc::Status
grpc::Status toGrpcStatus(const absl::Status& status);

// Convert grpc::Status to absl::Status
absl::Status fromGrpcStatus(const grpc::Status& grpc_status);

} // namespace network
} // namespace gvdb