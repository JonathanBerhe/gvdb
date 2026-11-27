// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Collection create, drop and list, health check and stats.

#include "service/vectordb/vectordb_service.h"
#include "core/type_names.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/collection_resolver.h"
#include "auth/audit_context.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "absl/strings/str_cat.h"
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace service {

// ============================================================================
// Collection Management
// ============================================================================

grpc::Status VectorDBService::CreateCollection(
    grpc::ServerContext* /*context*/,
    const proto::CreateCollectionRequest* request,
    proto::CreateCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::CREATE_COLLECTION, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());

  common::Logger::Instance().Info("CreateCollection: {}", request->collection_name());

  constexpr uint32_t MAX_DIMENSION = 8192; // TODO: remove this fixed value.
  constexpr uint32_t MIN_DIMENSION = 1;

  if (request->dimension() < MIN_DIMENSION || request->dimension() > MAX_DIMENSION) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Dimension must be between ", MIN_DIMENSION, " and ",
                     MAX_DIMENSION, ". Requested: ", request->dimension()));
  }

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Collection name cannot be empty");
  }

  auto metric_result = rpc::fromProto(request->metric());
  if (!metric_result.ok()) return rpc::toGrpcStatus(metric_result.status());

  auto index_type_result = rpc::fromProto(request->index_type());
  if (!index_type_result.ok()) return rpc::toGrpcStatus(index_type_result.status());

  size_t num_shards = request->num_shards() > 0 ? request->num_shards() : 1;
  auto collection_id_result = resolver_->CreateCollection(
      request->collection_name(), request->dimension(),
      *metric_result, *index_type_result, num_shards);

  if (!collection_id_result.ok()) {
    return rpc::toGrpcStatus(collection_id_result.status());
  }

  response->set_collection_id(core::ToUInt32(*collection_id_result));
  response->set_message("Collection created successfully");

  common::MetricsRegistry::Instance().SetCollectionCount(
      resolver_->CollectionCount());

  return grpc::Status::OK;
}

grpc::Status VectorDBService::DropCollection(
    grpc::ServerContext* /*context*/,
    const proto::DropCollectionRequest* request,
    proto::DropCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::DROP_COLLECTION, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());

  common::Logger::Instance().Info("DropCollection: {}", request->collection_name());

  // Get collection ID before drop (for cache invalidation)
  auto cid_before = resolver_->GetCollectionId(request->collection_name());

  auto status = resolver_->DropCollection(request->collection_name());
  if (!status.ok()) return rpc::toGrpcStatus(status);

  common::MetricsRegistry::Instance().SetCollectionCount(
      resolver_->CollectionCount());

  // Invalidate query cache
  if (auto* cache = query_executor_->GetCache()) {
    if (cid_before.ok()) cache->InvalidateCollection(core::ToUInt32(*cid_before));
  }

  response->set_message("Collection dropped successfully");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListCollections(
    grpc::ServerContext* /*context*/,
    const proto::ListCollectionsRequest* /*request*/,
    proto::ListCollectionsResponse* response) {
  auto perm = CheckPermission(auth::Permission::LIST_COLLECTIONS, "");
  if (!perm.ok()) return perm;

  auto collections = resolver_->ListCollections();

  for (const auto& coll : collections) {
    auto* info = response->add_collections();
    info->set_collection_id(core::ToUInt32(coll.collection_id));
    info->set_collection_name(coll.collection_name);
    info->set_dimension(coll.dimension);
    info->set_metric_type(core::toString(coll.metric_type));
    info->set_vector_count(coll.vector_count);
  }

  return grpc::Status::OK;
}

// ============================================================================
// Health and Stats
// ============================================================================

grpc::Status VectorDBService::HealthCheck(
    grpc::ServerContext* /*context*/,
    const proto::HealthCheckRequest* /*request*/,
    proto::HealthCheckResponse* response) {

  response->set_status(proto::HealthCheckResponse::SERVING);
  response->set_message("Server is healthy");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetStats(
    grpc::ServerContext* /*context*/,
    const proto::GetStatsRequest* /*request*/,
    proto::GetStatsResponse* response) {

  auto collections = resolver_->ListCollections();
  uint64_t total_vectors = 0;
  for (const auto& coll : collections) {
    total_vectors += coll.vector_count;
  }

  response->set_total_vectors(total_vectors);
  response->set_total_collections(collections.size());

  uint64_t queries = total_queries_.load(std::memory_order_relaxed);
  uint64_t total_time = total_query_time_ms_.load(std::memory_order_relaxed);

  response->set_total_queries(queries);
  response->set_avg_query_time_ms(
      queries > 0 ? static_cast<float>(total_time) / queries : 0.0f);

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gvdb
