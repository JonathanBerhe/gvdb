// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Collection create, drop and list, health check and stats.

#include <grpcpp/grpcpp.h>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "core/status.h"
#include "core/type_names.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

// ============================================================================
// Collection Management
// ============================================================================

grpc::Status VectorDBService::CreateCollection(grpc::ServerContext* /*context*/,
                                               const proto::CreateCollectionRequest* request,
                                               proto::CreateCollectionResponse* response) {
  RETURN_IF_ERROR(
      guard_.Authorize(auth::Permission::CREATE_COLLECTION, request->collection_name()));

  common::Logger::Instance().Info("CreateCollection: {}", request->collection_name());

  constexpr uint32_t kMaxDimension = 8192;  // TODO: remove this fixed value.
  constexpr uint32_t kMinDimension = 1;

  if (request->dimension() < kMinDimension || request->dimension() > kMaxDimension) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        absl::StrCat("Dimension must be between ", kMinDimension, " and ",
                                     kMaxDimension, ". Requested: ", request->dimension()));
  }

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Collection name cannot be empty");
  }

  auto metric_result = rpc::FromProto(request->metric());
  if (!metric_result.ok()) return rpc::ToGrpcStatus(metric_result.status());

  auto index_type_result = rpc::FromProto(request->index_type());
  if (!index_type_result.ok()) return rpc::ToGrpcStatus(index_type_result.status());

  size_t num_shards = request->num_shards() > 0 ? request->num_shards() : 1;
  auto collection_id_result =
      resolver_->CreateCollection(request->collection_name(), request->dimension(), *metric_result,
                                  *index_type_result, num_shards);

  if (!collection_id_result.ok()) {
    return rpc::ToGrpcStatus(collection_id_result.status());
  }

  response->set_collection_id(core::ToUInt32(*collection_id_result));
  response->set_message("Collection created successfully");

  common::MetricsRegistry::Instance().SetCollectionCount(resolver_->CollectionCount());

  return grpc::Status::OK;
}

grpc::Status VectorDBService::DropCollection(grpc::ServerContext* /*context*/,
                                             const proto::DropCollectionRequest* request,
                                             proto::DropCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::DROP_COLLECTION, request->collection_name()));

  common::Logger::Instance().Info("DropCollection: {}", request->collection_name());

  // Get collection ID before drop (for cache invalidation)
  auto cid_before = resolver_->GetCollectionId(request->collection_name());

  auto status = resolver_->DropCollection(request->collection_name());
  if (!status.ok()) return rpc::ToGrpcStatus(status);

  common::MetricsRegistry::Instance().SetCollectionCount(resolver_->CollectionCount());

  // Invalidate query cache
  if (auto* cache = query_executor_->GetCache()) {
    if (cid_before.ok()) cache->InvalidateCollection(core::ToUInt32(*cid_before));
  }

  response->set_message("Collection dropped successfully");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListCollections(grpc::ServerContext* /*context*/,
                                              const proto::ListCollectionsRequest* /*request*/,
                                              proto::ListCollectionsResponse* response) {
  RETURN_IF_ERROR(guard_.Check(auth::Permission::LIST_COLLECTIONS, ""));

  auto collections = resolver_->ListCollections();

  for (const auto& coll : collections) {
    auto* info = response->add_collections();
    info->set_collection_id(core::ToUInt32(coll.collection_id));
    info->set_collection_name(coll.collection_name);
    info->set_dimension(coll.dimension);
    info->set_metric_type(core::ToString(coll.metric_type));
    info->set_vector_count(coll.vector_count);
  }

  return grpc::Status::OK;
}

// ============================================================================
// Health and Stats
// ============================================================================

grpc::Status VectorDBService::HealthCheck(grpc::ServerContext* /*context*/,
                                          const proto::HealthCheckRequest* /*request*/,
                                          proto::HealthCheckResponse* response) {
  response->set_status(proto::HealthCheckResponse::SERVING);
  response->set_message("Server is healthy");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetStats(grpc::ServerContext* /*context*/,
                                       const proto::GetStatsRequest* /*request*/,
                                       proto::GetStatsResponse* response) {
  auto collections = resolver_->ListCollections();
  uint64_t total_vectors = 0;
  for (const auto& coll : collections) {
    total_vectors += coll.vector_count;
  }

  response->set_total_vectors(total_vectors);
  response->set_total_collections(collections.size());

  uint64_t queries = read_path_.total_queries();
  uint64_t total_time = read_path_.total_query_time_ms();

  response->set_total_queries(queries);
  response->set_avg_query_time_ms(queries > 0 ? static_cast<float>(total_time) / queries : 0.0f);

  return grpc::Status::OK;
}

}  // namespace gvdb::service
