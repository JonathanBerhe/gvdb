// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Collection create, drop and list, health check and stats. Each handler
// checks the permission, then calls CollectionAdmin.

#include <grpcpp/grpcpp.h>

#include "core/status.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

grpc::Status VectorDBService::CreateCollection(grpc::ServerContext* /*context*/,
                                               const proto::CreateCollectionRequest* request,
                                               proto::CreateCollectionResponse* response) {
  RETURN_IF_ERROR(
      guard_.Authorize(auth::Permission::CREATE_COLLECTION, request->collection_name()));
  return admin_.CreateCollection(*request, response);
}

grpc::Status VectorDBService::DropCollection(grpc::ServerContext* /*context*/,
                                             const proto::DropCollectionRequest* request,
                                             proto::DropCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::DROP_COLLECTION, request->collection_name()));
  return admin_.DropCollection(*request, response);
}

grpc::Status VectorDBService::ListCollections(grpc::ServerContext* /*context*/,
                                              const proto::ListCollectionsRequest* /*request*/,
                                              proto::ListCollectionsResponse* response) {
  RETURN_IF_ERROR(guard_.Check(auth::Permission::LIST_COLLECTIONS, ""));
  return admin_.ListCollections(response);
}

grpc::Status VectorDBService::HealthCheck(grpc::ServerContext* /*context*/,
                                          const proto::HealthCheckRequest* /*request*/,
                                          proto::HealthCheckResponse* response) {
  CollectionAdmin::HealthCheck(response);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetStats(grpc::ServerContext* /*context*/,
                                       const proto::GetStatsRequest* /*request*/,
                                       proto::GetStatsResponse* response) {
  return admin_.GetStats(read_path_.total_queries(), read_path_.total_query_time_ms(), response);
}

}  // namespace gvdb::service
