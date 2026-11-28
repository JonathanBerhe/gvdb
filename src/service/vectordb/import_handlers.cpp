// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Server-side bulk import: start, status and cancel. Each handler calls
// ImportOps, after the permission check where the handler has one.

#include <grpcpp/grpcpp.h>

#include "core/status.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

grpc::Status VectorDBService::BulkImport(grpc::ServerContext* /*context*/,
                                         const proto::BulkImportRequest* request,
                                         proto::BulkImportResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::INSERT, request->collection_name()));
  return import_.BulkImport(*request, response);
}

grpc::Status VectorDBService::GetImportStatus(grpc::ServerContext* /*context*/,
                                              const proto::GetImportStatusRequest* request,
                                              proto::GetImportStatusResponse* response) {
  return import_.GetImportStatus(*request, response);
}

grpc::Status VectorDBService::CancelImport(grpc::ServerContext* /*context*/,
                                           const proto::CancelImportRequest* request,
                                           proto::CancelImportResponse* response) {
  return import_.CancelImport(*request, response);
}

}  // namespace gvdb::service
