// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Server-side bulk import: start, status and cancel.

#include <grpcpp/grpcpp.h>
#include "core/status.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

// ============================================================================
// Server-Side Bulk Import
// ============================================================================

grpc::Status VectorDBService::BulkImport(grpc::ServerContext* /*context*/,
                                         const proto::BulkImportRequest* request,
                                         proto::BulkImportResponse* response) {
  RETURN_IF_ERROR(AuthorizeCollection(auth::Permission::INSERT, request->collection_name()));

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "collection_name is required");
  }
  if (request->source_uri().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "source_uri is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Bulk import is not configured on this server. "
                        "Build with -DGVDB_WITH_S3=ON and configure storage.object_store");
  }

  // Resolve collection metadata
  auto collections = resolver_->ListCollections();
  const service::CollectionInfo* coll_info = nullptr;
  for (const auto& c : collections) {
    if (c.collection_name == request->collection_name()) {
      coll_info = &c;
      break;
    }
  }
  if (!coll_info) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        "Collection not found: " + request->collection_name());
  }

  // Map proto format to internal format
  storage::ImportFormat format;
  switch (request->format()) {
    case proto::PARQUET:
      format = storage::ImportFormat::PARQUET;
      break;
    case proto::NUMPY:
      format = storage::ImportFormat::NUMPY;
      break;
    default:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unsupported import format");
  }

  auto result = bulk_importer_->StartImport(
      request->collection_name(), coll_info->collection_id, coll_info->dimension,
      coll_info->metric_type, coll_info->index_type, request->source_uri(), format,
      request->vector_column().empty() ? "vector" : request->vector_column(),
      request->id_column().empty() ? "id" : request->id_column());

  if (!result.ok()) {
    return rpc::ToGrpcStatus(result.status());
  }

  response->set_import_id(*result);
  response->set_message("Import job started");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetImportStatus(grpc::ServerContext* /*context*/,
                                              const proto::GetImportStatusRequest* request,
                                              proto::GetImportStatusResponse* response) {
  if (request->import_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "import_id is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Bulk import is not configured on this server");
  }

  auto result = bulk_importer_->GetStatus(request->import_id());
  if (!result.ok()) {
    return rpc::ToGrpcStatus(result.status());
  }

  // RBAC: check read access on the import's collection
  RETURN_IF_ERROR(CheckPermission(auth::Permission::SEARCH, result->collection_name));

  auto& status = *result;
  response->set_import_id(status.import_id);
  response->set_state(static_cast<proto::ImportState>(status.state));
  response->set_total_vectors(status.total_vectors);
  response->set_imported_vectors(status.imported_vectors);
  response->set_progress_percent(status.progress_percent);
  response->set_error_message(status.error_message);
  response->set_elapsed_seconds(status.elapsed_seconds);
  response->set_segments_created(status.segments_created);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::CancelImport(grpc::ServerContext* /*context*/,
                                           const proto::CancelImportRequest* request,
                                           proto::CancelImportResponse* response) {
  if (request->import_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "import_id is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Bulk import is not configured on this server");
  }

  // RBAC: look up import's collection, require INSERT (write) permission
  auto job_status = bulk_importer_->GetStatus(request->import_id());
  if (!job_status.ok()) {
    return rpc::ToGrpcStatus(job_status.status());
  }
  RETURN_IF_ERROR(CheckPermission(auth::Permission::INSERT, job_status->collection_name));

  auto status = bulk_importer_->CancelImport(request->import_id());
  if (!status.ok()) {
    return rpc::ToGrpcStatus(status);
  }

  response->set_success(true);
  response->set_message("Cancellation requested");
  return grpc::Status::OK;
}

}  // namespace gvdb::service
