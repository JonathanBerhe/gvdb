// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Backup and restore: start, status, list and cancel. Each handler calls
// BackupOps, after the permission check where the handler has one.

#include <grpcpp/grpcpp.h>

#include "core/status.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb::service {

grpc::Status VectorDBService::BackupCollection(grpc::ServerContext* /*context*/,
                                               const proto::BackupCollectionRequest* request,
                                               proto::BackupCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::BACKUP, request->collection_name()));
  return backup_.BackupCollection(*request, response);
}

grpc::Status VectorDBService::RestoreCollection(grpc::ServerContext* /*context*/,
                                                const proto::RestoreCollectionRequest* request,
                                                proto::RestoreCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::RESTORE, request->target_collection_name()));
  return backup_.RestoreCollection(*request, response);
}

grpc::Status VectorDBService::GetBackupStatus(grpc::ServerContext* /*context*/,
                                              const proto::GetBackupStatusRequest* request,
                                              proto::GetBackupStatusResponse* response) {
  return backup_.GetBackupStatus(*request, response);
}

grpc::Status VectorDBService::GetRestoreStatus(grpc::ServerContext* /*context*/,
                                               const proto::GetRestoreStatusRequest* request,
                                               proto::GetRestoreStatusResponse* response) {
  return backup_.GetRestoreStatus(*request, response);
}

grpc::Status VectorDBService::ListBackups(grpc::ServerContext* /*context*/,
                                          const proto::ListBackupsRequest* request,
                                          proto::ListBackupsResponse* response) {
  return backup_.ListBackups(*request, response);
}

grpc::Status VectorDBService::CancelBackup(grpc::ServerContext* /*context*/,
                                           const proto::CancelBackupRequest* request,
                                           proto::CancelBackupResponse* response) {
  return backup_.CancelBackup(*request, response);
}

}  // namespace gvdb::service
