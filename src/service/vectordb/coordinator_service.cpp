// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/vectordb/coordinator_service.h"

#include <utility>

#include "cluster/coordinator/coordinator.h"
#include "common/logger.h"
#include "core/status.h"

namespace gvdb::service {

CoordinatorService::CoordinatorService(std::shared_ptr<cluster::Coordinator> coordinator,
                                       std::shared_ptr<compute::QueryExecutor> query_executor,
                                       std::shared_ptr<auth::RbacStore> rbac_store)
    : resolver_(MakeCoordinatorResolver(coordinator)),
      guard_(std::move(rbac_store)),
      admin_(*resolver_, std::move(query_executor)),
      // Backups fan out to the shards through the coordinator, so the
      // coordinator needs no local managers or segment store.
      backup_(*resolver_, /*segment_store=*/nullptr, guard_, /*backup_manager=*/nullptr,
              /*restore_manager=*/nullptr, std::move(coordinator)) {
  common::Logger::Instance().Info("CoordinatorService initialized (RBAC {})",
                                  guard_.enabled() ? "enabled" : "disabled");
}

CoordinatorService::~CoordinatorService() = default;

grpc::Status CoordinatorService::CreateCollection(grpc::ServerContext* /*context*/,
                                                  const proto::CreateCollectionRequest* request,
                                                  proto::CreateCollectionResponse* response) {
  RETURN_IF_ERROR(
      guard_.Authorize(auth::Permission::CREATE_COLLECTION, request->collection_name()));
  return admin_.CreateCollection(*request, response);
}

grpc::Status CoordinatorService::DropCollection(grpc::ServerContext* /*context*/,
                                                const proto::DropCollectionRequest* request,
                                                proto::DropCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::DROP_COLLECTION, request->collection_name()));
  return admin_.DropCollection(*request, response);
}

grpc::Status CoordinatorService::ListCollections(grpc::ServerContext* /*context*/,
                                                 const proto::ListCollectionsRequest* /*request*/,
                                                 proto::ListCollectionsResponse* response) {
  RETURN_IF_ERROR(guard_.Check(auth::Permission::LIST_COLLECTIONS, ""));
  return admin_.ListCollections(response);
}

grpc::Status CoordinatorService::BackupCollection(grpc::ServerContext* /*context*/,
                                                  const proto::BackupCollectionRequest* request,
                                                  proto::BackupCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::BACKUP, request->collection_name()));
  return backup_.BackupCollection(*request, response);
}

grpc::Status CoordinatorService::RestoreCollection(grpc::ServerContext* /*context*/,
                                                   const proto::RestoreCollectionRequest* request,
                                                   proto::RestoreCollectionResponse* response) {
  RETURN_IF_ERROR(guard_.Authorize(auth::Permission::RESTORE, request->target_collection_name()));
  return backup_.RestoreCollection(*request, response);
}

grpc::Status CoordinatorService::GetBackupStatus(grpc::ServerContext* /*context*/,
                                                 const proto::GetBackupStatusRequest* request,
                                                 proto::GetBackupStatusResponse* response) {
  return backup_.GetBackupStatus(*request, response);
}

grpc::Status CoordinatorService::GetRestoreStatus(grpc::ServerContext* /*context*/,
                                                  const proto::GetRestoreStatusRequest* request,
                                                  proto::GetRestoreStatusResponse* response) {
  return backup_.GetRestoreStatus(*request, response);
}

grpc::Status CoordinatorService::ListBackups(grpc::ServerContext* /*context*/,
                                             const proto::ListBackupsRequest* request,
                                             proto::ListBackupsResponse* response) {
  return backup_.ListBackups(*request, response);
}

grpc::Status CoordinatorService::CancelBackup(grpc::ServerContext* /*context*/,
                                              const proto::CancelBackupRequest* request,
                                              proto::CancelBackupResponse* response) {
  return backup_.CancelBackup(*request, response);
}

grpc::Status CoordinatorService::HealthCheck(grpc::ServerContext* /*context*/,
                                             const proto::HealthCheckRequest* /*request*/,
                                             proto::HealthCheckResponse* response) {
  CollectionAdmin::HealthCheck(response);
  return grpc::Status::OK;
}

grpc::Status CoordinatorService::GetStats(grpc::ServerContext* /*context*/,
                                          const proto::GetStatsRequest* /*request*/,
                                          proto::GetStatsResponse* response) {
  return admin_.GetStats(/*total_queries=*/0, /*total_query_time_ms=*/0, response);
}

}  // namespace gvdb::service
