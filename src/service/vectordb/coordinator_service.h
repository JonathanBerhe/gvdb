// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "auth/rbac.h"
#include "compute/query_executor.h"
#include "service/vectordb/backup_ops.h"
#include "service/vectordb/collection_admin.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/request_guard.h"
#include "vectordb.grpc.pb.h"

namespace gvdb {
namespace cluster {
class Coordinator;
}  // namespace cluster

namespace service {

// The client API of the coordinator program. The coordinator keeps the
// collection registry and orchestrates backups, but holds no vectors, so it
// serves only the collection, stats, health and backup RPCs. Every other RPC
// of proto::VectorDBService keeps the generated default and returns
// UNIMPLEMENTED: clients send data operations to the data nodes, through
// the proxy.
class CoordinatorService final : public proto::VectorDBService::Service {
 public:
  // `query_executor` is the executor of the coordinator's own segment
  // store; a collection drop invalidates its query cache. A null
  // `rbac_store` turns off RBAC.
  CoordinatorService(std::shared_ptr<cluster::Coordinator> coordinator,
                     std::shared_ptr<compute::QueryExecutor> query_executor,
                     std::shared_ptr<auth::RbacStore> rbac_store = nullptr);

  ~CoordinatorService() override;

  grpc::Status CreateCollection(grpc::ServerContext* context,
                                const proto::CreateCollectionRequest* request,
                                proto::CreateCollectionResponse* response) override;
  grpc::Status DropCollection(grpc::ServerContext* context,
                              const proto::DropCollectionRequest* request,
                              proto::DropCollectionResponse* response) override;
  grpc::Status ListCollections(grpc::ServerContext* context,
                               const proto::ListCollectionsRequest* request,
                               proto::ListCollectionsResponse* response) override;

  grpc::Status BackupCollection(grpc::ServerContext* context,
                                const proto::BackupCollectionRequest* request,
                                proto::BackupCollectionResponse* response) override;
  grpc::Status RestoreCollection(grpc::ServerContext* context,
                                 const proto::RestoreCollectionRequest* request,
                                 proto::RestoreCollectionResponse* response) override;
  grpc::Status GetBackupStatus(grpc::ServerContext* context,
                               const proto::GetBackupStatusRequest* request,
                               proto::GetBackupStatusResponse* response) override;
  grpc::Status GetRestoreStatus(grpc::ServerContext* context,
                                const proto::GetRestoreStatusRequest* request,
                                proto::GetRestoreStatusResponse* response) override;
  grpc::Status ListBackups(grpc::ServerContext* context, const proto::ListBackupsRequest* request,
                           proto::ListBackupsResponse* response) override;
  grpc::Status CancelBackup(grpc::ServerContext* context, const proto::CancelBackupRequest* request,
                            proto::CancelBackupResponse* response) override;

  grpc::Status HealthCheck(grpc::ServerContext* context, const proto::HealthCheckRequest* request,
                           proto::HealthCheckResponse* response) override;
  // The coordinator serves no searches, so its query counters stay 0.
  grpc::Status GetStats(grpc::ServerContext* context, const proto::GetStatsRequest* request,
                        proto::GetStatsResponse* response) override;

 private:
  // The members are initialized in this order: admin_ and backup_ keep
  // references to resolver_, and backup_ to guard_.
  std::unique_ptr<ICollectionResolver> resolver_;
  RequestGuard guard_;
  CollectionAdmin admin_;
  BackupOps backup_;
};

}  // namespace service
}  // namespace gvdb
