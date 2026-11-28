// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/request_guard.h"
#include "storage/segment/segment_store.h"
#include "vectordb.pb.h"

namespace gvdb {
namespace cluster {
class Coordinator;
}  // namespace cluster
namespace storage {
class BackupManager;
class RestoreManager;
}  // namespace storage

namespace service {

// Backup and restore: start, status, list and cancel. With a coordinator
// (CoordinatorService), each backup and restore fans out to the shards.
// Without one (VectorDBService), the local managers run the single-shard
// path against the segment store; a null manager makes its RPCs return
// UNIMPLEMENTED. The handlers check the permission of BackupCollection and
// RestoreCollection first. CancelBackup checks it here, after the job lookup.
//
// The resolver and the guard must outlive the backup ops.
class BackupOps {
 public:
  BackupOps(ICollectionResolver& resolver, std::shared_ptr<storage::ISegmentStore> segment_store,
            const RequestGuard& guard, std::shared_ptr<storage::BackupManager> backup_manager,
            std::shared_ptr<storage::RestoreManager> restore_manager,
            std::shared_ptr<cluster::Coordinator> coordinator);

  [[nodiscard]] grpc::Status BackupCollection(const proto::BackupCollectionRequest& request,
                                              proto::BackupCollectionResponse* response);
  [[nodiscard]] grpc::Status RestoreCollection(const proto::RestoreCollectionRequest& request,
                                               proto::RestoreCollectionResponse* response);
  [[nodiscard]] grpc::Status GetBackupStatus(const proto::GetBackupStatusRequest& request,
                                             proto::GetBackupStatusResponse* response);
  [[nodiscard]] grpc::Status GetRestoreStatus(const proto::GetRestoreStatusRequest& request,
                                              proto::GetRestoreStatusResponse* response);
  [[nodiscard]] grpc::Status ListBackups(const proto::ListBackupsRequest& request,
                                         proto::ListBackupsResponse* response);
  [[nodiscard]] grpc::Status CancelBackup(const proto::CancelBackupRequest& request,
                                          proto::CancelBackupResponse* response);

  [[nodiscard]] bool backup_enabled() const { return backup_manager_ != nullptr; }
  [[nodiscard]] bool restore_enabled() const { return restore_manager_ != nullptr; }

 private:
  ICollectionResolver& resolver_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  const RequestGuard& guard_;
  std::shared_ptr<storage::BackupManager> backup_manager_;
  std::shared_ptr<storage::RestoreManager> restore_manager_;
  std::shared_ptr<cluster::Coordinator> coordinator_;
};

}  // namespace service
}  // namespace gvdb
