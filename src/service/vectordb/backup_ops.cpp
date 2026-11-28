// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Backup and restore: start, status, list and cancel.

#include <utility>

#include "cluster/coordinator/coordinator.h"
#include "core/status.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/backup_ops.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"

namespace gvdb::service {

BackupOps::BackupOps(ICollectionResolver& resolver,
                     std::shared_ptr<storage::ISegmentStore> segment_store,
                     const RequestGuard& guard,
                     std::shared_ptr<storage::BackupManager> backup_manager,
                     std::shared_ptr<storage::RestoreManager> restore_manager,
                     std::shared_ptr<cluster::Coordinator> coordinator)
    : resolver_(resolver),
      segment_store_(std::move(segment_store)),
      guard_(guard),
      backup_manager_(std::move(backup_manager)),
      restore_manager_(std::move(restore_manager)),
      coordinator_(std::move(coordinator)) {}

grpc::Status BackupOps::BackupCollection(const proto::BackupCollectionRequest& request,
                                         proto::BackupCollectionResponse* response) {
  if (request.collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "collection_name is required");
  }

  auto target = rpc::FromProto(request.target());
  if (!target.ok()) {
    return rpc::ToGrpcStatus(target.status());
  }

  // Dispatch to coordinator-side multi-shard orchestration when wired
  // (coordinator binary). Falls through to the local single-shard path
  // on a single-node binary.
  if (coordinator_) {
    auto result = coordinator_->StartBackupDistributed(request.collection_name(), *target,
                                                       request.backup_id());
    if (!result.ok()) return rpc::ToGrpcStatus(result.status());
    response->set_backup_id(*result);
    response->set_message("Backup job started (distributed)");
    return grpc::Status::OK;
  }

  if (!backup_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Backup is not configured on this server");
  }

  // Single-shard path: resolve collection metadata for the manifest.
  // The resolver knows the collection's id, dimension, metric, index
  // type, and shard count.
  auto collections = resolver_.ListCollections();
  const service::CollectionInfo* coll_info = nullptr;
  for (const auto& c : collections) {
    if (c.collection_name == request.collection_name()) {
      coll_info = &c;
      break;
    }
  }
  if (!coll_info) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        "Collection not found: " + request.collection_name());
  }

  auto result = backup_manager_->StartBackupSingleShard(
      request.collection_name(), coll_info->collection_id, coll_info->dimension,
      coll_info->metric_type, coll_info->index_type,
      /*num_shards=*/1,
      /*replication_factor=*/1, core::MakeShardId(0),
      /*primary_term=*/0, *target, segment_store_, request.backup_id());
  if (!result.ok()) {
    return rpc::ToGrpcStatus(result.status());
  }
  response->set_backup_id(*result);
  response->set_message("Backup job started");
  return grpc::Status::OK;
}

grpc::Status BackupOps::RestoreCollection(const proto::RestoreCollectionRequest& request,
                                          proto::RestoreCollectionResponse* response) {
  if (request.backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "backup_id is required");
  }

  auto source = rpc::FromProto(request.source());
  if (!source.ok()) {
    return rpc::ToGrpcStatus(source.status());
  }

  // Coordinator path: it reads the manifest, creates/overwrites the
  // target collection, and fans out RestoreShard. Single-node path
  // (below) restores into the original collection_id since there's no
  // coordinator to allocate a new id.
  if (coordinator_) {
    auto result = coordinator_->StartRestoreDistributed(
        *source, request.backup_id(), request.target_collection_name(), request.overwrite());
    if (!result.ok()) return rpc::ToGrpcStatus(result.status());
    response->set_restore_id(*result);
    response->set_message("Restore job started (distributed)");
    return grpc::Status::OK;
  }

  if (!restore_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Restore is not configured on this server");
  }

  auto manifest = restore_manager_->ReadManifest(*source, request.backup_id());
  if (!manifest.ok()) {
    return rpc::ToGrpcStatus(manifest.status());
  }

  // Mirror the coordinator-orchestrated path: drop the target if
  // overwrite is set, then create a fresh collection entry from the
  // manifest's metadata. Without this the restored segments would
  // install via AddReplicatedSegment but the resolver would have no
  // collection entry: Search / Get would return NotFound. The
  // segment-id remap inside RestoreManager::RunShardRestore reconciles
  // ids when the freshly-created collection's id differs from the
  // backup's.
  const std::string target_name = !request.target_collection_name().empty()
                                      ? request.target_collection_name()
                                      : manifest->collection.collection_name;

  auto existing = resolver_.GetCollectionId(target_name);
  if (existing.ok()) {
    if (!request.overwrite()) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS,
                          "Target collection '" + target_name +
                              "' already exists; set overwrite=true to drop and recreate");
    }
    auto drop_status = resolver_.DropCollection(target_name);
    if (!drop_status.ok()) return rpc::ToGrpcStatus(drop_status);
  }
  auto create_or = resolver_.CreateCollection(
      target_name, static_cast<core::Dimension>(manifest->collection.dimension),
      static_cast<core::MetricType>(manifest->collection.metric),
      static_cast<core::IndexType>(manifest->collection.index_type),
      manifest->collection.num_shards);
  if (!create_or.ok()) return rpc::ToGrpcStatus(create_or.status());

  auto result = restore_manager_->StartRestoreSingleShard(*source, request.backup_id(), *create_or,
                                                          core::MakeShardId(0), segment_store_);
  if (!result.ok()) {
    return rpc::ToGrpcStatus(result.status());
  }
  response->set_restore_id(*result);
  response->set_message("Restore job started");
  return grpc::Status::OK;
}

grpc::Status BackupOps::GetBackupStatus(const proto::GetBackupStatusRequest& request,
                                        proto::GetBackupStatusResponse* response) {
  if (request.backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "backup_id is required");
  }
  // Coordinator owns the multi-shard job state; query it when wired.
  // Otherwise consult the local single-shard BackupManager.
  absl::StatusOr<storage::BackupJobStatus> status =
      coordinator_
          ? coordinator_->GetDistributedBackupStatus(request.backup_id())
          : (backup_manager_ ? backup_manager_->GetStatus(request.backup_id())
                             : absl::UnimplementedError("Backup is not configured on this server"));
  if (!status.ok()) {
    return rpc::ToGrpcStatus(status.status());
  }
  response->set_backup_id(status->backup_id);
  response->set_state(rpc::ToProto(status->state));
  response->set_shards_total(status->shards_total);
  response->set_shards_completed(status->shards_completed);
  response->set_bytes_uploaded(status->bytes_uploaded);
  response->set_error_message(status->error_message);
  response->set_elapsed_seconds(status->elapsed_seconds);
  response->set_manifest_uri(status->manifest_uri);
  return grpc::Status::OK;
}

grpc::Status BackupOps::GetRestoreStatus(const proto::GetRestoreStatusRequest& request,
                                         proto::GetRestoreStatusResponse* response) {
  if (request.restore_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "restore_id is required");
  }
  absl::StatusOr<storage::RestoreJobStatus> status =
      coordinator_ ? coordinator_->GetDistributedRestoreStatus(request.restore_id())
                   : (restore_manager_
                          ? restore_manager_->GetStatus(request.restore_id())
                          : absl::UnimplementedError("Restore is not configured on this server"));
  if (!status.ok()) {
    return rpc::ToGrpcStatus(status.status());
  }
  response->set_restore_id(status->restore_id);
  response->set_state(rpc::ToProto(status->state));
  response->set_shards_total(status->shards_total);
  response->set_shards_completed(status->shards_completed);
  response->set_error_message(status->error_message);
  response->set_elapsed_seconds(status->elapsed_seconds);
  return grpc::Status::OK;
}

grpc::Status BackupOps::ListBackups(const proto::ListBackupsRequest& /*request*/,
                                    proto::ListBackupsResponse* /*response*/) {
  // Listing every backup at a target requires walking the target's
  // object store and parsing each top-level manifest. That helper lands
  // with the coordinator orchestration; v1 of this RPC therefore
  // returns UNIMPLEMENTED so clients can detect the gap explicitly
  // instead of silently getting an empty list.
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                      "ListBackups is not yet implemented; use the operator's "
                      "GVDBBackup CRs as the canonical inventory");
}

grpc::Status BackupOps::CancelBackup(const proto::CancelBackupRequest& request,
                                     proto::CancelBackupResponse* response) {
  if (request.backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "backup_id is required");
  }
  if (!backup_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Backup is not configured on this server");
  }
  // RBAC: the caller must hold BACKUP on the underlying collection. We
  // need to look up which collection by id before we can authorize.
  auto status_or = backup_manager_->GetStatus(request.backup_id());
  if (!status_or.ok()) {
    return rpc::ToGrpcStatus(status_or.status());
  }
  // We don't store collection_name on BackupJobStatus directly; do the
  // permission check at the collection-wildcard level by passing the
  // empty name (admin-only paths will reject; others fall through). The
  // strict per-collection gate lands when we surface collection_name on
  // the status struct in a later refinement.
  RETURN_IF_ERROR(guard_.Check(auth::Permission::BACKUP, /*collection_name=*/""));

  auto status = backup_manager_->Cancel(request.backup_id());
  if (!status.ok()) {
    return rpc::ToGrpcStatus(status);
  }
  response->set_success(true);
  response->set_message("Cancellation requested");
  return grpc::Status::OK;
}

}  // namespace gvdb::service
