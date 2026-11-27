// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Backups and restores fanned out to the data nodes of each shard.

#include "cluster/coordinator/coordinator.h"
#include "core/status.h"
#include "cluster/coordinator/internal_client.h"
#include "rpc/proto_conversions.h"
#include "common/logger.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace cluster {

// =============================================================================
// Backup / Restore orchestration
// =============================================================================
//
// The actual per-shard work runs on the data-node hosting each shard's
// primary (BackupShard / RestoreShard handlers). The coordinator's job
// is only orchestration: pick the primary, bracket BackupShard with
// FreezeWrites / UnfreezeWrites, and stitch the top-level manifest.
//
// Async job tracking lives in the storage-layer BackupManager /
// RestoreManager (one source of truth). Coordinator just provides a
// per-shard executor lambda that turns "do this shard" into a sequence
// of gRPC calls.

void Coordinator::SetBackupManager(
    std::shared_ptr<storage::BackupManager> mgr) {
  backup_manager_ = std::move(mgr);
}

void Coordinator::SetRestoreManager(
    std::shared_ptr<storage::RestoreManager> mgr) {
  restore_manager_ = std::move(mgr);
}

namespace {

// Convert a BackupShardResponse from a data-node back into the
// storage-layer ShardBackupResult that BackupManager's distributed path
// aggregates. Keeps the manifest stitching outside the coordinator.
storage::ShardBackupResult FromBackupShardResponse(
    const proto::internal::BackupShardResponse& resp) {
  storage::ShardBackupResult r;
  r.shard_manifest_key = resp.shard_manifest_key();
  r.bytes_uploaded = resp.bytes_uploaded();
  r.segments.reserve(resp.segments_size());
  for (const auto& s : resp.segments()) {
    storage::ShardBackupSegmentResult entry;
    entry.segment_id = s.segment_id();
    entry.is_growing = s.is_growing();
    entry.vector_count = s.vector_count();
    entry.size_bytes = s.size_bytes();
    entry.object_keys.reserve(s.object_keys_size());
    for (const auto& k : s.object_keys()) entry.object_keys.push_back(k);
    r.segments.push_back(std::move(entry));
  }
  return r;
}

constexpr int64_t kBackupFreezeLeaseMs = 60'000;  // 60 s, matches gate cap

}  // namespace

absl::StatusOr<std::string> Coordinator::StartBackupDistributed(
    const std::string& collection_name,
    const storage::BackupTarget& target,
    const std::string& requested_backup_id) {
  if (!backup_manager_) {
    return absl::UnimplementedError(
        "Backup is not configured on this coordinator");
  }
  auto coll_or = GetCollectionMetadata(collection_name);
  if (!coll_or.ok()) return coll_or.status();
  const auto& coll = *coll_or;

  // Pin (shard_id, primary_term) atomically: a primary swap mid-backup
  // would otherwise produce a manifest stamped with a stale term.
  std::vector<core::ShardId> shard_ids = coll.shard_ids;
  std::vector<uint64_t> primary_terms;
  primary_terms.reserve(shard_ids.size());
  for (auto sid : shard_ids) {
    auto view = shard_manager_->GetPrimaryNodeAndTerm(sid);
    if (!view.ok()) return view.status();
    primary_terms.push_back(view->term);
  }

  // Per-shard executor: dialed on demand, freezes the shard, calls
  // BackupShard, then unfreezes. UnfreezeWrites failure is non-fatal:
  // the data-node's lease auto-expires, and the upload is already done.
  auto* self = this;
  const auto collection_id = coll.collection_id;
  auto executor = [self, collection_id](
      const std::string& backup_id, core::ShardId shard_id,
      uint64_t primary_term,
      const storage::BackupTarget& target_inner)
      -> absl::StatusOr<storage::ShardBackupResult> {
    (void)primary_term;  // recorded for forensics, not consulted here

    auto primary = self->shard_manager_->GetPrimaryNode(shard_id);
    if (!primary.ok()) return primary.status();
    auto* client = self->GetOrCreateDataNodeClient(*primary);
    if (!client) {
      return absl::UnavailableError(absl::StrCat(
          "Cannot dial primary node ", core::ToUInt32(*primary),
          " for shard ", core::ToUInt16(shard_id)));
    }

    // 1. Freeze.
    {
      proto::internal::FreezeWritesRequest req;
      req.set_shard_id(core::ToUInt16(shard_id));
      req.set_fence_token(backup_id);
      req.set_lease_ms(kBackupFreezeLeaseMs);
      proto::internal::FreezeWritesResponse resp;
      grpc::ClientContext ctx;
      auto status = client->FreezeWrites(&ctx, req, &resp);
      if (!status.ok()) {
        return absl::UnavailableError(absl::StrCat(
            "FreezeWrites for shard ", core::ToUInt16(shard_id),
            " failed: ", status.error_message()));
      }
      if (!resp.success()) {
        return absl::FailedPreconditionError(absl::StrCat(
            "FreezeWrites for shard ", core::ToUInt16(shard_id),
            " refused: ", resp.message()));
      }
    }

    // 2. BackupShard. On failure we still attempt to Unfreeze so the
    //    data-node doesn't sit frozen for the full lease.
    proto::internal::BackupShardResponse backup_resp;
    grpc::Status backup_status;
    {
      proto::internal::BackupShardRequest req;
      req.set_backup_id(backup_id);
      req.set_collection_id(core::ToUInt32(collection_id));
      req.set_shard_id(core::ToUInt16(shard_id));
      RETURN_IF_ERROR(rpc::ToProto(target_inner, req.mutable_target()));
      grpc::ClientContext ctx;
      backup_status = client->BackupShard(&ctx, req, &backup_resp);
    }

    // 3. Unfreeze (best effort).
    {
      proto::internal::UnfreezeWritesRequest req;
      req.set_shard_id(core::ToUInt16(shard_id));
      req.set_fence_token(backup_id);
      proto::internal::UnfreezeWritesResponse resp;
      grpc::ClientContext ctx;
      auto unfreeze_status = client->UnfreezeWrites(&ctx, req, &resp);
      if (!unfreeze_status.ok()) {
        common::Logger::Instance().Warn(
            "UnfreezeWrites for shard {} after backup {} failed: {}; "
            "lease will auto-expire",
            core::ToUInt16(shard_id), backup_id,
            unfreeze_status.error_message());
      }
    }

    // 4. Propagate the backup result.
    if (!backup_status.ok()) {
      return absl::UnavailableError(absl::StrCat(
          "BackupShard for shard ", core::ToUInt16(shard_id),
          " failed: ", backup_status.error_message()));
    }
    if (!backup_resp.success()) {
      return absl::InternalError(absl::StrCat(
          "BackupShard for shard ", core::ToUInt16(shard_id),
          " refused: ", backup_resp.message()));
    }
    return FromBackupShardResponse(backup_resp);
  };

  return backup_manager_->StartBackupDistributed(
      collection_name, coll.collection_id, coll.dimension, coll.metric_type,
      coll.index_type, static_cast<uint32_t>(coll.replication_factor),
      shard_ids, primary_terms, target, executor,
      requested_backup_id);
}

absl::StatusOr<std::string> Coordinator::StartRestoreDistributed(
    const storage::BackupTarget& source,
    const std::string& backup_id,
    const std::string& target_collection_name,
    bool overwrite) {
  if (!restore_manager_) {
    return absl::UnimplementedError(
        "Restore is not configured on this coordinator");
  }
  if (backup_id.empty()) {
    return absl::InvalidArgumentError("backup_id is required");
  }
  if (target_collection_name.empty()) {
    return absl::InvalidArgumentError("target_collection_name is required");
  }

  // 1. Read the backup's top manifest to learn the collection shape.
  auto manifest_or = restore_manager_->ReadManifest(source, backup_id);
  if (!manifest_or.ok()) return manifest_or.status();
  const auto& m = *manifest_or;

  // 2. Resolve / create the target collection. If overwrite and the
  //    name already exists, drop it; then create from the manifest.
  {
    auto existing = GetCollectionMetadata(target_collection_name);
    if (existing.ok()) {
      if (!overwrite) {
        return absl::AlreadyExistsError(absl::StrCat(
            "Target collection '", target_collection_name,
            "' already exists; pass overwrite=true to drop and recreate"));
      }
      RETURN_IF_ERROR(DropCollection(target_collection_name));
    }
  }
  auto create_or = CreateCollection(
      target_collection_name,
      static_cast<core::Dimension>(m.collection.dimension),
      static_cast<core::MetricType>(m.collection.metric),
      static_cast<core::IndexType>(m.collection.index_type),
      m.collection.replication_factor,
      m.collection.num_shards);
  if (!create_or.ok()) return create_or.status();
  auto target_collection_id = *create_or;

  // 3. Build the per-shard executor: dispatches RestoreShard to each
  //    shard's primary on the freshly-created collection.
  auto coll_or = GetCollectionMetadata(target_collection_id);
  if (!coll_or.ok()) return coll_or.status();
  std::vector<core::ShardId> shard_ids = coll_or->shard_ids;

  auto* self = this;
  auto executor = [self](
      const storage::BackupTarget& source_inner,
      const std::string& bkp_id,
      core::CollectionId target_id,
      core::ShardId shard_id) -> absl::Status {
    auto primary = self->shard_manager_->GetPrimaryNode(shard_id);
    if (!primary.ok()) return primary.status();
    auto* client = self->GetOrCreateDataNodeClient(*primary);
    if (!client) {
      return absl::UnavailableError(absl::StrCat(
          "Cannot dial primary node ", core::ToUInt32(*primary),
          " for shard ", core::ToUInt16(shard_id)));
    }
    proto::internal::RestoreShardRequest req;
    req.set_backup_id(bkp_id);
    req.set_target_collection_id(core::ToUInt32(target_id));
    req.set_shard_id(core::ToUInt16(shard_id));
    RETURN_IF_ERROR(rpc::ToProto(source_inner, req.mutable_source()));
    proto::internal::RestoreShardResponse resp;
    grpc::ClientContext ctx;
    auto status = client->RestoreShard(&ctx, req, &resp);
    if (!status.ok()) {
      return absl::UnavailableError(absl::StrCat(
          "RestoreShard for shard ", core::ToUInt16(shard_id),
          " failed: ", status.error_message()));
    }
    if (!resp.success()) {
      return absl::InternalError(absl::StrCat(
          "RestoreShard for shard ", core::ToUInt16(shard_id),
          " refused: ", resp.message()));
    }
    return absl::OkStatus();
  };

  return restore_manager_->StartRestoreDistributed(
      source, backup_id, target_collection_id, shard_ids, executor);
}

absl::StatusOr<storage::BackupJobStatus>
Coordinator::GetDistributedBackupStatus(const std::string& backup_id) const {
  if (!backup_manager_) {
    return absl::UnimplementedError(
        "Backup is not configured on this coordinator");
  }
  return backup_manager_->GetStatus(backup_id);
}

absl::StatusOr<storage::RestoreJobStatus>
Coordinator::GetDistributedRestoreStatus(const std::string& restore_id) const {
  if (!restore_manager_) {
    return absl::UnimplementedError(
        "Restore is not configured on this coordinator");
  }
  return restore_manager_->GetStatus(restore_id);
}

}  // namespace cluster
}  // namespace gvdb
