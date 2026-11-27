// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "server/builders.h"

#include <utility>

#include "common/logger.h"
#include "core/config.h"
#include "rpc/interceptors/audit_interceptor.h"
#include "rpc/interceptors/auth_processor.h"
#include "storage/objstore/object_store_factory.h"
#include "storage/segment/segment_cache.h"
#include "storage/segment/segment_manager.h"
#include "storage/segment/tiered_segment_manager.h"

namespace gvdb::server {

absl::StatusOr<config::GVDBConfig> LoadConfig(const std::string& path) {
  if (path.empty()) return config::Config::GetDefault();
  auto loaded = config::Config::LoadFromFile(path);
  if (!loaded.ok()) return WithPrefix("Failed to load config: ", loaded.status());
  return std::move(*loaded);
}

config::GVDBConfig LoadConfigOrDefault(const std::string& path) {
  auto loaded = LoadConfig(path);
  return loaded.ok() ? std::move(*loaded) : config::Config::GetDefault();
}

absl::StatusOr<SegmentStoreParts> BuildSegmentStore(const config::StorageConfig& storage,
                                                    const std::string& data_dir,
                                                    ObjectStoreFailure on_failure) {
  SegmentStoreParts parts;
  parts.index_factory = std::make_unique<index::IndexFactory>();

  // At least 1 MB, so that a segment_max_size_mb of 0 does not seal every
  // segment on its first insert.
  const size_t segment_max_bytes =
      static_cast<size_t>(storage.segment_max_size_mb ? storage.segment_max_size_mb : 1) * 1024 *
      1024;
  auto local_manager = std::make_unique<storage::SegmentManager>(
      data_dir + "/segments", parts.index_factory.get(), segment_max_bytes);

  // The factory picks S3, MinIO, GCS or a folder from the configuration. A
  // null store means that object storage is off.
  auto object_store = storage::CreateObjectStore(storage.object_store);
  if (!object_store.ok()) {
    if (on_failure == ObjectStoreFailure::kStop) {
      return WithPrefix("Failed to create object store: ", object_store.status());
    }
    common::Logger::Instance().Warn(
        "Object store creation failed ({}); falling back to local-only storage",
        object_store.status().message());
  }

  if (object_store.ok() && *object_store) {
    auto cache_size = static_cast<size_t>(storage.object_store_cache_size_mb) * 1024 * 1024;
    auto cache = std::make_unique<storage::SegmentCache>(data_dir + "/cache", cache_size);
    auto prefix = storage.object_store.prefix.empty() ? "gvdb" : storage.object_store.prefix;
    parts.object_store = object_store->get();
    parts.segment_store = std::make_shared<storage::TieredSegmentManager>(
        std::move(local_manager), std::move(*object_store), std::move(cache), prefix,
        storage.object_store_upload_threads);
  } else {
    parts.segment_store = std::shared_ptr<storage::SegmentManager>(std::move(local_manager));
  }

  if (auto status = parts.segment_store->LoadAllSegments(); !status.ok()) {
    common::Logger::Instance().Warn("Failed to load existing segments on startup: {}",
                                    status.message());
  }
  return parts;
}

void SealFullSegmentsInline(const std::shared_ptr<storage::ISegmentStore>& segment_store) {
  segment_store->SetSealCallback([segment_store](core::SegmentId sid, core::IndexType idx_type) {
    auto* seg = segment_store->GetSegment(sid);
    if (!seg) return;
    auto config = core::ResolveAutoIndexConfig(idx_type, seg->GetVectorCount(), seg->GetDimension(),
                                               seg->GetMetric());
    common::Logger::Instance().Info("Auto-sealing segment {} ({} vectors, index={})",
                                    core::ToUInt32(sid), seg->GetVectorCount(),
                                    static_cast<int>(config.index_type));
    auto status = segment_store->SealSegment(sid, config);
    if (!status.ok()) {
      common::Logger::Instance().Error("Auto-seal failed: {}", status.message());
      return;
    }
    // Persist the sealed segment. The plain manager writes it to local
    // disk. The tiered manager also schedules the upload to object storage.
    // Without the flush, a sealed segment stays in memory only.
    auto flush_status = segment_store->FlushSegment(sid);
    if (!flush_status.ok()) {
      common::Logger::Instance().Error("Auto-flush failed for segment {}: {}", core::ToUInt32(sid),
                                       flush_status.message());
    }
  });
}

std::shared_ptr<storage::BulkImporter> MakeBulkImporter(const SegmentStoreParts& parts,
                                                        const std::string& data_dir) {
  if (parts.object_store == nullptr) return nullptr;
  return std::make_shared<storage::BulkImporter>(parts.segment_store, parts.object_store,
                                                 data_dir + "/tmp", 2);
}

BackupEngines BuildBackupEngines(const config::StorageConfig& storage, const BackupSetup& setup) {
  storage::BackupManagerOptions backup;
  backup.default_object_store = setup.object_store;
  backup.s3_bucket = storage.object_store.bucket;
  backup.local_root_allowlist = storage.local_backup_dir;
  if (setup.local_segments) backup.flushed_segments_root = setup.data_dir + "/segments";
  backup.tmp_dir = setup.data_dir + "/tmp/backup";
  backup.gvdb_version = setup.version;
  backup.node_id = static_cast<uint32_t>(setup.node_id);

  storage::RestoreManagerOptions restore;
  restore.default_object_store = setup.object_store;
  restore.s3_bucket = storage.object_store.bucket;
  restore.local_root_allowlist = storage.local_backup_dir;
  restore.staging_dir = setup.data_dir + "/tmp/restore";

  return {std::make_shared<storage::BackupManager>(std::move(backup)),
          std::make_shared<storage::RestoreManager>(std::move(restore))};
}

absl::StatusOr<std::shared_ptr<auth::RbacStore>> AddAuthInterceptor(
    const auth::AuthConfig& auth, InterceptorFactories& interceptors) {
  if (!auth.enabled) return std::shared_ptr<auth::RbacStore>();
  auto rbac_store = auth::RbacStore::Create(auth);
  if (!rbac_store.ok()) return WithPrefix("Invalid auth config: ", rbac_store.status());
  std::shared_ptr<auth::RbacStore> store = std::move(*rbac_store);
  interceptors.push_back(std::make_unique<rpc::ApiKeyAuthInterceptorFactory>(store));
  return store;
}

void AddAuditInterceptor(const auth::AuditLogConfig& audit, InterceptorFactories& interceptors) {
  if (!audit.enabled) return;
  auth::AuditLogger::Initialize(audit);
  interceptors.push_back(std::make_unique<rpc::AuditInterceptorFactory>());
}

}  // namespace gvdb::server
