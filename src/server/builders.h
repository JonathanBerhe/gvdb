// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "auth/audit_logger.h"
#include "auth/rbac.h"
#include "config/config.h"
#include "index/index_factory.h"
#include "server/bootstrap.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"
#include "storage/import/bulk_importer.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_store.h"

// Builders that turn the YAML configuration into the parts of a server
// program: segment store, backup engines and gRPC interceptors.
namespace gvdb {
namespace server {

// Loads the YAML configuration at `path`, or the defaults when `path` is
// empty. The error message starts with "Failed to load config: ".
absl::StatusOr<config::GVDBConfig> LoadConfig(const std::string& path);

// Like LoadConfig, but returns the defaults when the file does not load.
config::GVDBConfig LoadConfigOrDefault(const std::string& path);

// What BuildSegmentStore does when the object store in the configuration
// cannot be created.
enum class ObjectStoreFailure {
  kStop,       // Return the error.
  kLocalOnly,  // Log a warning and keep the segments on local disk only.
};

struct SegmentStoreParts {
  // The segment managers keep a raw pointer to it, so it must outlive them.
  std::unique_ptr<index::IndexFactory> index_factory;
  std::shared_ptr<storage::ISegmentStore> segment_store;
  // Owned by segment_store. Null when the configuration names no object store.
  storage::IObjectStore* object_store = nullptr;
};

// Builds the segment store of a program that keeps data. The local
// segments live under <data_dir>/segments. When the configuration names an
// object store, the tiered manager puts it behind a cache in
// <data_dir>/cache. The store then loads the segments that are already on
// disk, and a load error is only logged.
absl::StatusOr<SegmentStoreParts> BuildSegmentStore(const config::StorageConfig& storage,
                                                    const std::string& data_dir,
                                                    ObjectStoreFailure on_failure);

// Seals a full segment and flushes it in the calling thread. A program
// without a background index builder uses this.
void SealFullSegmentsInline(const std::shared_ptr<storage::ISegmentStore>& segment_store);

// The bulk importer reads its files from the object store, so the result is
// null without one. It stages files in <data_dir>/tmp.
std::shared_ptr<storage::BulkImporter> MakeBulkImporter(const SegmentStoreParts& parts,
                                                        const std::string& data_dir);

struct BackupSetup {
  // Null when no object store is configured. Then only local backup targets
  // work.
  storage::IObjectStore* object_store = nullptr;
  std::string data_dir;
  // Written into each backup manifest as the GVDB version: the program role.
  std::string version;
  int node_id = 0;
  // False on the coordinator: it backs up no segments itself, it asks the
  // data nodes to do it.
  bool local_segments = true;
};

struct BackupEngines {
  std::shared_ptr<storage::BackupManager> backup;
  std::shared_ptr<storage::RestoreManager> restore;
};

// Builds the backup and restore managers. They stage files under
// <data_dir>/tmp/backup and <data_dir>/tmp/restore.
BackupEngines BuildBackupEngines(const config::StorageConfig& storage, const BackupSetup& setup);

// Adds the API key interceptor when auth is on. Returns the RBAC store, or
// null when auth is off. The error message starts with "Invalid auth config: ".
absl::StatusOr<std::shared_ptr<auth::RbacStore>> AddAuthInterceptor(
    const auth::AuthConfig& auth, InterceptorFactories& interceptors);

// Starts the audit logger and adds the audit interceptor when the audit log
// is on.
void AddAuditInterceptor(const auth::AuditLogConfig& audit, InterceptorFactories& interceptors);

}  // namespace server
}  // namespace gvdb
