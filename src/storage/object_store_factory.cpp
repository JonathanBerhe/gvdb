// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/object_store_factory.h"

#include <string>
#include <utility>

#include "utils/config.h"
#include "utils/logger.h"

#ifdef GVDB_HAS_S3
#include "storage/s3_object_store.h"
#endif
#ifdef GVDB_HAS_GCS
#include "storage/gcs_object_store.h"
#endif

namespace gvdb {
namespace storage {

core::StatusOr<std::unique_ptr<IObjectStore>> CreateObjectStore(
    const utils::StorageConfig& config) {
  const std::string& type = config.object_store_type;
  const bool wants_gcs = (type == "gcs");
  // S3/MinIO activate on a configured endpoint (the historical gate), unless
  // the type explicitly selects GCS.
  const bool wants_s3 = !wants_gcs && !config.object_store_endpoint.empty();

  if (wants_gcs) {
#ifdef GVDB_HAS_GCS
    GcsConfig gcs_config;
    gcs_config.bucket = config.object_store_bucket;
    gcs_config.project_id = config.object_store_project;
    gcs_config.credentials_path = config.object_store_credentials_path;
    // Empty in production (ADC + real GCS endpoint); set only to point at a
    // local emulator (fake-gcs-server).
    gcs_config.endpoint = config.object_store_endpoint;
    auto store = GcsObjectStore::Create(gcs_config);
    if (!store.ok()) {
      return store.status();
    }
    return std::unique_ptr<IObjectStore>(std::move(*store));
#else
    utils::Logger::Instance().Warn(
        "object_store.type=gcs is configured but this binary was built "
        "without -DGVDB_WITH_GCS=ON; object storage is DISABLED (local disk "
        "only). Rebuild with -DGVDB_WITH_GCS=ON to enable it.");
    return std::unique_ptr<IObjectStore>(nullptr);
#endif
  }

  if (wants_s3) {
#ifdef GVDB_HAS_S3
    S3Config s3_config;
    s3_config.endpoint = config.object_store_endpoint;
    s3_config.access_key = config.object_store_access_key;
    s3_config.secret_key = config.object_store_secret_key;
    s3_config.bucket = config.object_store_bucket;
    s3_config.region = config.object_store_region;
    s3_config.use_ssl = config.object_store_use_ssl;
    s3_config.path_style = (type == "minio");
    auto store = S3ObjectStore::Create(s3_config);
    if (!store.ok()) {
      return store.status();
    }
    return std::unique_ptr<IObjectStore>(std::move(*store));
#else
    utils::Logger::Instance().Warn(
        "object_store.endpoint is configured but this binary was built "
        "without -DGVDB_WITH_S3=ON; object storage is DISABLED (local disk "
        "only). Rebuild with -DGVDB_WITH_S3=ON to enable it.");
    return std::unique_ptr<IObjectStore>(nullptr);
#endif
  }

  // No object store configured — local-disk only.
  return std::unique_ptr<IObjectStore>(nullptr);
}

}  // namespace storage
}  // namespace gvdb
