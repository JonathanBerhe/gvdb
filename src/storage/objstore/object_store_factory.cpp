// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/objstore/object_store_factory.h"

#include <string>
#include <utility>

#include "storage/objstore/filesystem_object_store.h"
#include "common/logger.h"

#ifdef GVDB_HAS_S3
#include "storage/objstore/s3_object_store.h"
#endif
#ifdef GVDB_HAS_GCS
#include "storage/objstore/gcs_object_store.h"
#endif

namespace gvdb {
namespace storage {

core::StatusOr<std::unique_ptr<IObjectStore>> CreateObjectStore(
    const ObjectStoreConfig& config) {
  const std::string& type = config.type;

  // Filesystem tier: always available (no SDK). bucket = root directory.
  // Useful for dev / CI / single-node / NFS-mounted cold tiers.
  if (type == "filesystem") {
    if (config.bucket.empty()) {
      return core::InvalidArgumentError(
          "object_store.type=filesystem requires object_store.bucket "
          "(interpreted as the root directory)");
    }
    auto store = FilesystemObjectStore::Create(config.bucket);
    if (!store.ok()) {
      return store.status();
    }
    return std::unique_ptr<IObjectStore>(std::move(*store));
  }

  const bool wants_gcs = (type == "gcs");

  if (wants_gcs) {
#ifdef GVDB_HAS_GCS
    GcsConfig gcs_config;
    gcs_config.bucket = config.bucket;
    gcs_config.project_id = config.project;
    gcs_config.credentials_path = config.credentials_path;
    // Empty in production (ADC + real GCS endpoint); set only to point at a
    // local emulator (fake-gcs-server).
    gcs_config.endpoint = config.endpoint;
    auto store = GcsObjectStore::Create(gcs_config);
    if (!store.ok()) {
      return store.status();
    }
    return std::unique_ptr<IObjectStore>(std::move(*store));
#else
    common::Logger::Instance().Warn(
        "object_store.type=gcs is configured but this binary was built "
        "without -DGVDB_WITH_GCS=ON; object storage is DISABLED (local disk "
        "only). Rebuild with -DGVDB_WITH_GCS=ON to enable it.");
    return std::unique_ptr<IObjectStore>(nullptr);
#endif
  }

  // S3/MinIO activate on a configured endpoint (the historical gate). If the
  // type explicitly names s3/minio but no endpoint is given, that's a
  // misconfiguration, not a deliberate "disabled" — surface it rather than
  // silently running local-only.
  if (config.endpoint.empty()) {
    if (type == "s3" || type == "minio") {
      common::Logger::Instance().Warn(
          "object_store.type=\"{}\" requires object_store.endpoint to be set; "
          "object storage is DISABLED (local disk only).", type);
    }
    return std::unique_ptr<IObjectStore>(nullptr);
  }

  // Endpoint is set and the type is not GCS: S3 / MinIO.
#ifdef GVDB_HAS_S3
  S3Config s3_config;
  s3_config.endpoint = config.endpoint;
  s3_config.access_key = config.access_key;
  s3_config.secret_key = config.secret_key;
  s3_config.bucket = config.bucket;
  s3_config.region = config.region;
  s3_config.use_ssl = config.use_ssl;
  s3_config.path_style = (type == "minio");
  auto store = S3ObjectStore::Create(s3_config);
  if (!store.ok()) {
    return store.status();
  }
  return std::unique_ptr<IObjectStore>(std::move(*store));
#else
  common::Logger::Instance().Warn(
      "object_store.endpoint is configured but this binary was built "
      "without -DGVDB_WITH_S3=ON; object storage is DISABLED (local disk "
      "only). Rebuild with -DGVDB_WITH_S3=ON to enable it.");
  return std::unique_ptr<IObjectStore>(nullptr);
#endif
}

}  // namespace storage
}  // namespace gvdb
