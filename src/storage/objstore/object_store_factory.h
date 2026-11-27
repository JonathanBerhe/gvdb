// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>

#include "core/status.h"
#include "storage/objstore/object_store.h"

namespace gvdb::storage {

// Settings of the object store that holds sealed segments.
//   type == "gcs"            -> Google Cloud Storage (auth via ADC /
//                               Workload Identity; needs -DGVDB_WITH_GCS=ON)
//   type == "s3" | "minio"   -> S3 / MinIO (needs a non-empty endpoint and
//                               -DGVDB_WITH_S3=ON)
//   otherwise / empty        -> object storage disabled (local disk only)
struct ObjectStoreConfig {
  std::string type;        // "s3", "minio", or "gcs"
  std::string endpoint;    // S3/MinIO endpoint, or a GCS emulator
  std::string access_key;  // S3/MinIO only
  std::string secret_key;  // S3/MinIO only
  std::string bucket;
  std::string region;  // S3 only (GCS location is bucket-scoped)
  std::string prefix;
  // GCS only. project id is usually supplied by ADC; credentials_path points
  // at a service-account JSON for local/dev (equivalently set via the
  // GOOGLE_APPLICATION_CREDENTIALS env var). Both empty on GKE Workload
  // Identity.
  std::string project;
  std::string credentials_path;
  bool use_ssl = true;  // S3/MinIO only
};

// Construct the object-store backend selected by the runtime config, so the
// backend dispatch lives in one place instead of being duplicated across every
// server entry point.
//
// Selection (by `config.type`):
//   "gcs"                         -> GcsObjectStore  (needs -DGVDB_WITH_GCS=ON)
//   otherwise, endpoint non-empty -> S3ObjectStore   (needs -DGVDB_WITH_S3=ON;
//                                    path-style addressing when type=="minio")
//   otherwise                     -> disabled
//
// Returns:
//   ok + non-null : the configured backend, ready to use.
//   ok + null     : object storage is disabled, either not configured, or
//                   configured for a backend this binary was not compiled with
//                   (a warning is logged in the latter case so the fall back to
//                   local-only storage is visible rather than silent).
//   error         : the backend is compiled in and configured but creation
//                   failed (e.g. invalid config).
[[nodiscard]] absl::StatusOr<std::unique_ptr<IObjectStore>> CreateObjectStore(
    const ObjectStoreConfig& config);

}  // namespace gvdb::storage
