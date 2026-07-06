// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_OBJECT_STORE_FACTORY_H_
#define GVDB_STORAGE_OBJECT_STORE_FACTORY_H_

#include <memory>

#include "core/status.h"
#include "storage/object_store.h"

namespace gvdb {
namespace utils {
struct StorageConfig;
}  // namespace utils

namespace storage {

// Construct the object-store backend selected by the runtime config, so the
// backend dispatch lives in one place instead of being duplicated across every
// server entry point.
//
// Selection (by `config.object_store_type`):
//   "gcs"                         -> GcsObjectStore  (needs -DGVDB_WITH_GCS=ON)
//   otherwise, endpoint non-empty -> S3ObjectStore   (needs -DGVDB_WITH_S3=ON;
//                                    path-style addressing when type=="minio")
//   otherwise                     -> disabled
//
// Returns:
//   ok + non-null : the configured backend, ready to use.
//   ok + null     : object storage is disabled — either not configured, or
//                   configured for a backend this binary was not compiled with
//                   (a warning is logged in the latter case so the fall back to
//                   local-only storage is visible rather than silent).
//   error         : the backend is compiled in and configured but creation
//                   failed (e.g. invalid config).
[[nodiscard]] core::StatusOr<std::unique_ptr<IObjectStore>> CreateObjectStore(
    const utils::StorageConfig& config);

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_OBJECT_STORE_FACTORY_H_
