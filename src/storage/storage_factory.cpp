// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/storage_factory.h"

#include "storage/local_storage.h"

namespace gvdb {
namespace storage {

StorageFactory::StorageFactory(core::IIndexFactory* index_factory)
    : index_factory_(index_factory) {}

core::StatusOr<std::unique_ptr<core::IStorage>> StorageFactory::CreateStorage(
    const core::StorageConfig& config) {
  // Validate config
  if (!config.IsValid()) {
    return core::InvalidArgumentError("Invalid storage configuration");
  }

  // Create storage based on type
  switch (config.type) {
    case core::StorageConfig::Type::LOCAL_DISK:
    case core::StorageConfig::Type::MEMORY:
      // Both use LocalStorage for now (MEMORY just uses tmpfs path)
      return std::make_unique<LocalStorage>(config, index_factory_);

    case core::StorageConfig::Type::S3:
      return core::UnimplementedError("S3 storage not yet implemented");

    case core::StorageConfig::Type::MINIO:
      return core::UnimplementedError("MinIO storage not yet implemented");

    default:
      return core::InvalidArgumentError("Unknown storage type");
  }
}

}  // namespace storage
}  // namespace gvdb