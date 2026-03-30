// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cluster/coordinator.h"

namespace gvdb {
namespace network {

// Use the canonical CollectionMetadata from the cluster module
using CollectionMetadata = cluster::CollectionMetadata;

// Thread-safe cache for collection metadata
// Used by data/query nodes to avoid querying coordinator on every request
class CollectionMetadataCache {
 public:
  CollectionMetadataCache() = default;
  ~CollectionMetadataCache() = default;

  // Get collection metadata by name
  absl::StatusOr<CollectionMetadata> GetByName(const std::string& collection_name) const;

  // Get collection metadata by ID
  absl::StatusOr<CollectionMetadata> GetById(core::CollectionId collection_id) const;

  // Add or update collection metadata in cache
  void Put(const CollectionMetadata& metadata);

  // Remove collection from cache
  void Remove(const std::string& collection_name);
  void Remove(core::CollectionId collection_id);

  // Check if collection exists in cache
  bool Contains(const std::string& collection_name) const;
  bool Contains(core::CollectionId collection_id) const;

  // Clear all cached metadata
  void Clear();

  // Get number of cached collections
  size_t Size() const;

 private:
  mutable std::shared_mutex mutex_;

  // Dual indexing for fast lookup by name or ID
  absl::flat_hash_map<std::string, CollectionMetadata> by_name_;
  absl::flat_hash_map<core::CollectionId, CollectionMetadata> by_id_;
};

}  // namespace network
}  // namespace gvdb