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
#include "meta/collection_metadata.h"

namespace gvdb {
namespace network {

// Thread-safe cache for collection metadata
// Used by data/query nodes to avoid querying coordinator on every request
class CollectionMetadataCache {
 public:
  CollectionMetadataCache() = default;
  ~CollectionMetadataCache() = default;

  // Get collection metadata by name
  absl::StatusOr<meta::CollectionMetadata> GetByName(const std::string& collection_name) const;

  // Add or update collection metadata in cache
  void Put(const meta::CollectionMetadata& metadata);

  // Get number of cached collections
  size_t Size() const;

 private:
  mutable std::shared_mutex mutex_;

  // Dual indexing for fast lookup by name or ID
  absl::flat_hash_map<std::string, meta::CollectionMetadata> by_name_;
  absl::flat_hash_map<core::CollectionId, meta::CollectionMetadata> by_id_;
};

}  // namespace network
}  // namespace gvdb