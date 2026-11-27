// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/collection_metadata_cache.h"
#include "absl/strings/str_format.h"

namespace gvdb {
namespace service {

absl::StatusOr<meta::CollectionMetadata> CollectionMetadataCache::GetByName(
    const std::string& collection_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto it = by_name_.find(collection_name);
  if (it == by_name_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("Collection not found in cache: %s", collection_name));
  }

  return it->second;
}

void CollectionMetadataCache::Put(const meta::CollectionMetadata& metadata) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  // Remove old entries if collection name or ID changed
  auto name_it = by_name_.find(metadata.collection_name);
  if (name_it != by_name_.end() && name_it->second.collection_id != metadata.collection_id) {
    by_id_.erase(name_it->second.collection_id);
  }

  auto id_it = by_id_.find(metadata.collection_id);
  if (id_it != by_id_.end() && id_it->second.collection_name != metadata.collection_name) {
    by_name_.erase(id_it->second.collection_name);
  }

  // Insert or update
  by_name_[metadata.collection_name] = metadata;
  by_id_[metadata.collection_id] = metadata;
}

size_t CollectionMetadataCache::Size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return by_name_.size();
}

}  // namespace service
}  // namespace gvdb