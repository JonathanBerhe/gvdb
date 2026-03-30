// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/collection_metadata_cache.h"
#include "absl/strings/str_format.h"

namespace gvdb {
namespace network {

absl::StatusOr<CollectionMetadata> CollectionMetadataCache::GetByName(
    const std::string& collection_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto it = by_name_.find(collection_name);
  if (it == by_name_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("Collection not found in cache: %s", collection_name));
  }

  return it->second;
}

absl::StatusOr<CollectionMetadata> CollectionMetadataCache::GetById(
    core::CollectionId collection_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto it = by_id_.find(collection_id);
  if (it == by_id_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("Collection not found in cache: %d",
                        core::ToUInt32(collection_id)));
  }

  return it->second;
}

void CollectionMetadataCache::Put(const CollectionMetadata& metadata) {
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

void CollectionMetadataCache::Remove(const std::string& collection_name) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  auto it = by_name_.find(collection_name);
  if (it != by_name_.end()) {
    by_id_.erase(it->second.collection_id);
    by_name_.erase(it);
  }
}

void CollectionMetadataCache::Remove(core::CollectionId collection_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  auto it = by_id_.find(collection_id);
  if (it != by_id_.end()) {
    by_name_.erase(it->second.collection_name);
    by_id_.erase(it);
  }
}

bool CollectionMetadataCache::Contains(const std::string& collection_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return by_name_.find(collection_name) != by_name_.end();
}

bool CollectionMetadataCache::Contains(core::CollectionId collection_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return by_id_.find(collection_id) != by_id_.end();
}

void CollectionMetadataCache::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  by_name_.clear();
  by_id_.clear();
}

size_t CollectionMetadataCache::Size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return by_name_.size();
}

}  // namespace network
}  // namespace gvdb