// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/object_store.h"

#include <fstream>
#include <shared_mutex>
#include <sstream>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

// ============================================================================
// InMemoryObjectStore implementation
// ============================================================================

core::Status InMemoryObjectStore::PutObject(
    const std::string& key, const std::string& data) {
  std::unique_lock lock(mutex_);
  objects_[key] = data;
  return core::OkStatus();
}

core::StatusOr<std::string> InMemoryObjectStore::GetObject(
    const std::string& key) {
  std::shared_lock lock(mutex_);
  auto it = objects_.find(key);
  if (it == objects_.end()) {
    return core::NotFoundError(absl::StrCat("Object not found: ", key));
  }
  return it->second;
}

core::Status InMemoryObjectStore::DeleteObject(const std::string& key) {
  std::unique_lock lock(mutex_);
  objects_.erase(key);
  return core::OkStatus();
}

core::StatusOr<std::vector<std::string>> InMemoryObjectStore::ListObjects(
    const std::string& prefix) {
  std::shared_lock lock(mutex_);
  std::vector<std::string> result;
  for (const auto& [key, _] : objects_) {
    if (key.substr(0, prefix.size()) == prefix) {
      result.push_back(key);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

core::StatusOr<bool> InMemoryObjectStore::ObjectExists(
    const std::string& key) {
  std::shared_lock lock(mutex_);
  return objects_.count(key) > 0;
}

core::Status InMemoryObjectStore::PutObjectFromFile(
    const std::string& key, const std::string& local_file_path) {
  std::ifstream file(local_file_path, std::ios::binary);
  if (!file.is_open()) {
    return core::NotFoundError(
        absl::StrCat("Local file not found: ", local_file_path));
  }
  std::ostringstream ss;
  ss << file.rdbuf();
  return PutObject(key, ss.str());
}

core::Status InMemoryObjectStore::GetObjectToFile(
    const std::string& key, const std::string& local_file_path) {
  auto result = GetObject(key);
  if (!result.ok()) {
    return result.status();
  }
  std::ofstream file(local_file_path, std::ios::binary);
  if (!file.is_open()) {
    return core::InternalError(
        absl::StrCat("Failed to open local file: ", local_file_path));
  }
  file.write(result->data(), result->size());
  return core::OkStatus();
}

size_t InMemoryObjectStore::ObjectCount() const {
  std::shared_lock lock(mutex_);
  return objects_.size();
}

void InMemoryObjectStore::Clear() {
  std::unique_lock lock(mutex_);
  objects_.clear();
}

}  // namespace storage
}  // namespace gvdb
