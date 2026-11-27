// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/status.h"

namespace gvdb {
namespace storage {

// ============================================================================
// IObjectStore - Abstract interface for object storage backends
// ============================================================================
// Implementations: S3ObjectStore (AWS S3 / MinIO), future GCSObjectStore, etc.
// All keys are slash-delimited paths (e.g., "gvdb/collections/1/segments/5/vectors.bin").
// Thread-safety: Implementations must be thread-safe for concurrent calls.
class IObjectStore {
 public:
  virtual ~IObjectStore() = default;

  // Put an object from a memory buffer
  [[nodiscard]] virtual absl::Status PutObject(const std::string& key, const std::string& data) = 0;

  // Get an object into a memory buffer
  [[nodiscard]] virtual absl::StatusOr<std::string> GetObject(const std::string& key) = 0;

  // Delete an object (idempotent — deleting a missing key is OK)
  [[nodiscard]] virtual absl::Status DeleteObject(const std::string& key) = 0;

  // List objects matching a key prefix
  [[nodiscard]] virtual absl::StatusOr<std::vector<std::string>> ListObjects(
      const std::string& prefix) = 0;

  // Check if an object exists
  [[nodiscard]] virtual absl::StatusOr<bool> ObjectExists(const std::string& key) = 0;

  // Put an object by streaming from a local file (avoids double-buffering)
  [[nodiscard]] virtual absl::Status PutObjectFromFile(const std::string& key,
                                                       const std::string& local_file_path) = 0;

  // Get an object by streaming to a local file (avoids double-buffering)
  [[nodiscard]] virtual absl::Status GetObjectToFile(const std::string& key,
                                                     const std::string& local_file_path) = 0;
};

// ============================================================================
// InMemoryObjectStore - Test double for IObjectStore
// ============================================================================
// Stores objects in an in-memory map. Thread-safe. Used in unit tests.
class InMemoryObjectStore : public IObjectStore {
 public:
  InMemoryObjectStore() = default;

  absl::Status PutObject(const std::string& key, const std::string& data) override;

  absl::StatusOr<std::string> GetObject(const std::string& key) override;

  absl::Status DeleteObject(const std::string& key) override;

  absl::StatusOr<std::vector<std::string>> ListObjects(const std::string& prefix) override;

  absl::StatusOr<bool> ObjectExists(const std::string& key) override;

  absl::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override;

  absl::Status GetObjectToFile(const std::string& key, const std::string& local_file_path) override;

  // Test helpers
  size_t ObjectCount() const;
  void Clear();

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::string> objects_;
};

}  // namespace storage
}  // namespace gvdb
