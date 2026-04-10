// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_S3_OBJECT_STORE_H_
#define GVDB_STORAGE_S3_OBJECT_STORE_H_

#ifdef GVDB_HAS_S3

#include <memory>
#include <string>

#include "core/status.h"
#include "storage/object_store.h"

// Forward declarations for AWS SDK types
namespace Aws {
namespace S3 {
class S3Client;
}  // namespace S3
}  // namespace Aws

namespace gvdb {
namespace storage {

// Configuration for creating an S3ObjectStore
struct S3Config {
  std::string endpoint;
  std::string access_key;
  std::string secret_key;
  std::string bucket;
  std::string region;
  bool use_ssl = true;
  bool path_style = false;  // true for MinIO
};

// ============================================================================
// S3ObjectStore - IObjectStore implementation using AWS S3 / MinIO
// ============================================================================
// Uses the AWS SDK for C++ (s3 component only).
// Thread-safe — the underlying S3Client is thread-safe for concurrent requests.
class S3ObjectStore : public IObjectStore {
 public:
  // Factory method — handles AWS SDK initialization
  [[nodiscard]] static core::StatusOr<std::unique_ptr<S3ObjectStore>> Create(
      const S3Config& config);

  ~S3ObjectStore() override;

  core::Status PutObject(
      const std::string& key, const std::string& data) override;

  core::StatusOr<std::string> GetObject(const std::string& key) override;

  core::Status DeleteObject(const std::string& key) override;

  core::StatusOr<std::vector<std::string>> ListObjects(
      const std::string& prefix) override;

  core::StatusOr<bool> ObjectExists(const std::string& key) override;

  core::Status PutObjectFromFile(
      const std::string& key,
      const std::string& local_file_path) override;

  core::Status GetObjectToFile(
      const std::string& key,
      const std::string& local_file_path) override;

 private:
  S3ObjectStore(std::shared_ptr<Aws::S3::S3Client> client,
                const std::string& bucket);

  std::shared_ptr<Aws::S3::S3Client> client_;
  std::string bucket_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_HAS_S3
#endif  // GVDB_STORAGE_S3_OBJECT_STORE_H_
