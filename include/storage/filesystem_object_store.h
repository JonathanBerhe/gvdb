// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_FILESYSTEM_OBJECT_STORE_H_
#define GVDB_STORAGE_FILESYSTEM_OBJECT_STORE_H_

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "core/status.h"
#include "storage/object_store.h"

namespace gvdb {
namespace storage {

// ============================================================================
// FilesystemObjectStore - IObjectStore backed by the local filesystem
// ============================================================================
//
// Stores each object as a file under a root directory. Keys are treated as
// slash-delimited relative paths beneath the root. This exists so that
// backup/restore and any other IObjectStore consumer can work without an
// S3/MinIO dependency — useful for single-node deployments, local tests,
// and initial bring-up.
//
// Semantics (matches the IObjectStore contract):
//   - PutObject overwrites atomically (temp file + rename) so concurrent
//     readers never see a partial write.
//   - DeleteObject is idempotent.
//   - ListObjects walks the tree and returns keys sorted lexicographically,
//     using forward slashes regardless of host OS.
//   - Keys must not be empty, must not be absolute, and must not contain
//     ".." segments that escape the root. Such keys are rejected with
//     InvalidArgumentError.
//
// Thread-safety: safe for concurrent calls. Concurrency is provided by the
// filesystem itself: writes use unique temp paths + atomic rename, so no
// store-level mutex is required.
class FilesystemObjectStore : public IObjectStore {
 public:
  // Construct a store rooted at `root`. Creates the directory (and any
  // missing parents) if it does not exist. Returns InvalidArgumentError if
  // `root` is empty, or if `root` already exists and is not a directory.
  [[nodiscard]] static core::StatusOr<std::unique_ptr<FilesystemObjectStore>>
      Create(const std::string& root);

  ~FilesystemObjectStore() override = default;

  FilesystemObjectStore(const FilesystemObjectStore&) = delete;
  FilesystemObjectStore& operator=(const FilesystemObjectStore&) = delete;
  FilesystemObjectStore(FilesystemObjectStore&&) = delete;
  FilesystemObjectStore& operator=(FilesystemObjectStore&&) = delete;

  core::Status PutObject(const std::string& key,
                         const std::string& data) override;

  core::StatusOr<std::string> GetObject(const std::string& key) override;

  core::Status DeleteObject(const std::string& key) override;

  core::StatusOr<std::vector<std::string>> ListObjects(
      const std::string& prefix) override;

  core::StatusOr<bool> ObjectExists(const std::string& key) override;

  core::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override;

  core::Status GetObjectToFile(const std::string& key,
                               const std::string& local_file_path) override;

  // Root directory this store writes to. Useful for tests and operational
  // tooling that needs to introspect the on-disk layout.
  [[nodiscard]] const std::filesystem::path& Root() const noexcept { return root_; }

 private:
  explicit FilesystemObjectStore(std::filesystem::path root);

  // Resolve a key to its on-disk path, rejecting keys that escape the root
  // or are otherwise malformed.
  [[nodiscard]] core::StatusOr<std::filesystem::path> KeyToPath(
      const std::string& key) const;

  std::filesystem::path root_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_FILESYSTEM_OBJECT_STORE_H_
