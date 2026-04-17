// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/filesystem_object_store.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

namespace {

namespace stdfs = std::filesystem;

// Read the entire file at `path` into a string using binary mode. Returns
// NotFoundError if the path does not exist, InternalError on read failure.
core::StatusOr<std::string> ReadEntireFile(const stdfs::path& path) {
  std::error_code ec;
  if (!stdfs::exists(path, ec) || ec) {
    return core::NotFoundError(
        absl::StrCat("FilesystemObjectStore: object not found: ", path.string()));
  }

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return core::InternalError(
        absl::StrCat("FilesystemObjectStore: failed to open for read: ",
                     path.string()));
  }

  in.seekg(0, std::ios::end);
  const std::streampos end_pos = in.tellg();
  if (end_pos < 0) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: tellg failed: ", path.string()));
  }
  in.seekg(0, std::ios::beg);

  std::string buffer(static_cast<std::size_t>(end_pos), '\0');
  if (end_pos > 0) {
    in.read(buffer.data(), end_pos);
    if (in.bad()) {
      return core::InternalError(
          absl::StrCat("FilesystemObjectStore: read failed: ", path.string()));
    }
  }
  return buffer;
}

// Reserved subdirectory under the store root that holds in-flight temp
// files during atomic writes. Kept as a separate namespace (rather than
// naming temp files with a suffix) so user keys can freely contain any
// substring, including historical sentinels like ".tmp.".
//
// Invariants:
//   - User keys starting with this prefix are rejected by KeyToPath.
//   - ListObjects skips any entry whose relative path begins with it.
//   - `rename` from this subdir to a destination anywhere under root is
//     atomic because both paths live on the same filesystem.
constexpr const char* kReservedTempPrefix = ".gvdb-tmp/";

// Generate a unique path under {root}/.gvdb-tmp/ for atomic rename. Uniqueness
// comes from a process-wide monotonic counter so no two in-flight writes can
// collide, even when targeting the same destination.
stdfs::path UniqueTempPath(const stdfs::path& root) {
  static std::atomic<std::uint64_t> counter{0};
  const std::uint64_t n = counter.fetch_add(1, std::memory_order_relaxed);
  return root / ".gvdb-tmp" / std::to_string(n);
}

// Write `data` to `dest` atomically: write to a temp file inside the
// reserved .gvdb-tmp/ subdir, then rename into place. Creates parent
// directories on demand.
core::Status AtomicWrite(const stdfs::path& root, const stdfs::path& dest,
                         const std::string& data) {
  std::error_code ec;
  stdfs::create_directories(dest.parent_path(), ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to create parent dir for ",
        dest.string(), ": ", ec.message()));
  }
  stdfs::create_directories(root / ".gvdb-tmp", ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to create reserved temp dir: ",
        ec.message()));
  }

  const stdfs::path tmp = UniqueTempPath(root);
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return core::InternalError(absl::StrCat(
          "FilesystemObjectStore: failed to open temp file: ", tmp.string()));
    }
    if (!data.empty()) {
      out.write(data.data(), static_cast<std::streamsize>(data.size()));
      if (!out.good()) {
        stdfs::remove(tmp, ec);  // best-effort cleanup; ignore ec
        return core::InternalError(absl::StrCat(
            "FilesystemObjectStore: write failed: ", tmp.string()));
      }
    }
  }  // ofstream closes here, flushing the OS buffer.

  stdfs::rename(tmp, dest, ec);
  if (ec) {
    stdfs::remove(tmp, ec);  // best-effort cleanup
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: rename failed: ", dest.string()));
  }
  return core::OkStatus();
}

}  // namespace

// ============================================================================
// Construction
// ============================================================================

core::StatusOr<std::unique_ptr<FilesystemObjectStore>>
FilesystemObjectStore::Create(const std::string& root) {
  if (root.empty()) {
    return core::InvalidArgumentError(
        "FilesystemObjectStore: root path must not be empty");
  }

  const stdfs::path root_path(root);
  std::error_code ec;

  if (stdfs::exists(root_path, ec)) {
    if (ec) {
      return core::InternalError(absl::StrCat(
          "FilesystemObjectStore: stat failed on root: ", ec.message()));
    }
    if (!stdfs::is_directory(root_path, ec)) {
      return core::InvalidArgumentError(absl::StrCat(
          "FilesystemObjectStore: root exists but is not a directory: ",
          root));
    }
  } else {
    stdfs::create_directories(root_path, ec);
    if (ec) {
      return core::InternalError(absl::StrCat(
          "FilesystemObjectStore: failed to create root: ", ec.message()));
    }
  }

  // Use a fresh error_code: the one above may have been set and cleared by
  // the exists / create_directories branches, and we must not silently carry
  // a stale status into the absolute() call below.
  std::error_code abs_ec;
  auto abs_root = stdfs::absolute(root_path, abs_ec);
  if (abs_ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to resolve root to absolute path: ",
        root, ": ", abs_ec.message()));
  }

  // Cannot use std::make_unique here because the constructor is private.
  return std::unique_ptr<FilesystemObjectStore>(
      new FilesystemObjectStore(std::move(abs_root)));
}

FilesystemObjectStore::FilesystemObjectStore(stdfs::path root)
    : root_(std::move(root)) {}

// ============================================================================
// Key validation
// ============================================================================

core::StatusOr<stdfs::path> FilesystemObjectStore::KeyToPath(
    const std::string& key) const {
  if (key.empty()) {
    return core::InvalidArgumentError(
        "FilesystemObjectStore: key must not be empty");
  }
  // Absolute keys would escape the root; reject outright.
  if (key.front() == '/' || key.front() == '\\') {
    return core::InvalidArgumentError(absl::StrCat(
        "FilesystemObjectStore: key must be relative: ", key));
  }
  // Reserve the .gvdb-tmp/ subtree for in-flight atomic-write temp files.
  // Users cannot place objects there or they would be invisible to
  // ListObjects and race with ongoing writes.
  if (key.compare(0, std::strlen(kReservedTempPrefix),
                   kReservedTempPrefix) == 0) {
    return core::InvalidArgumentError(absl::StrCat(
        "FilesystemObjectStore: key uses reserved prefix '",
        kReservedTempPrefix, "': ", key));
  }

  const stdfs::path candidate = root_ / stdfs::path(key);

  // Lexical normalisation collapses "a/../b" to "b" without touching the
  // filesystem. After normalisation, the result must still live under root.
  const stdfs::path normalised = candidate.lexically_normal();
  const std::string root_str = root_.lexically_normal().generic_string();
  const std::string norm_str = normalised.generic_string();

  // Require a trailing slash on the root comparison so "/rootfoo" does not
  // match "/root" as a prefix.
  const std::string root_prefix =
      root_str.empty() || root_str.back() == '/' ? root_str : root_str + "/";
  if (norm_str != root_str && norm_str.compare(0, root_prefix.size(),
                                                root_prefix) != 0) {
    return core::InvalidArgumentError(absl::StrCat(
        "FilesystemObjectStore: key escapes root: ", key));
  }

  return normalised;
}

// ============================================================================
// Object API
// ============================================================================

core::Status FilesystemObjectStore::PutObject(const std::string& key,
                                              const std::string& data) {
  auto path = KeyToPath(key);
  if (!path.ok()) {
    return path.status();
  }
  return AtomicWrite(root_, *path, data);
}

core::StatusOr<std::string> FilesystemObjectStore::GetObject(
    const std::string& key) {
  auto path = KeyToPath(key);
  if (!path.ok()) {
    return path.status();
  }
  return ReadEntireFile(*path);
}

core::Status FilesystemObjectStore::DeleteObject(const std::string& key) {
  auto path = KeyToPath(key);
  if (!path.ok()) {
    return path.status();
  }

  std::error_code ec;
  stdfs::remove(*path, ec);  // false when missing; ec set only on real error
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: delete failed: ", path->string(), ": ",
        ec.message()));
  }
  return core::OkStatus();
}

core::StatusOr<std::vector<std::string>> FilesystemObjectStore::ListObjects(
    const std::string& prefix) {
  std::error_code ec;
  if (!stdfs::exists(root_, ec)) {
    return std::vector<std::string>{};
  }

  std::vector<std::string> result;
  for (const auto& entry : stdfs::recursive_directory_iterator(root_, ec)) {
    if (ec) {
      return core::InternalError(absl::StrCat(
          "FilesystemObjectStore: list walk failed: ", ec.message()));
    }
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const stdfs::path rel = stdfs::relative(entry.path(), root_, ec);
    if (ec) {
      continue;
    }
    // Use generic_string so keys always use '/' regardless of host OS.
    std::string key = rel.generic_string();
    // Skip the reserved subtree that holds in-flight atomic-write temps.
    if (key.compare(0, std::strlen(kReservedTempPrefix),
                     kReservedTempPrefix) == 0) {
      continue;
    }
    if (key.compare(0, prefix.size(), prefix) == 0) {
      result.push_back(std::move(key));
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

core::StatusOr<bool> FilesystemObjectStore::ObjectExists(
    const std::string& key) {
  auto path = KeyToPath(key);
  if (!path.ok()) {
    return path.status();
  }
  std::error_code ec;
  return stdfs::is_regular_file(*path, ec);
}

core::Status FilesystemObjectStore::PutObjectFromFile(
    const std::string& key, const std::string& local_file_path) {
  std::error_code ec;
  if (!stdfs::exists(local_file_path, ec) || ec) {
    return core::NotFoundError(absl::StrCat(
        "FilesystemObjectStore: local file not found: ", local_file_path));
  }

  auto dest = KeyToPath(key);
  if (!dest.ok()) {
    return dest.status();
  }

  stdfs::create_directories(dest->parent_path(), ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to create parent dir for ",
        dest->string(), ": ", ec.message()));
  }
  stdfs::create_directories(root_ / ".gvdb-tmp", ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to create reserved temp dir: ",
        ec.message()));
  }

  const stdfs::path tmp = UniqueTempPath(root_);
  stdfs::copy_file(local_file_path, tmp,
                   stdfs::copy_options::overwrite_existing, ec);
  if (ec) {
    stdfs::remove(tmp, ec);
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: copy failed: ", local_file_path, " -> ",
        tmp.string()));
  }

  stdfs::rename(tmp, *dest, ec);
  if (ec) {
    stdfs::remove(tmp, ec);
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: rename failed: ", dest->string()));
  }
  return core::OkStatus();
}

core::Status FilesystemObjectStore::GetObjectToFile(
    const std::string& key, const std::string& local_file_path) {
  auto src = KeyToPath(key);
  if (!src.ok()) {
    return src.status();
  }

  std::error_code ec;
  if (!stdfs::exists(*src, ec) || ec) {
    return core::NotFoundError(absl::StrCat(
        "FilesystemObjectStore: object not found: ", key));
  }

  const stdfs::path dest(local_file_path);
  stdfs::create_directories(dest.parent_path(), ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: failed to create parent dir for ",
        dest.string(), ": ", ec.message()));
  }

  stdfs::copy_file(*src, dest, stdfs::copy_options::overwrite_existing, ec);
  if (ec) {
    return core::InternalError(absl::StrCat(
        "FilesystemObjectStore: copy failed: ", src->string(), " -> ",
        dest.string()));
  }
  return core::OkStatus();
}

}  // namespace storage
}  // namespace gvdb
