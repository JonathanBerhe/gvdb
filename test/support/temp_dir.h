// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Temporary folders that no other test, test case or test process uses, so
// that test binaries can run in parallel.

#pragma once

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <system_error>

namespace gvdb {
namespace test {

// Returns a new, empty folder under the system temp folder. The name holds
// the prefix, the process id, a per-process counter and a random number, so
// two processes or two calls never get the same folder. The caller removes it.
inline std::filesystem::path MakeUniqueTempDir(const std::string& prefix) {
  static std::atomic<std::uint64_t> counter{0};
  static const std::uint64_t salt = std::random_device{}();
  const auto n = counter.fetch_add(1, std::memory_order_relaxed);
  auto dir = std::filesystem::temp_directory_path() /
             (prefix + "_" + std::to_string(::getpid()) + "_" + std::to_string(n) +
              "_" + std::to_string(salt % 1000000));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

// A unique temp folder that is removed when the object goes out of scope.
class ScopedTempDir {
 public:
  explicit ScopedTempDir(const std::string& prefix = "gvdb_test")
      : path_(MakeUniqueTempDir(prefix)) {}

  ~ScopedTempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;

  const std::filesystem::path& path() const { return path_; }
  std::string str() const { return path_.string(); }

  // A path inside the folder. The function does not create it.
  std::filesystem::path operator/(const std::string& name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

}  // namespace test
}  // namespace gvdb
