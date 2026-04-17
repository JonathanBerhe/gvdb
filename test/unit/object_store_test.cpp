// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Contract tests for IObjectStore implementations. Every test in the
// "ObjectStore contract" group runs against each concrete backend via
// doctest's TEST_CASE_TEMPLATE — so behavioural drift between the in-memory
// test double, the filesystem backend, and (if enabled) the S3 backend gets
// caught here.

#include <doctest/doctest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "storage/filesystem_object_store.h"
#include "storage/object_store.h"

namespace gvdb {
namespace storage {
namespace {

// ============================================================================
// Fixtures
// ============================================================================
//
// Each fixture owns the lifetime of a concrete IObjectStore and a unique
// scratch directory for any file-system side-channel a test needs (e.g., a
// source file for PutObjectFromFile or a destination for GetObjectToFile).
// Both fixtures expose the store through a common Get() method, which is
// what the templated tests depend on.

// Generate a per-test unique temp directory. Using pid + atomic counter is
// enough: even if two test processes run in parallel they get different
// pids, and within a process the counter disambiguates.
std::filesystem::path MakeUniqueTempDir(const char* prefix) {
  static std::atomic<std::uint64_t> seq{0};
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  auto dir = std::filesystem::temp_directory_path() /
             (std::string(prefix) + "_" +
              std::to_string(static_cast<std::uint64_t>(
                  std::hash<std::thread::id>{}(std::this_thread::get_id()))) +
              "_" + std::to_string(n));
  std::filesystem::create_directories(dir);
  return dir;
}

struct InMemoryObjectStoreFixture {
  InMemoryObjectStoreFixture()
      : tmp_dir(MakeUniqueTempDir("gvdb_inmem_objstore")),
        store(std::make_unique<InMemoryObjectStore>()) {}

  ~InMemoryObjectStoreFixture() {
    std::error_code ec;
    std::filesystem::remove_all(tmp_dir, ec);
  }

  IObjectStore& Get() { return *store; }

  std::filesystem::path tmp_dir;
  std::unique_ptr<InMemoryObjectStore> store;
};

struct FilesystemObjectStoreFixture {
  FilesystemObjectStoreFixture()
      : tmp_dir(MakeUniqueTempDir("gvdb_fs_objstore")) {
    auto created = FilesystemObjectStore::Create((tmp_dir / "store").string());
    REQUIRE(created.ok());
    store = std::move(*created);
  }

  ~FilesystemObjectStoreFixture() {
    std::error_code ec;
    std::filesystem::remove_all(tmp_dir, ec);
  }

  IObjectStore& Get() { return *store; }

  std::filesystem::path tmp_dir;
  std::unique_ptr<FilesystemObjectStore> store;
};

// Alias the list of fixtures so every contract test names it once.
#define OBJECT_STORE_FIXTURES \
  InMemoryObjectStoreFixture, FilesystemObjectStoreFixture

// ============================================================================
// Contract — basic CRUD
// ============================================================================

TEST_CASE_TEMPLATE("ObjectStore contract: Put then Get returns the value",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "value1").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "value1");
}

TEST_CASE_TEMPLATE("ObjectStore contract: Get on missing key returns NotFound",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto result = f.Get().GetObject("missing");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE_TEMPLATE("ObjectStore contract: Delete removes the object",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "value1").ok());
  REQUIRE(store.DeleteObject("key1").ok());
  CHECK_FALSE(store.GetObject("key1").ok());
}

TEST_CASE_TEMPLATE("ObjectStore contract: Delete on missing key is idempotent",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  CHECK(f.Get().DeleteObject("missing").ok());
}

TEST_CASE_TEMPLATE("ObjectStore contract: overwrite replaces prior value",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "v1").ok());
  REQUIRE(store.PutObject("key1", "v2").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "v2");
}

TEST_CASE_TEMPLATE("ObjectStore contract: empty payload is valid",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("empty", "").ok());
  auto result = store.GetObject("empty");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

// ============================================================================
// Contract — listing and existence
// ============================================================================

TEST_CASE_TEMPLATE("ObjectStore contract: ListObjects filters by prefix and sorts",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("prefix/a", "1").ok());
  REQUIRE(store.PutObject("prefix/b", "2").ok());
  REQUIRE(store.PutObject("other/c", "3").ok());

  auto result = store.ListObjects("prefix/");
  REQUIRE(result.ok());
  REQUIRE_EQ(result->size(), 2);
  CHECK_EQ((*result)[0], "prefix/a");
  CHECK_EQ((*result)[1], "prefix/b");
}

TEST_CASE_TEMPLATE("ObjectStore contract: ListObjects returns empty on no match",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto result = f.Get().ListObjects("does-not-exist/");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

TEST_CASE_TEMPLATE("ObjectStore contract: ObjectExists reports true / false",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "data").ok());

  auto exists = store.ObjectExists("key1");
  REQUIRE(exists.ok());
  CHECK(*exists);

  auto missing = store.ObjectExists("missing");
  REQUIRE(missing.ok());
  CHECK_FALSE(*missing);
}

// ============================================================================
// Contract — file-streaming variants
// ============================================================================

TEST_CASE_TEMPLATE("ObjectStore contract: PutObjectFromFile uploads file bytes",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  const auto src_path = f.tmp_dir / "upload.bin";
  {
    std::ofstream out(src_path, std::ios::binary);
    out << "file contents here";
  }
  REQUIRE(store.PutObjectFromFile("from_file", src_path.string()).ok());

  auto result = store.GetObject("from_file");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "file contents here");
}

TEST_CASE_TEMPLATE(
    "ObjectStore contract: PutObjectFromFile on missing file returns NotFound",
    Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto status = f.Get().PutObjectFromFile(
      "key", (f.tmp_dir / "does-not-exist.bin").string());
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_TEMPLATE("ObjectStore contract: GetObjectToFile materialises bytes",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "download me").ok());
  const auto dest = f.tmp_dir / "download.bin";
  REQUIRE(store.GetObjectToFile("key1", dest.string()).ok());

  std::ifstream in(dest, std::ios::binary);
  std::string contents((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
  CHECK_EQ(contents, "download me");
}

TEST_CASE_TEMPLATE(
    "ObjectStore contract: GetObjectToFile on missing key returns NotFound",
    Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto status = f.Get().GetObjectToFile(
      "missing", (f.tmp_dir / "out.bin").string());
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

// ============================================================================
// Contract — size and concurrency
// ============================================================================

TEST_CASE_TEMPLATE("ObjectStore contract: large (1 MiB) object round-trips",
                   Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  const std::string large(1024 * 1024, 'X');
  REQUIRE(store.PutObject("large", large).ok());
  auto result = store.GetObject("large");
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), large.size());
  CHECK_EQ(*result, large);
}

TEST_CASE_TEMPLATE(
    "ObjectStore contract: concurrent Puts from many threads all persist",
    Fixture, OBJECT_STORE_FIXTURES) {
  Fixture f;
  auto& store = f.Get();

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 100;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string key =
            "t" + std::to_string(t) + "/" + std::to_string(i);
        REQUIRE(store.PutObject(key, "data").ok());
      }
    });
  }
  for (auto& th : threads) th.join();

  // Every key should be retrievable. This catches races where atomic-rename
  // or in-memory map updates drop writes.
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      const std::string key =
          "t" + std::to_string(t) + "/" + std::to_string(i);
      auto got = store.GetObject(key);
      REQUIRE_MESSAGE(got.ok(), "missing after concurrent put: ", key);
      CHECK_EQ(*got, "data");
    }
  }
}

TEST_CASE_TEMPLATE(
    "ObjectStore contract: concurrent overwrites of a single key converge "
    "to one of the written values",
    Fixture, OBJECT_STORE_FIXTURES) {
  // Stresses the atomic-rename invariant of the filesystem backend and the
  // mutex invariant of the in-memory backend. We don't care *which* writer
  // wins — only that the final value is a full payload that one of them
  // actually wrote (no partial file, no empty blob, no mangled bytes).
  Fixture f;
  auto& store = f.Get();

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 50;
  const std::string kKey = "shared-key";

  // Enumerate the full set of payloads up-front so the final read can be
  // validated against a membership check.
  std::vector<std::string> all_payloads;
  all_payloads.reserve(kThreads * kOpsPerThread);
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      all_payloads.push_back("t" + std::to_string(t) + "-" +
                             std::to_string(i));
    }
  }

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, &kKey, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string payload = "t" + std::to_string(t) + "-" +
                                    std::to_string(i);
        REQUIRE(store.PutObject(kKey, payload).ok());
      }
    });
  }
  for (auto& th : threads) th.join();

  auto final_value = store.GetObject(kKey);
  REQUIRE(final_value.ok());
  const auto match = std::find(all_payloads.begin(), all_payloads.end(),
                                *final_value);
  CHECK_MESSAGE(match != all_payloads.end(),
                "final value not one of the written payloads: '",
                *final_value, "'");
}

// ============================================================================
// InMemoryObjectStore-specific tests (test helpers not on the interface)
// ============================================================================

TEST_CASE("InMemoryObjectStore: ObjectCount and Clear track size") {
  InMemoryObjectStore store;
  CHECK_EQ(store.ObjectCount(), 0);
  REQUIRE(store.PutObject("a", "1").ok());
  REQUIRE(store.PutObject("b", "2").ok());
  CHECK_EQ(store.ObjectCount(), 2);
  store.Clear();
  CHECK_EQ(store.ObjectCount(), 0);
}

// ============================================================================
// FilesystemObjectStore-specific tests (path safety, on-disk layout)
// ============================================================================

TEST_CASE("FilesystemObjectStore: Create fails when root path is empty") {
  auto created = FilesystemObjectStore::Create("");
  CHECK_FALSE(created.ok());
  CHECK_EQ(created.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("FilesystemObjectStore: Create fails when root exists as a file") {
  const auto tmp = MakeUniqueTempDir("gvdb_fs_objstore_root_is_file");
  const auto root_as_file = tmp / "not-a-dir";
  {
    std::ofstream f(root_as_file);
    f << "hello";
  }

  auto created = FilesystemObjectStore::Create(root_as_file.string());
  CHECK_FALSE(created.ok());
  CHECK_EQ(created.status().code(), absl::StatusCode::kInvalidArgument);

  std::error_code ec;
  std::filesystem::remove_all(tmp, ec);
}

TEST_CASE("FilesystemObjectStore: Create creates the root directory if absent") {
  const auto tmp = MakeUniqueTempDir("gvdb_fs_objstore_new_root");
  const auto root = tmp / "new" / "deep" / "root";
  CHECK_FALSE(std::filesystem::exists(root));

  auto created = FilesystemObjectStore::Create(root.string());
  REQUIRE(created.ok());
  CHECK(std::filesystem::is_directory(root));

  std::error_code ec;
  std::filesystem::remove_all(tmp, ec);
}

TEST_CASE("FilesystemObjectStore: rejects empty and absolute keys") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject("", "x").code(), absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject("/abs", "x").code(),
           absl::StatusCode::kInvalidArgument);
}

TEST_CASE("FilesystemObjectStore: rejects keys that escape the root via ..") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject("../escape", "x").code(),
           absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject("a/../../escape", "x").code(),
           absl::StatusCode::kInvalidArgument);
}

TEST_CASE(
    "FilesystemObjectStore: ListObjects hides the reserved .gvdb-tmp subtree") {
  // Simulate a crashed write that left a file behind under the reserved
  // subtree. ListObjects must not expose it.
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("real.bin", "data").ok());

  const auto reserved_dir = f.store->Root() / ".gvdb-tmp";
  std::filesystem::create_directories(reserved_dir);
  const auto stray = reserved_dir / "42";
  {
    std::ofstream out(stray);
    out << "leftover";
  }

  auto result = store.ListObjects("");
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 1);
  CHECK_EQ((*result)[0], "real.bin");
}

TEST_CASE("FilesystemObjectStore: PutObject creates parent directories") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("deep/nested/path/obj.bin", "hello").ok());
  CHECK(std::filesystem::exists(f.store->Root() / "deep" / "nested" /
                                 "path" / "obj.bin"));
}

TEST_CASE(
    "FilesystemObjectStore: user keys containing '.tmp.' are first-class") {
  // Regression guard for an earlier design that filtered ".tmp." from
  // listings as a sentinel: user-controlled keys like archive.tmp.2024 or
  // configs/.tmp.json were silently hidden. With the reserved-subdir design
  // these keys are indistinguishable from any other object.
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("configs/.tmp.json", "{}").ok());
  REQUIRE(store.PutObject("archive.tmp.2024", "payload").ok());

  auto listed = store.ListObjects("");
  REQUIRE(listed.ok());
  CHECK(std::find(listed->begin(), listed->end(), "configs/.tmp.json") !=
        listed->end());
  CHECK(std::find(listed->begin(), listed->end(), "archive.tmp.2024") !=
        listed->end());

  auto got = store.GetObject("configs/.tmp.json");
  REQUIRE(got.ok());
  CHECK_EQ(*got, "{}");
}

TEST_CASE(
    "FilesystemObjectStore: keys under the reserved .gvdb-tmp/ prefix "
    "are rejected") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject(".gvdb-tmp/x", "x").code(),
           absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject(".gvdb-tmp/deep/nested/obj", "x").code(),
           absl::StatusCode::kInvalidArgument);

  // Sanity: a key that merely *contains* '.gvdb-tmp' without being at the
  // start of the path is fine. Only the leading prefix is reserved.
  CHECK(store.PutObject("logs/.gvdb-tmp-alike", "x").ok());
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
