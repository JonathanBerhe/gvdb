// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Contract tests for IObjectStore implementations. The cases are defined once
// here, and each test binary runs them against its backends with
// GVDB_OBJECT_STORE_CONTRACT(FixtureA, FixtureB, ...), so behavioral drift
// between the in-memory test double, the filesystem backend and the cloud
// backends gets caught.
//
// A fixture is default-constructible and provides Get() (the store under
// test) and tmp_dir (a scratch directory for the file-streaming cases).
// Include this header in exactly one translation unit per test binary, and
// use the macro inside namespace gvdb::storage::contract.

#pragma once

#include <doctest/doctest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/status.h"
#include "storage/objstore/object_store.h"

namespace gvdb::storage::contract {

// Wraps a store that several tests share (a cloud bucket) and puts every key
// under a prefix that is unique to this instance. The contract cases then see
// an empty store, as they do with a fresh local backend. The destructor
// deletes every object under the prefix.
class PrefixedObjectStore final : public IObjectStore {
 public:
  PrefixedObjectStore(std::unique_ptr<IObjectStore> inner, std::string prefix)
      : inner_(std::move(inner)), prefix_(std::move(prefix)) {}

  ~PrefixedObjectStore() override {
    auto keys = inner_->ListObjects(prefix_);
    if (!keys.ok()) return;
    for (const auto& key : *keys) {
      inner_->DeleteObject(key).IgnoreError();
    }
  }

  // A prefix that no other test or test process uses.
  static std::string UniquePrefix() {
    static std::atomic<std::uint64_t> seq{0};
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return "gvdb-contract/" + std::to_string(::getpid()) + "-" + std::to_string(now) + "-" +
           std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "/";
  }

  absl::Status PutObject(const std::string& key, const std::string& data) override {
    return inner_->PutObject(prefix_ + key, data);
  }
  absl::StatusOr<std::string> GetObject(const std::string& key) override {
    return inner_->GetObject(prefix_ + key);
  }
  absl::Status DeleteObject(const std::string& key) override {
    return inner_->DeleteObject(prefix_ + key);
  }
  absl::StatusOr<std::vector<std::string>> ListObjects(const std::string& prefix) override {
    auto keys = inner_->ListObjects(prefix_ + prefix);
    if (!keys.ok()) return keys.status();
    std::vector<std::string> stripped;
    stripped.reserve(keys->size());
    for (const auto& key : *keys) stripped.push_back(key.substr(prefix_.size()));
    return stripped;
  }
  absl::StatusOr<bool> ObjectExists(const std::string& key) override {
    return inner_->ObjectExists(prefix_ + key);
  }
  absl::Status PutObjectFromFile(const std::string& key,
                                 const std::string& local_file_path) override {
    return inner_->PutObjectFromFile(prefix_ + key, local_file_path);
  }
  absl::Status GetObjectToFile(const std::string& key,
                               const std::string& local_file_path) override {
    return inner_->GetObjectToFile(prefix_ + key, local_file_path);
  }

 private:
  std::unique_ptr<IObjectStore> inner_;
  std::string prefix_;
};

// ============================================================================
// Contract: basic CRUD
// ============================================================================

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: Put then Get returns the value", Fixture,
                          put_then_get) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "value1").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "value1");
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: Get on missing key returns NotFound", Fixture,
                          get_missing) {
  Fixture f;
  auto result = f.Get().GetObject("missing");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: Delete removes the object", Fixture,
                          delete_removes) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "value1").ok());
  REQUIRE(store.DeleteObject("key1").ok());
  CHECK_FALSE(store.GetObject("key1").ok());
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: Delete on missing key is idempotent", Fixture,
                          delete_missing) {
  Fixture f;
  CHECK(f.Get().DeleteObject("missing").ok());
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: overwrite replaces prior value", Fixture,
                          overwrite) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "v1").ok());
  REQUIRE(store.PutObject("key1", "v2").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "v2");
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: empty payload is valid", Fixture, empty_payload) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("empty", "").ok());
  auto result = store.GetObject("empty");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

// ============================================================================
// Contract: listing and existence
// ============================================================================

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: ListObjects filters by prefix and sorts", Fixture,
                          list_by_prefix) {
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

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: ListObjects returns empty on no match", Fixture,
                          list_no_match) {
  Fixture f;
  auto result = f.Get().ListObjects("does-not-exist/");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: ObjectExists reports true / false", Fixture,
                          object_exists) {
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
// Contract: file-streaming variants
// ============================================================================

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: PutObjectFromFile uploads file bytes", Fixture,
                          put_from_file) {
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

TEST_CASE_TEMPLATE_DEFINE(
    "ObjectStore contract: PutObjectFromFile on missing file returns NotFound", Fixture,
    put_from_missing_file) {
  Fixture f;
  auto status = f.Get().PutObjectFromFile("key", (f.tmp_dir / "does-not-exist.bin").string());
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: GetObjectToFile materializes bytes", Fixture,
                          get_to_file) {
  Fixture f;
  auto& store = f.Get();

  REQUIRE(store.PutObject("key1", "download me").ok());
  const auto dest = f.tmp_dir / "download.bin";
  REQUIRE(store.GetObjectToFile("key1", dest.string()).ok());

  std::ifstream in(dest, std::ios::binary);
  std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  CHECK_EQ(contents, "download me");
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: GetObjectToFile on missing key returns NotFound",
                          Fixture, get_missing_to_file) {
  Fixture f;
  auto status = f.Get().GetObjectToFile("missing", (f.tmp_dir / "out.bin").string());
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

// ============================================================================
// Contract: size and concurrency
// ============================================================================

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: large (1 MiB) object round-trips", Fixture,
                          large_object) {
  Fixture f;
  auto& store = f.Get();

  const std::string large(size_t{1024} * 1024, 'X');
  REQUIRE(store.PutObject("large", large).ok());
  auto result = store.GetObject("large");
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), large.size());
  CHECK_EQ(*result, large);
}

TEST_CASE_TEMPLATE_DEFINE("ObjectStore contract: concurrent Puts from many threads all persist",
                          Fixture, concurrent_puts) {
  Fixture f;
  auto& store = f.Get();

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 100;

  // Workers only count failures: a doctest assertion that fails on a worker
  // thread would end the whole binary, not just this case.
  std::atomic<int> failed_puts{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, &failed_puts, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string key = "t" + std::to_string(t) + "/" + std::to_string(i);
        if (!store.PutObject(key, "data").ok()) failed_puts.fetch_add(1);
      }
    });
  }
  for (auto& th : threads) th.join();
  REQUIRE_EQ(failed_puts.load(), 0);

  // Every key must be retrievable. This catches races where atomic-rename or
  // in-memory map updates drop writes.
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      const std::string key = "t" + std::to_string(t) + "/" + std::to_string(i);
      auto got = store.GetObject(key);
      REQUIRE_MESSAGE(got.ok(), "missing after concurrent put: ", key);
      CHECK_EQ(*got, "data");
    }
  }
}

TEST_CASE_TEMPLATE_DEFINE(
    "ObjectStore contract: concurrent overwrites of a single key converge "
    "to one of the written values",
    Fixture, concurrent_overwrites) {
  // Stresses the atomic-rename invariant of the filesystem backend and the
  // mutex invariant of the in-memory backend. Which writer wins does not
  // matter, only that the final value is a full payload that one of them
  // wrote (no partial file, no empty blob, no mangled bytes).
  Fixture f;
  auto& store = f.Get();

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 50;
  const std::string kKey = "shared-key";

  // Enumerate the full set of payloads up front so the final read can be
  // validated against a membership check.
  std::vector<std::string> all_payloads;
  all_payloads.reserve(static_cast<size_t>(kThreads) * kOpsPerThread);
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      all_payloads.push_back("t" + std::to_string(t) + "-" + std::to_string(i));
    }
  }

  // Workers only count failures (see the previous case).
  std::atomic<int> failed_puts{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, &kKey, &failed_puts, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string payload = "t" + std::to_string(t) + "-" + std::to_string(i);
        if (!store.PutObject(kKey, payload).ok()) failed_puts.fetch_add(1);
      }
    });
  }
  for (auto& th : threads) th.join();
  REQUIRE_EQ(failed_puts.load(), 0);

  auto final_value = store.GetObject(kKey);
  REQUIRE(final_value.ok());
  const auto match = std::find(all_payloads.begin(), all_payloads.end(), *final_value);
  CHECK_MESSAGE(match != all_payloads.end(), "final value not one of the written payloads: '",
                *final_value, "'");
}

}  // namespace gvdb::storage::contract

// Runs every contract case above against each fixture type in the argument
// list. Use it once per test binary, inside namespace gvdb::storage::contract.
//
// The macro expands on one source line, and doctest names the variables
// that register a case after __LINE__ (on Clang 22 and later). So every
// case is invoked in a namespace of its own, and the names cannot collide.
// The static_assert at the end takes the semicolon after the macro call.
#define GVDB_OBJECT_STORE_CONTRACT_INVOKE(id, ...) \
  namespace invoke_##id {                          \
    TEST_CASE_TEMPLATE_INVOKE(id, __VA_ARGS__);    \
  }

#define GVDB_OBJECT_STORE_CONTRACT(...)                                 \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(put_then_get, __VA_ARGS__)          \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(get_missing, __VA_ARGS__)           \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(delete_removes, __VA_ARGS__)        \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(delete_missing, __VA_ARGS__)        \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(overwrite, __VA_ARGS__)             \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(empty_payload, __VA_ARGS__)         \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(list_by_prefix, __VA_ARGS__)        \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(list_no_match, __VA_ARGS__)         \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(object_exists, __VA_ARGS__)         \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(put_from_file, __VA_ARGS__)         \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(put_from_missing_file, __VA_ARGS__) \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(get_to_file, __VA_ARGS__)           \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(get_missing_to_file, __VA_ARGS__)   \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(large_object, __VA_ARGS__)          \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(concurrent_puts, __VA_ARGS__)       \
  GVDB_OBJECT_STORE_CONTRACT_INVOKE(concurrent_overwrites, __VA_ARGS__) \
  static_assert(true, "object store contract")
