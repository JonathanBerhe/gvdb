// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Object store tests for the local backends: the shared contract
// (object_store_contract.h) against InMemoryObjectStore and
// FilesystemObjectStore, backend-specific cases, and CreateObjectStore.
// The S3 and GCS backends run the same contract in object_store_s3_test.cpp
// and object_store_gcs_test.cpp.

#include <doctest/doctest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "config/config.h"
#include "object_store_contract.h"
#include "storage/objstore/filesystem_object_store.h"
#include "storage/objstore/object_store.h"
#include "storage/objstore/object_store_factory.h"
#include "support/temp_dir.h"

namespace gvdb {
namespace storage {
namespace {

using test::ScopedTempDir;

// ============================================================================
// Fixtures
// ============================================================================
//
// Each fixture owns a concrete IObjectStore and a unique scratch directory
// for the file-streaming cases.

struct InMemoryObjectStoreFixture {
  InMemoryObjectStoreFixture()
      : tmp_dir("gvdb_inmem_objstore"), store(std::make_unique<InMemoryObjectStore>()) {}

  IObjectStore& Get() { return *store; }

  ScopedTempDir tmp_dir;
  std::unique_ptr<InMemoryObjectStore> store;
};

struct FilesystemObjectStoreFixture {
  FilesystemObjectStoreFixture() : tmp_dir("gvdb_fs_objstore") {
    auto created = FilesystemObjectStore::Create((tmp_dir / "store").string());
    REQUIRE(created.ok());
    store = std::move(*created);
  }

  IObjectStore& Get() { return *store; }

  ScopedTempDir tmp_dir;
  std::unique_ptr<FilesystemObjectStore> store;
};

}  // namespace

namespace contract {
GVDB_OBJECT_STORE_CONTRACT(InMemoryObjectStoreFixture, FilesystemObjectStoreFixture);
}  // namespace contract

namespace {

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
  const ScopedTempDir tmp("gvdb_fs_objstore_root_is_file");
  const auto root_as_file = tmp / "not-a-dir";
  {
    std::ofstream f(root_as_file);
    f << "hello";
  }

  auto created = FilesystemObjectStore::Create(root_as_file.string());
  CHECK_FALSE(created.ok());
  CHECK_EQ(created.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("FilesystemObjectStore: Create creates the root directory if absent") {
  const ScopedTempDir tmp("gvdb_fs_objstore_new_root");
  const auto root = tmp / "new" / "deep" / "root";
  CHECK_FALSE(std::filesystem::exists(root));

  auto created = FilesystemObjectStore::Create(root.string());
  REQUIRE(created.ok());
  CHECK(std::filesystem::is_directory(root));
}

TEST_CASE("FilesystemObjectStore: rejects empty and absolute keys") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject("", "x").code(), absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject("/abs", "x").code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("FilesystemObjectStore: rejects keys that escape the root via ..") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject("../escape", "x").code(), absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject("a/../../escape", "x").code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("FilesystemObjectStore: ListObjects hides the reserved .gvdb-tmp subtree") {
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
  CHECK(std::filesystem::exists(f.store->Root() / "deep" / "nested" / "path" / "obj.bin"));
}

TEST_CASE("FilesystemObjectStore: user keys containing '.tmp.' are first-class") {
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
  CHECK(std::find(listed->begin(), listed->end(), "configs/.tmp.json") != listed->end());
  CHECK(std::find(listed->begin(), listed->end(), "archive.tmp.2024") != listed->end());

  auto got = store.GetObject("configs/.tmp.json");
  REQUIRE(got.ok());
  CHECK_EQ(*got, "{}");
}

TEST_CASE(
    "FilesystemObjectStore: keys under the reserved .gvdb-tmp/ prefix "
    "are rejected") {
  FilesystemObjectStoreFixture f;
  auto& store = f.Get();

  CHECK_EQ(store.PutObject(".gvdb-tmp/x", "x").code(), absl::StatusCode::kInvalidArgument);
  CHECK_EQ(store.PutObject(".gvdb-tmp/deep/nested/obj", "x").code(),
           absl::StatusCode::kInvalidArgument);

  // Sanity: a key that merely *contains* '.gvdb-tmp' without being at the
  // start of the path is fine. Only the leading prefix is reserved.
  CHECK(store.PutObject("logs/.gvdb-tmp-alike", "x").ok());
}

// ============================================================================
// CreateObjectStore (backend selection from the storage config)
// ============================================================================

TEST_CASE("CreateObjectStore: filesystem type builds a store rooted at the bucket") {
  const ScopedTempDir tmp("gvdb_objstore_factory_fs");
  ObjectStoreConfig config;
  config.type = "filesystem";
  config.bucket = (tmp / "cold").string();

  auto store = CreateObjectStore(config);
  REQUIRE(store.ok());
  REQUIRE(*store != nullptr);
  REQUIRE((*store)->PutObject("k", "v").ok());
  CHECK(std::filesystem::exists(tmp / "cold" / "k"));
}

TEST_CASE("CreateObjectStore: filesystem type without a bucket is rejected") {
  ObjectStoreConfig config;
  config.type = "filesystem";

  auto store = CreateObjectStore(config);
  CHECK_FALSE(store.ok());
  CHECK_EQ(store.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE("CreateObjectStore: no type and no endpoint disables object storage") {
  ObjectStoreConfig config;

  auto store = CreateObjectStore(config);
  REQUIRE(store.ok());
  CHECK(*store == nullptr);
}

TEST_CASE("CreateObjectStore: s3 or minio type without an endpoint disables object storage") {
  for (const char* type : {"s3", "minio"}) {
    CAPTURE(type);
    ObjectStoreConfig config;
    config.type = type;
    config.bucket = "bucket";

    auto store = CreateObjectStore(config);
    REQUIRE(store.ok());
    CHECK(*store == nullptr);
  }
}

TEST_CASE("CreateObjectStore: an endpoint selects S3 when the binary has S3 support") {
  ObjectStoreConfig config;
  config.type = "minio";
  config.endpoint = "localhost:9000";
  config.bucket = "bucket";
  config.access_key = "key";
  config.secret_key = "secret";
  config.use_ssl = false;
  config.region = "us-east-1";

  // Creating the client does not contact the endpoint.
  auto store = CreateObjectStore(config);
  REQUIRE(store.ok());
#ifdef GVDB_HAS_S3
  CHECK(*store != nullptr);
#else
  CHECK(*store == nullptr);
#endif
}

TEST_CASE("CreateObjectStore: gcs type selects GCS when the binary has GCS support") {
  ObjectStoreConfig config;
  config.type = "gcs";
  config.bucket = "bucket";
  config.endpoint = "http://localhost:4443";

  // Creating the client does not contact the endpoint.
  auto store = CreateObjectStore(config);
  REQUIRE(store.ok());
#ifdef GVDB_HAS_GCS
  CHECK(*store != nullptr);
#else
  CHECK(*store == nullptr);
#endif
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
