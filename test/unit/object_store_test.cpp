// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/object_store.h"

#include <doctest/doctest.h>

#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

namespace gvdb {
namespace storage {
namespace {

const std::string kTestDir = "/tmp/gvdb_object_store_test";

struct ObjectStoreTest {
  InMemoryObjectStore store;

  ObjectStoreTest() {
    std::filesystem::create_directories(kTestDir);
  }

  ~ObjectStoreTest() {
    std::filesystem::remove_all(kTestDir);
  }
};

TEST_CASE_FIXTURE(ObjectStoreTest, "PutAndGetObject") {
  REQUIRE(store.PutObject("key1", "value1").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "value1");
}

TEST_CASE_FIXTURE(ObjectStoreTest, "GetNonexistentObject") {
  auto result = store.GetObject("missing");
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE_FIXTURE(ObjectStoreTest, "DeleteObject") {
  REQUIRE(store.PutObject("key1", "value1").ok());
  REQUIRE(store.DeleteObject("key1").ok());
  CHECK_FALSE(store.GetObject("key1").ok());
}

TEST_CASE_FIXTURE(ObjectStoreTest, "DeleteNonexistentIsIdempotent") {
  CHECK(store.DeleteObject("missing").ok());
}

TEST_CASE_FIXTURE(ObjectStoreTest, "ListObjects") {
  REQUIRE(store.PutObject("prefix/a", "1").ok());
  REQUIRE(store.PutObject("prefix/b", "2").ok());
  REQUIRE(store.PutObject("other/c", "3").ok());

  auto result = store.ListObjects("prefix/");
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 2);
  CHECK_EQ((*result)[0], "prefix/a");
  CHECK_EQ((*result)[1], "prefix/b");
}

TEST_CASE_FIXTURE(ObjectStoreTest, "ListObjectsEmpty") {
  auto result = store.ListObjects("nothing/");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

TEST_CASE_FIXTURE(ObjectStoreTest, "ObjectExists") {
  REQUIRE(store.PutObject("key1", "data").ok());
  auto exists = store.ObjectExists("key1");
  REQUIRE(exists.ok());
  CHECK(*exists);

  auto missing = store.ObjectExists("missing");
  REQUIRE(missing.ok());
  CHECK_FALSE(*missing);
}

TEST_CASE_FIXTURE(ObjectStoreTest, "OverwriteObject") {
  REQUIRE(store.PutObject("key1", "v1").ok());
  REQUIRE(store.PutObject("key1", "v2").ok());
  auto result = store.GetObject("key1");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "v2");
}

TEST_CASE_FIXTURE(ObjectStoreTest, "PutEmptyObject") {
  REQUIRE(store.PutObject("empty", "").ok());
  auto result = store.GetObject("empty");
  REQUIRE(result.ok());
  CHECK(result->empty());
}

TEST_CASE_FIXTURE(ObjectStoreTest, "PutObjectFromFile") {
  auto path = kTestDir + "/test_upload.bin";
  {
    std::ofstream f(path, std::ios::binary);
    f << "file contents here";
  }
  REQUIRE(store.PutObjectFromFile("from_file", path).ok());
  auto result = store.GetObject("from_file");
  REQUIRE(result.ok());
  CHECK_EQ(*result, "file contents here");
}

TEST_CASE_FIXTURE(ObjectStoreTest, "PutObjectFromFileMissing") {
  auto status = store.PutObjectFromFile("key", "/nonexistent/path.bin");
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(ObjectStoreTest, "GetObjectToFile") {
  REQUIRE(store.PutObject("key1", "download me").ok());
  auto path = kTestDir + "/test_download.bin";
  REQUIRE(store.GetObjectToFile("key1", path).ok());

  std::ifstream f(path);
  std::string contents((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
  CHECK_EQ(contents, "download me");
}

TEST_CASE_FIXTURE(ObjectStoreTest, "GetObjectToFileMissing") {
  auto status = store.GetObjectToFile("missing", kTestDir + "/out.bin");
  CHECK_FALSE(status.ok());
}

TEST_CASE_FIXTURE(ObjectStoreTest, "LargeObject") {
  std::string large(1024 * 1024, 'X');  // 1 MB
  REQUIRE(store.PutObject("large", large).ok());
  auto result = store.GetObject("large");
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), large.size());
  CHECK_EQ(*result, large);
}

TEST_CASE_FIXTURE(ObjectStoreTest, "ObjectCount") {
  CHECK_EQ(store.ObjectCount(), 0);
  REQUIRE(store.PutObject("a", "1").ok());
  REQUIRE(store.PutObject("b", "2").ok());
  CHECK_EQ(store.ObjectCount(), 2);
  store.Clear();
  CHECK_EQ(store.ObjectCount(), 0);
}

TEST_CASE_FIXTURE(ObjectStoreTest, "ConcurrentPuts") {
  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 100;
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        auto key = "t" + std::to_string(t) + "_" + std::to_string(i);
        REQUIRE(store.PutObject(key, "data").ok());
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(store.ObjectCount(), kThreads * kOpsPerThread);
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
