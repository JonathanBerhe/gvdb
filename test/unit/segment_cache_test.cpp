// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/segment_cache.h"

#include <doctest/doctest.h>

#include <filesystem>
#include <fstream>
#include <thread>

namespace gvdb {
namespace storage {
namespace {

const std::string kTestDir = "/tmp/gvdb_segment_cache_test";

struct CacheTest {
  CacheTest() {
    std::filesystem::remove_all(kTestDir);
  }
  ~CacheTest() {
    std::filesystem::remove_all(kTestDir);
  }

  static core::SegmentId MakeId(uint32_t v) {
    return static_cast<core::SegmentId>(v);
  }
};

TEST_CASE_FIXTURE(CacheTest, "EmptyCache") {
  SegmentCache cache(kTestDir, 1024);
  CHECK_EQ(cache.GetCachedSize(), 0);
  CHECK_EQ(cache.GetCachedCount(), 0);
  CHECK_FALSE(cache.HasSegment(MakeId(1)));
  CHECK(cache.GetSegmentPath(MakeId(1)).empty());
}

TEST_CASE_FIXTURE(CacheTest, "RegisterAndCheck") {
  SegmentCache cache(kTestDir, 1024);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  CHECK(cache.HasSegment(MakeId(1)));
  CHECK_FALSE(cache.GetSegmentPath(MakeId(1)).empty());
  CHECK_EQ(cache.GetCachedSize(), 100);
  CHECK_EQ(cache.GetCachedCount(), 1);
}

TEST_CASE_FIXTURE(CacheTest, "GetSegmentPath") {
  SegmentCache cache(kTestDir, 1024);
  REQUIRE(cache.RegisterSegment(MakeId(5), 50).ok());
  auto path = cache.GetSegmentPath(MakeId(5));
  CHECK_NE(path.find("segment_5"), std::string::npos);
}

TEST_CASE_FIXTURE(CacheTest, "LRUEviction") {
  SegmentCache cache(kTestDir, 300);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(2), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(3), 100).ok());
  CHECK_EQ(cache.GetCachedSize(), 300);

  // Evict 150 bytes — should remove segment 1 (oldest), then 2
  auto evicted = cache.Evict(150);
  REQUIRE(evicted.ok());
  CHECK_EQ(evicted->size(), 2);
  CHECK_FALSE(cache.HasSegment(MakeId(1)));
  CHECK_FALSE(cache.HasSegment(MakeId(2)));
  CHECK(cache.HasSegment(MakeId(3)));
  CHECK_EQ(cache.GetCachedSize(), 100);
}

TEST_CASE_FIXTURE(CacheTest, "TouchUpdatesLRU") {
  SegmentCache cache(kTestDir, 300);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(2), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(3), 100).ok());

  // Touch segment 1 (oldest) to make it most recent
  cache.Touch(MakeId(1));

  // Evict 100 bytes — should remove segment 2 (now oldest)
  auto evicted = cache.Evict(100);
  REQUIRE(evicted.ok());
  CHECK_EQ(evicted->size(), 1);
  CHECK(cache.HasSegment(MakeId(1)));   // touched, not evicted
  CHECK_FALSE(cache.HasSegment(MakeId(2)));  // evicted
  CHECK(cache.HasSegment(MakeId(3)));
}

TEST_CASE_FIXTURE(CacheTest, "EvictAll") {
  SegmentCache cache(kTestDir, 200);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(2), 100).ok());

  auto evicted = cache.Evict(500);  // more than total
  REQUIRE(evicted.ok());
  CHECK_EQ(evicted->size(), 2);
  CHECK_EQ(cache.GetCachedSize(), 0);
  CHECK_EQ(cache.GetCachedCount(), 0);
}

TEST_CASE_FIXTURE(CacheTest, "RegisterDuplicate") {
  SegmentCache cache(kTestDir, 1024);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  REQUIRE(cache.RegisterSegment(MakeId(1), 200).ok());  // update size
  CHECK_EQ(cache.GetCachedSize(), 200);
  CHECK_EQ(cache.GetCachedCount(), 1);
}

TEST_CASE_FIXTURE(CacheTest, "MultipleRegistrations") {
  SegmentCache cache(kTestDir, 10000);
  for (uint32_t i = 0; i < 50; ++i) {
    REQUIRE(cache.RegisterSegment(MakeId(i), 100).ok());
  }
  CHECK_EQ(cache.GetCachedCount(), 50);
  CHECK_EQ(cache.GetCachedSize(), 5000);
}

TEST_CASE_FIXTURE(CacheTest, "MaxCacheZero") {
  SegmentCache cache(kTestDir, 0);
  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());
  // Cache allows registration but eviction should always trigger
  CHECK_EQ(cache.GetCachedSize(), 100);
}

TEST_CASE_FIXTURE(CacheTest, "ConcurrentAccess") {
  SegmentCache cache(kTestDir, 100000);
  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 50;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        auto id = MakeId(t * kOpsPerThread + i);
        REQUIRE(cache.RegisterSegment(id, 10).ok());
        cache.Touch(id);
        CHECK(cache.HasSegment(id));
      }
    });
  }
  for (auto& t : threads) t.join();
  CHECK_EQ(cache.GetCachedCount(), kThreads * kOpsPerThread);
}

TEST_CASE_FIXTURE(CacheTest, "EvictDeletesFiles") {
  SegmentCache cache(kTestDir, 200);

  // Create fake segment directory
  auto seg_dir = kTestDir + "/segment_1";
  std::filesystem::create_directories(seg_dir);
  std::ofstream(seg_dir + "/test.txt") << "data";
  CHECK(std::filesystem::exists(seg_dir));

  REQUIRE(cache.RegisterSegment(MakeId(1), 100).ok());

  auto evicted = cache.Evict(100);
  REQUIRE(evicted.ok());
  CHECK_EQ(evicted->size(), 1);
  CHECK_FALSE(std::filesystem::exists(seg_dir));
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
