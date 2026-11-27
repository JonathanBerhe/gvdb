// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/vectordb/collection_metadata_cache.h"

#include <doctest/doctest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace {

using namespace gvdb;
using namespace gvdb::service;

class CollectionMetadataCacheTest {
 public:
  CollectionMetadataCacheTest() { cache_ = std::make_unique<CollectionMetadataCache>(); }

  std::unique_ptr<CollectionMetadataCache> cache_;
};

// Test basic put and get by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "PutAndGetByName") {
  meta::CollectionMetadata metadata(static_cast<core::CollectionId>(1), "test_collection", 128,
                                    core::MetricType::L2, core::IndexType::HNSW);

  cache_->Put(metadata);

  auto result = cache_->GetByName("test_collection");
  REQUIRE(result.ok());
  CHECK_EQ(result->collection_id, static_cast<core::CollectionId>(1));
  CHECK_EQ(result->collection_name, "test_collection");
  CHECK_EQ(result->dimension, 128);
  CHECK_EQ(result->metric_type, core::MetricType::L2);
  CHECK_EQ(result->index_type, core::IndexType::HNSW);
}

// Test get non-existent collection by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "GetNonExistentByName") {
  auto result = cache_->GetByName("nonexistent");
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

// Test update existing collection
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "UpdateCollection") {
  meta::CollectionMetadata metadata1(static_cast<core::CollectionId>(1), "test_collection", 128,
                                     core::MetricType::L2, core::IndexType::FLAT);

  cache_->Put(metadata1);

  // Update with different parameters
  meta::CollectionMetadata metadata2(static_cast<core::CollectionId>(1), "test_collection",
                                     256,                       // Changed dimension
                                     core::MetricType::COSINE,  // Changed metric
                                     core::IndexType::HNSW);    // Changed index

  cache_->Put(metadata2);

  auto result = cache_->GetByName("test_collection");
  REQUIRE(result.ok());
  CHECK_EQ(result->dimension, 256);
  CHECK_EQ(result->metric_type, core::MetricType::COSINE);
  CHECK_EQ(result->index_type, core::IndexType::HNSW);
}

// Test size
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "Size") {
  CHECK_EQ(cache_->Size(), 0);

  meta::CollectionMetadata metadata1(static_cast<core::CollectionId>(1), "collection1", 128,
                                     core::MetricType::L2, core::IndexType::FLAT);
  cache_->Put(metadata1);
  CHECK_EQ(cache_->Size(), 1);

  meta::CollectionMetadata metadata2(static_cast<core::CollectionId>(2), "collection2", 256,
                                     core::MetricType::COSINE, core::IndexType::HNSW);
  cache_->Put(metadata2);
  CHECK_EQ(cache_->Size(), 2);
}

// Test multiple collections
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "MultipleCollections") {
  std::vector<meta::CollectionMetadata> collections;
  for (int i = 1; i <= 10; ++i) {
    collections.emplace_back(static_cast<core::CollectionId>(i), "collection_" + std::to_string(i),
                             128 * i, core::MetricType::L2, core::IndexType::FLAT);
    cache_->Put(collections.back());
  }

  CHECK_EQ(cache_->Size(), 10);

  // Verify all collections can be retrieved by name
  for (int i = 1; i <= 10; ++i) {
    auto result = cache_->GetByName("collection_" + std::to_string(i));
    REQUIRE(result.ok());
    CHECK_EQ(result->collection_id, static_cast<core::CollectionId>(i));
    CHECK_EQ(result->dimension, 128 * i);
  }
}

// Test thread safety: concurrent reads
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ConcurrentReads") {
  // Populate cache
  for (int i = 1; i <= 100; ++i) {
    meta::CollectionMetadata metadata(static_cast<core::CollectionId>(i),
                                      "collection_" + std::to_string(i), 128, core::MetricType::L2,
                                      core::IndexType::FLAT);
    cache_->Put(metadata);
  }

  // Concurrent readers
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  threads.reserve(10);
  for (int t = 0; t < 10; ++t) {
    threads.emplace_back([this, &success_count]() {
      for (int i = 1; i <= 100; ++i) {
        auto result = cache_->GetByName("collection_" + std::to_string(i));
        if (result.ok()) {
          success_count++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All reads should succeed
  CHECK_EQ(success_count.load(), 1000);  // 10 threads * 100 collections
}

// Test thread safety: concurrent writes
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ConcurrentWrites") {
  std::vector<std::thread> threads;

  // Concurrent writers
  threads.reserve(10);
  for (int t = 0; t < 10; ++t) {
    threads.emplace_back([this, t]() {
      for (int i = 0; i < 10; ++i) {
        int id = t * 10 + i + 1;
        meta::CollectionMetadata metadata(static_cast<core::CollectionId>(id),
                                          "collection_" + std::to_string(id), 128,
                                          core::MetricType::L2, core::IndexType::FLAT);
        cache_->Put(metadata);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All collections should be present
  CHECK_EQ(cache_->Size(), 100);

  // Verify all can be retrieved
  for (int i = 1; i <= 100; ++i) {
    auto result = cache_->GetByName("collection_" + std::to_string(i));
    CHECK(result.ok());
  }
}

// Test thread safety: concurrent reads and writes
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ConcurrentReadsAndWrites") {
  std::vector<std::thread> writers;
  std::vector<std::thread> readers;
  std::atomic<bool> writers_done{false};

  // Writers: each one writes exactly 50 collections. The short pause between
  // writes only spreads them out so that reads interleave with them.
  writers.reserve(5);
  for (int t = 0; t < 5; ++t) {
    writers.emplace_back([this, t]() {
      for (int count = 0; count < 50; ++count) {
        int id = t * 50 + count + 1;
        meta::CollectionMetadata metadata(static_cast<core::CollectionId>(id),
                                          "collection_" + std::to_string(id), 128,
                                          core::MetricType::L2, core::IndexType::FLAT);
        cache_->Put(metadata);
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Readers: read until every writer is done, so reads overlap the whole
  // write phase. Each reader makes at least one full pass.
  readers.reserve(5);
  for (int t = 0; t < 5; ++t) {
    readers.emplace_back([this, &writers_done]() {
      do {
        for (int i = 1; i <= 250; ++i) {
          // Only exercises concurrent reads; the result can be a miss.
          (void)cache_->GetByName("collection_" + std::to_string(i));
        }
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      } while (!writers_done.load());
    });
  }

  for (auto& writer : writers) {
    writer.join();
  }
  writers_done.store(true);
  for (auto& reader : readers) {
    reader.join();
  }

  // Should have all collections written by writers
  CHECK_EQ(cache_->Size(), 250);
}

}  // namespace
