#include "network/collection_metadata_cache.h"

#include <doctest/doctest.h>
#include <thread>
#include <vector>

using namespace gvdb;
using namespace gvdb::network;

class CollectionMetadataCacheTest {
 public:
  CollectionMetadataCacheTest() {
    cache_ = std::make_unique<CollectionMetadataCache>();
  }

  std::unique_ptr<CollectionMetadataCache> cache_;
};

// Test basic put and get by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "PutAndGetByName") {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::HNSW);

  cache_->Put(metadata);

  auto result = cache_->GetByName("test_collection");
  REQUIRE(result.ok());
  CHECK_EQ(result->collection_id, static_cast<core::CollectionId>(1));
  CHECK_EQ(result->collection_name, "test_collection");
  CHECK_EQ(result->dimension, 128);
  CHECK_EQ(result->metric_type, core::MetricType::L2);
  CHECK_EQ(result->index_type, core::IndexType::HNSW);
}

// Test basic put and get by ID
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "PutAndGetById") {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(42),
      "my_collection",
      256,
      core::MetricType::INNER_PRODUCT,
      core::IndexType::IVF_FLAT);

  cache_->Put(metadata);

  auto result = cache_->GetById(static_cast<core::CollectionId>(42));
  REQUIRE(result.ok());
  CHECK_EQ(result->collection_id, static_cast<core::CollectionId>(42));
  CHECK_EQ(result->collection_name, "my_collection");
  CHECK_EQ(result->dimension, 256);
  CHECK_EQ(result->metric_type, core::MetricType::INNER_PRODUCT);
  CHECK_EQ(result->index_type, core::IndexType::IVF_FLAT);
}

// Test get non-existent collection by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "GetNonExistentByName") {
  auto result = cache_->GetByName("nonexistent");
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

// Test get non-existent collection by ID
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "GetNonExistentById") {
  auto result = cache_->GetById(static_cast<core::CollectionId>(999));
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

// Test update existing collection
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "UpdateCollection") {
  CollectionMetadata metadata1(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata1);

  // Update with different parameters
  CollectionMetadata metadata2(
      static_cast<core::CollectionId>(1),
      "test_collection",
      256,  // Changed dimension
      core::MetricType::COSINE,  // Changed metric
      core::IndexType::HNSW);  // Changed index

  cache_->Put(metadata2);

  auto result = cache_->GetByName("test_collection");
  REQUIRE(result.ok());
  CHECK_EQ(result->dimension, 256);
  CHECK_EQ(result->metric_type, core::MetricType::COSINE);
  CHECK_EQ(result->index_type, core::IndexType::HNSW);
}

// Test remove by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "RemoveByName") {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  CHECK(cache_->Contains("test_collection"));

  cache_->Remove("test_collection");
  CHECK_FALSE(cache_->Contains("test_collection"));

  // Should not be findable by ID either
  auto result = cache_->GetById(static_cast<core::CollectionId>(1));
  CHECK_FALSE(result.ok());
}

// Test remove by ID
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "RemoveById") {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  CHECK(cache_->Contains(static_cast<core::CollectionId>(1)));

  cache_->Remove(static_cast<core::CollectionId>(1));
  CHECK_FALSE(cache_->Contains(static_cast<core::CollectionId>(1)));

  // Should not be findable by name either
  auto result = cache_->GetByName("test_collection");
  CHECK_FALSE(result.ok());
}

// Test contains by name
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ContainsByName") {
  CHECK_FALSE(cache_->Contains("test_collection"));

  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  CHECK(cache_->Contains("test_collection"));
  CHECK_FALSE(cache_->Contains("other_collection"));
}

// Test contains by ID
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ContainsById") {
  CHECK_FALSE(cache_->Contains(static_cast<core::CollectionId>(1)));

  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  CHECK(cache_->Contains(static_cast<core::CollectionId>(1)));
  CHECK_FALSE(cache_->Contains(static_cast<core::CollectionId>(2)));
}

// Test clear
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "Clear") {
  // Add multiple collections
  for (int i = 1; i <= 5; ++i) {
    CollectionMetadata metadata(
        static_cast<core::CollectionId>(i),
        "collection_" + std::to_string(i),
        128,
        core::MetricType::L2,
        core::IndexType::FLAT);
    cache_->Put(metadata);
  }

  CHECK_EQ(cache_->Size(), 5);

  cache_->Clear();
  CHECK_EQ(cache_->Size(), 0);

  for (int i = 1; i <= 5; ++i) {
    CHECK_FALSE(cache_->Contains(static_cast<core::CollectionId>(i)));
  }
}

// Test size
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "Size") {
  CHECK_EQ(cache_->Size(), 0);

  CollectionMetadata metadata1(
      static_cast<core::CollectionId>(1),
      "collection1",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);
  cache_->Put(metadata1);
  CHECK_EQ(cache_->Size(), 1);

  CollectionMetadata metadata2(
      static_cast<core::CollectionId>(2),
      "collection2",
      256,
      core::MetricType::COSINE,
      core::IndexType::HNSW);
  cache_->Put(metadata2);
  CHECK_EQ(cache_->Size(), 2);

  cache_->Remove("collection1");
  CHECK_EQ(cache_->Size(), 1);

  cache_->Clear();
  CHECK_EQ(cache_->Size(), 0);
}

// Test multiple collections
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "MultipleCollections") {
  std::vector<CollectionMetadata> collections;
  for (int i = 1; i <= 10; ++i) {
    collections.emplace_back(
        static_cast<core::CollectionId>(i),
        "collection_" + std::to_string(i),
        128 * i,
        core::MetricType::L2,
        core::IndexType::FLAT);
    cache_->Put(collections.back());
  }

  CHECK_EQ(cache_->Size(), 10);

  // Verify all collections can be retrieved by name
  for (int i = 1; i <= 10; ++i) {
    auto result = cache_->GetByName("collection_" + std::to_string(i));
    REQUIRE(result.ok());
    CHECK_EQ(result->dimension, 128 * i);
  }

  // Verify all collections can be retrieved by ID
  for (int i = 1; i <= 10; ++i) {
    auto result = cache_->GetById(static_cast<core::CollectionId>(i));
    REQUIRE(result.ok());
    CHECK_EQ(result->collection_name, "collection_" + std::to_string(i));
  }
}

// Test thread safety: concurrent reads
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ConcurrentReads") {
  // Populate cache
  for (int i = 1; i <= 100; ++i) {
    CollectionMetadata metadata(
        static_cast<core::CollectionId>(i),
        "collection_" + std::to_string(i),
        128,
        core::MetricType::L2,
        core::IndexType::FLAT);
    cache_->Put(metadata);
  }

  // Concurrent readers
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

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
  for (int t = 0; t < 10; ++t) {
    threads.emplace_back([this, t]() {
      for (int i = 0; i < 10; ++i) {
        int id = t * 10 + i + 1;
        CollectionMetadata metadata(
            static_cast<core::CollectionId>(id),
            "collection_" + std::to_string(id),
            128,
            core::MetricType::L2,
            core::IndexType::FLAT);
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
    auto result = cache_->GetById(static_cast<core::CollectionId>(i));
    CHECK(result.ok());
  }
}

// Test thread safety: concurrent reads and writes
TEST_CASE_FIXTURE(CollectionMetadataCacheTest, "ConcurrentReadsAndWrites") {
  std::vector<std::thread> threads;
  std::atomic<bool> stop{false};

  // Writers
  for (int t = 0; t < 5; ++t) {
    threads.emplace_back([this, &stop, t]() {
      int count = 0;
      while (!stop.load() && count < 50) {
        int id = t * 50 + count + 1;
        CollectionMetadata metadata(
            static_cast<core::CollectionId>(id),
            "collection_" + std::to_string(id),
            128,
            core::MetricType::L2,
            core::IndexType::FLAT);
        cache_->Put(metadata);
        count++;
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Readers
  for (int t = 0; t < 5; ++t) {
    threads.emplace_back([this, &stop]() {
      while (!stop.load()) {
        for (int i = 1; i <= 250; ++i) {
          cache_->GetByName("collection_" + std::to_string(i));
        }
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Let them run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  // Should have all collections written by writers
  CHECK_EQ(cache_->Size(), 250);
}
