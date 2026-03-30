#include "network/collection_metadata_cache.h"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace gvdb;
using namespace gvdb::network;

class CollectionMetadataCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cache_ = std::make_unique<CollectionMetadataCache>();
  }

  std::unique_ptr<CollectionMetadataCache> cache_;
};

// Test basic put and get by name
TEST_F(CollectionMetadataCacheTest, PutAndGetByName) {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::HNSW);

  cache_->Put(metadata);

  auto result = cache_->GetByName("test_collection");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->collection_id, static_cast<core::CollectionId>(1));
  EXPECT_EQ(result->collection_name, "test_collection");
  EXPECT_EQ(result->dimension, 128);
  EXPECT_EQ(result->metric_type, core::MetricType::L2);
  EXPECT_EQ(result->index_type, core::IndexType::HNSW);
}

// Test basic put and get by ID
TEST_F(CollectionMetadataCacheTest, PutAndGetById) {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(42),
      "my_collection",
      256,
      core::MetricType::INNER_PRODUCT,
      core::IndexType::IVF_FLAT);

  cache_->Put(metadata);

  auto result = cache_->GetById(static_cast<core::CollectionId>(42));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->collection_id, static_cast<core::CollectionId>(42));
  EXPECT_EQ(result->collection_name, "my_collection");
  EXPECT_EQ(result->dimension, 256);
  EXPECT_EQ(result->metric_type, core::MetricType::INNER_PRODUCT);
  EXPECT_EQ(result->index_type, core::IndexType::IVF_FLAT);
}

// Test get non-existent collection by name
TEST_F(CollectionMetadataCacheTest, GetNonExistentByName) {
  auto result = cache_->GetByName("nonexistent");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

// Test get non-existent collection by ID
TEST_F(CollectionMetadataCacheTest, GetNonExistentById) {
  auto result = cache_->GetById(static_cast<core::CollectionId>(999));
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

// Test update existing collection
TEST_F(CollectionMetadataCacheTest, UpdateCollection) {
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
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->dimension, 256);
  EXPECT_EQ(result->metric_type, core::MetricType::COSINE);
  EXPECT_EQ(result->index_type, core::IndexType::HNSW);
}

// Test remove by name
TEST_F(CollectionMetadataCacheTest, RemoveByName) {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  EXPECT_TRUE(cache_->Contains("test_collection"));

  cache_->Remove("test_collection");
  EXPECT_FALSE(cache_->Contains("test_collection"));

  // Should not be findable by ID either
  auto result = cache_->GetById(static_cast<core::CollectionId>(1));
  EXPECT_FALSE(result.ok());
}

// Test remove by ID
TEST_F(CollectionMetadataCacheTest, RemoveById) {
  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  EXPECT_TRUE(cache_->Contains(static_cast<core::CollectionId>(1)));

  cache_->Remove(static_cast<core::CollectionId>(1));
  EXPECT_FALSE(cache_->Contains(static_cast<core::CollectionId>(1)));

  // Should not be findable by name either
  auto result = cache_->GetByName("test_collection");
  EXPECT_FALSE(result.ok());
}

// Test contains by name
TEST_F(CollectionMetadataCacheTest, ContainsByName) {
  EXPECT_FALSE(cache_->Contains("test_collection"));

  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  EXPECT_TRUE(cache_->Contains("test_collection"));
  EXPECT_FALSE(cache_->Contains("other_collection"));
}

// Test contains by ID
TEST_F(CollectionMetadataCacheTest, ContainsById) {
  EXPECT_FALSE(cache_->Contains(static_cast<core::CollectionId>(1)));

  CollectionMetadata metadata(
      static_cast<core::CollectionId>(1),
      "test_collection",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);

  cache_->Put(metadata);
  EXPECT_TRUE(cache_->Contains(static_cast<core::CollectionId>(1)));
  EXPECT_FALSE(cache_->Contains(static_cast<core::CollectionId>(2)));
}

// Test clear
TEST_F(CollectionMetadataCacheTest, Clear) {
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

  EXPECT_EQ(cache_->Size(), 5);

  cache_->Clear();
  EXPECT_EQ(cache_->Size(), 0);

  for (int i = 1; i <= 5; ++i) {
    EXPECT_FALSE(cache_->Contains(static_cast<core::CollectionId>(i)));
  }
}

// Test size
TEST_F(CollectionMetadataCacheTest, Size) {
  EXPECT_EQ(cache_->Size(), 0);

  CollectionMetadata metadata1(
      static_cast<core::CollectionId>(1),
      "collection1",
      128,
      core::MetricType::L2,
      core::IndexType::FLAT);
  cache_->Put(metadata1);
  EXPECT_EQ(cache_->Size(), 1);

  CollectionMetadata metadata2(
      static_cast<core::CollectionId>(2),
      "collection2",
      256,
      core::MetricType::COSINE,
      core::IndexType::HNSW);
  cache_->Put(metadata2);
  EXPECT_EQ(cache_->Size(), 2);

  cache_->Remove("collection1");
  EXPECT_EQ(cache_->Size(), 1);

  cache_->Clear();
  EXPECT_EQ(cache_->Size(), 0);
}

// Test multiple collections
TEST_F(CollectionMetadataCacheTest, MultipleCollections) {
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

  EXPECT_EQ(cache_->Size(), 10);

  // Verify all collections can be retrieved by name
  for (int i = 1; i <= 10; ++i) {
    auto result = cache_->GetByName("collection_" + std::to_string(i));
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->dimension, 128 * i);
  }

  // Verify all collections can be retrieved by ID
  for (int i = 1; i <= 10; ++i) {
    auto result = cache_->GetById(static_cast<core::CollectionId>(i));
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->collection_name, "collection_" + std::to_string(i));
  }
}

// Test thread safety: concurrent reads
TEST_F(CollectionMetadataCacheTest, ConcurrentReads) {
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
  EXPECT_EQ(success_count.load(), 1000);  // 10 threads * 100 collections
}

// Test thread safety: concurrent writes
TEST_F(CollectionMetadataCacheTest, ConcurrentWrites) {
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
  EXPECT_EQ(cache_->Size(), 100);

  // Verify all can be retrieved
  for (int i = 1; i <= 100; ++i) {
    auto result = cache_->GetById(static_cast<core::CollectionId>(i));
    EXPECT_TRUE(result.ok());
  }
}

// Test thread safety: concurrent reads and writes
TEST_F(CollectionMetadataCacheTest, ConcurrentReadsAndWrites) {
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
  EXPECT_EQ(cache_->Size(), 250);
}
