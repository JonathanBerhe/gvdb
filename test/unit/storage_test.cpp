#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/local_storage.h"
#include "storage/segment.h"
#include "storage/segment_manager.h"
#include "storage/storage_factory.h"

using namespace gvdb;

// Test fixture for storage tests
class StorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary directory for tests
    test_dir_ = "/tmp/gvdb_storage_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Default test parameters
    dimension_ = 128;
    metric_ = core::MetricType::L2;
    collection_id_ = core::MakeCollectionId(1);
  }

  void TearDown() override {
    // Clean up test directory
    std::filesystem::remove_all(test_dir_);
  }

  // Helper: Create test vectors
  std::vector<core::Vector> CreateTestVectors(size_t count) {
    std::vector<core::Vector> vectors;
    vectors.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      vectors.push_back(core::RandomVector(dimension_));
    }
    return vectors;
  }

  // Helper: Create test vector IDs
  std::vector<core::VectorId> CreateTestVectorIds(size_t count,
                                                   uint64_t start = 1) {
    std::vector<core::VectorId> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(core::MakeVectorId(start + i));
    }
    return ids;
  }

  std::string test_dir_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  core::Dimension dimension_;
  core::MetricType metric_;
  core::CollectionId collection_id_;
};

// ============================================================================
// Segment Tests
// ============================================================================

TEST_F(StorageTest, SegmentCreate) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  EXPECT_EQ(segment.GetId(), segment_id);
  EXPECT_EQ(segment.GetCollectionId(), collection_id_);
  EXPECT_EQ(segment.GetDimension(), dimension_);
  EXPECT_EQ(segment.GetMetric(), metric_);
  EXPECT_EQ(segment.GetState(), core::SegmentState::GROWING);
  EXPECT_EQ(segment.GetVectorCount(), 0);
  EXPECT_TRUE(segment.CanAcceptWrites());
}

TEST_F(StorageTest, SegmentAddVectors) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);

  auto status = segment.AddVectors(vectors, ids);
  ASSERT_TRUE(status.ok()) << "AddVectors failed: " << status.message();

  EXPECT_EQ(segment.GetVectorCount(), 100);
  EXPECT_GT(segment.GetMemoryUsage(), 0);
}

TEST_F(StorageTest, SegmentReadVectors) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);

  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Read back specific vectors
  std::vector<core::VectorId> query_ids = {core::MakeVectorId(1),
                                             core::MakeVectorId(5),
                                             core::MakeVectorId(10)};

  auto result = segment.ReadVectors(query_ids);
  ASSERT_TRUE(result.ok()) << "ReadVectors failed: " << result.status().message();

  EXPECT_EQ(result.value().size(), 3);
}

TEST_F(StorageTest, SegmentSeal) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Create index
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());

  // Seal segment
  auto seal_status = segment.Seal(index_result.value().release());
  ASSERT_TRUE(seal_status.ok()) << "Seal failed: " << seal_status.message();

  EXPECT_EQ(segment.GetState(), core::SegmentState::SEALED);
  EXPECT_FALSE(segment.CanAcceptWrites());
}

TEST_F(StorageTest, SegmentSearch) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add and seal
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // Search
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.Search(query, 10);
  ASSERT_TRUE(search_result.ok())
      << "Search failed: " << search_result.status().message();

  EXPECT_LE(search_result.value().entries.size(), 10);
}

TEST_F(StorageTest, SegmentCannotAddAfterSeal) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Seal
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // Try to add more vectors (should fail)
  auto more_vectors = CreateTestVectors(5);
  auto more_ids = CreateTestVectorIds(5, 100);
  auto status = segment.AddVectors(more_vectors, more_ids);

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(status));
}

TEST_F(StorageTest, SegmentKeepsVectorsAfterSeal) {
  // Test Option 1: Vectors remain in memory after sealing
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  size_t memory_before_seal = segment.GetMemoryUsage();
  EXPECT_GT(memory_before_seal, 0);

  // Seal segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // CRITICAL TEST: Verify vectors are still readable after sealing
  // This proves Option 1 (in-memory) is active
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(1),
      core::MakeVectorId(50),
      core::MakeVectorId(100)
  };

  auto read_result = segment.ReadVectors(query_ids);
  ASSERT_TRUE(read_result.ok())
      << "ReadVectors after seal failed: " << read_result.status().message();

  EXPECT_EQ(read_result.value().size(), 3);

  // Verify memory usage is still high (vectors + index in memory)
  size_t memory_after_seal = segment.GetMemoryUsage();
  EXPECT_GT(memory_after_seal, 0);

  // For FLAT index: memory should be ~2x (vectors + index both store vectors)
  // This proves vectors weren't cleared
  EXPECT_GT(memory_after_seal, memory_before_seal * 0.5);
}

// ============================================================================
// Metadata Tests
// ============================================================================

TEST_F(StorageTest, SegmentAddVectorsWithMetadata) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Create vectors with metadata
  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 10; ++i) {
    core::Metadata meta = {
        {"price", core::MetadataValue(static_cast<int64_t>(100 + i * 10))},
        {"brand", core::MetadataValue(std::string(i % 2 == 0 ? "Nike" : "Adidas"))},
        {"in_stock", core::MetadataValue(i % 3 != 0)}
    };
    metadata.push_back(meta);
  }

  auto status = segment.AddVectorsWithMetadata(vectors, ids, metadata);
  ASSERT_TRUE(status.ok()) << "AddVectorsWithMetadata failed: " << status.message();

  EXPECT_EQ(segment.GetVectorCount(), 10);
}

TEST_F(StorageTest, SegmentGetMetadata) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 5; ++i) {
    core::Metadata meta = {
        {"id", core::MetadataValue(static_cast<int64_t>(i))},
        {"name", core::MetadataValue(std::string("item_") + std::to_string(i))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Get metadata for first vector
  auto meta_result = segment.GetMetadata(core::MakeVectorId(1));
  ASSERT_TRUE(meta_result.ok()) << "GetMetadata failed: " << meta_result.status().message();

  auto& meta = meta_result.value();
  EXPECT_EQ(std::get<int64_t>(meta.at("id")), 0);
  EXPECT_EQ(std::get<std::string>(meta.at("name")), "item_0");
}

TEST_F(StorageTest, SegmentGetMetadataNotFound) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);

  std::vector<core::Metadata> metadata(5);
  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Try to get metadata for non-existent vector
  auto meta_result = segment.GetMetadata(core::MakeVectorId(999));
  EXPECT_FALSE(meta_result.ok());
  EXPECT_TRUE(absl::IsNotFound(meta_result.status()));
}

TEST_F(StorageTest, SegmentSearchWithFilterGrowing) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(20);
  auto ids = CreateTestVectorIds(20);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 20; ++i) {
    core::Metadata meta = {
        {"price", core::MetadataValue(static_cast<int64_t>(50 + i * 10))},
        {"brand", core::MetadataValue(std::string(i % 2 == 0 ? "Nike" : "Adidas"))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Search with filter: price < 150
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 5, "price < 150");
  ASSERT_TRUE(search_result.ok())
      << "SearchWithFilter failed: " << search_result.status().message();

  // Should only return vectors with price < 150 (first 10 vectors)
  EXPECT_LE(search_result.value().Size(), 5);
  EXPECT_GT(search_result.value().Size(), 0);
}

TEST_F(StorageTest, SegmentSearchWithFilterSealed) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(50);
  auto ids = CreateTestVectorIds(50);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 50; ++i) {
    core::Metadata meta = {
        {"category", core::MetadataValue(std::string(i < 25 ? "shoes" : "apparel"))},
        {"rating", core::MetadataValue(3.0 + (i % 5) * 0.5)}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Seal segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // Search with filter after sealing
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "category = 'shoes' AND rating >= 4.0");
  ASSERT_TRUE(search_result.ok())
      << "SearchWithFilter on sealed segment failed: " << search_result.status().message();

  EXPECT_LE(search_result.value().Size(), 10);
}

TEST_F(StorageTest, SegmentSearchWithFilterComplex) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(30);
  auto ids = CreateTestVectorIds(30);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 30; ++i) {
    core::Metadata meta = {
        {"price", core::MetadataValue(static_cast<int64_t>(100 + i * 20))},
        {"brand", core::MetadataValue(std::string(i % 3 == 0 ? "Nike" : i % 3 == 1 ? "Adidas" : "Puma"))},
        {"in_stock", core::MetadataValue(i % 4 != 0)}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Complex filter: (price < 300 OR brand = 'Nike') AND in_stock = true
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(
      query, 10, "(price < 300 OR brand = 'Nike') AND in_stock = true");
  ASSERT_TRUE(search_result.ok())
      << "Complex filter failed: " << search_result.status().message();

  EXPECT_GT(search_result.value().Size(), 0);
}

TEST_F(StorageTest, SegmentSearchWithFilterLike) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(15);
  auto ids = CreateTestVectorIds(15);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 15; ++i) {
    core::Metadata meta = {
        {"name", core::MetadataValue(std::string(
            i < 5 ? "Nike Air Max" :
            i < 10 ? "Nike Pegasus" :
            "Adidas Ultra"))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter with LIKE pattern
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "name LIKE 'Nike%'");
  ASSERT_TRUE(search_result.ok())
      << "LIKE filter failed: " << search_result.status().message();

  // Should return only Nike products (first 10 vectors)
  EXPECT_LE(search_result.value().Size(), 10);
  EXPECT_GT(search_result.value().Size(), 0);
}

TEST_F(StorageTest, SegmentSearchWithFilterIn) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(20);
  auto ids = CreateTestVectorIds(20);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 20; ++i) {
    core::Metadata meta = {
        {"brand", core::MetadataValue(std::string(
            i % 4 == 0 ? "Nike" :
            i % 4 == 1 ? "Adidas" :
            i % 4 == 2 ? "Puma" : "Reebok"))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter with IN operator
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "brand IN ('Nike', 'Adidas')");
  ASSERT_TRUE(search_result.ok())
      << "IN filter failed: " << search_result.status().message();

  EXPECT_GT(search_result.value().Size(), 0);
}

TEST_F(StorageTest, SegmentSearchWithInvalidFilter) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  std::vector<core::Metadata> metadata(10);

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Invalid filter syntax
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "price <");
  EXPECT_FALSE(search_result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(search_result.status()));
}

TEST_F(StorageTest, SegmentAddVectorsWithMetadataSizeMismatch) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  std::vector<core::Metadata> metadata(5);  // Wrong size!

  auto status = segment.AddVectorsWithMetadata(vectors, ids, metadata);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(StorageTest, SegmentAddVectorsWithInvalidMetadata) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 5; ++i) {
    core::Metadata meta;
    // Add too many fields (> 100)
    for (int j = 0; j < 101; ++j) {
      meta["field_" + std::to_string(j)] = core::MetadataValue(int64_t(j));
    }
    metadata.push_back(meta);
  }

  auto status = segment.AddVectorsWithMetadata(vectors, ids, metadata);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(StorageTest, SegmentSearchWithFilterNoMatches) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 10; ++i) {
    core::Metadata meta = {
        {"price", core::MetadataValue(int64_t(100))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter that matches nothing
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "price > 1000");
  ASSERT_TRUE(search_result.ok());

  // Should return empty result
  EXPECT_EQ(search_result.value().Size(), 0);
}

// ============================================================================
// SegmentManager Tests
// ============================================================================

TEST_F(StorageTest, SegmentManagerCreate) {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  ASSERT_TRUE(segment_id.ok())
      << "CreateSegment failed: " << segment_id.status().message();

  EXPECT_EQ(manager.GetSegmentCount(), 1);

  auto* segment = manager.GetSegment(segment_id.value());
  ASSERT_NE(segment, nullptr);
  EXPECT_EQ(segment->GetState(), core::SegmentState::GROWING);
}

TEST_F(StorageTest, SegmentManagerWriteRead) {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  ASSERT_TRUE(segment_id.ok());

  // Write vectors
  auto vectors = CreateTestVectors(50);
  auto ids = CreateTestVectorIds(50);

  auto write_status =
      manager.WriteVectors(segment_id.value(), vectors, ids);
  ASSERT_TRUE(write_status.ok())
      << "WriteVectors failed: " << write_status.message();

  // Read back
  std::vector<core::VectorId> query_ids = {core::MakeVectorId(1),
                                             core::MakeVectorId(25),
                                             core::MakeVectorId(50)};

  auto read_result = manager.ReadVectors(segment_id.value(), query_ids);
  ASSERT_TRUE(read_result.ok())
      << "ReadVectors failed: " << read_result.status().message();

  EXPECT_EQ(read_result.value().size(), 3);
}

TEST_F(StorageTest, SegmentManagerSealSegment) {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  ASSERT_TRUE(segment_id.ok());

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  ASSERT_TRUE(manager.WriteVectors(segment_id.value(), vectors, ids).ok());

  // Seal
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto seal_status = manager.SealSegment(segment_id.value(), config);
  ASSERT_TRUE(seal_status.ok()) << "Seal failed: " << seal_status.message();

  auto* segment = manager.GetSegment(segment_id.value());
  ASSERT_NE(segment, nullptr);
  EXPECT_EQ(segment->GetState(), core::SegmentState::SEALED);
}

TEST_F(StorageTest, SegmentManagerMultipleSegments) {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  // Create 3 segments
  auto seg1 = manager.CreateSegment(collection_id_, dimension_, metric_);
  auto seg2 = manager.CreateSegment(collection_id_, dimension_, metric_);
  auto seg3 = manager.CreateSegment(collection_id_, dimension_, metric_);

  ASSERT_TRUE(seg1.ok());
  ASSERT_TRUE(seg2.ok());
  ASSERT_TRUE(seg3.ok());

  EXPECT_EQ(manager.GetSegmentCount(), 3);

  auto collection_segments = manager.GetCollectionSegments(collection_id_);
  EXPECT_EQ(collection_segments.size(), 3);
}

TEST_F(StorageTest, SegmentManagerDropSegment) {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  ASSERT_TRUE(segment_id.ok());

  EXPECT_EQ(manager.GetSegmentCount(), 1);

  auto drop_status = manager.DropSegment(segment_id.value(), false);
  ASSERT_TRUE(drop_status.ok()) << "Drop failed: " << drop_status.message();

  EXPECT_EQ(manager.GetSegmentCount(), 0);
  EXPECT_EQ(manager.GetSegment(segment_id.value()), nullptr);
}

// ============================================================================
// LocalStorage Tests
// ============================================================================

TEST_F(StorageTest, LocalStorageCreate) {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  EXPECT_EQ(storage.GetStorageSize(), 0);
}

TEST_F(StorageTest, LocalStorageMetadata) {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  // Put metadata
  ASSERT_TRUE(storage.PutMetadata("key1", "value1").ok());
  ASSERT_TRUE(storage.PutMetadata("key2", "value2").ok());

  // Get metadata
  auto result1 = storage.GetMetadata("key1");
  ASSERT_TRUE(result1.ok());
  EXPECT_EQ(result1.value(), "value1");

  auto result2 = storage.GetMetadata("key2");
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.value(), "value2");

  // Delete metadata
  ASSERT_TRUE(storage.DeleteMetadata("key1").ok());

  auto result3 = storage.GetMetadata("key1");
  EXPECT_FALSE(result3.ok());
  EXPECT_TRUE(absl::IsNotFound(result3.status()));
}

TEST_F(StorageTest, LocalStorageSegmentOperations) {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  // Register collection first by creating a segment
  // (This implicitly registers the collection)
  core::IndexConfig index_config;
  index_config.dimension = dimension_;
  index_config.metric_type = metric_;

  // We need to register the collection first through metadata
  // For now, we'll use a workaround by directly creating through
  // segment manager This is a limitation of the current API

  // Create segment (this should fail without collection registration)
  auto segment_result = storage.CreateSegment(collection_id_);
  EXPECT_FALSE(segment_result.ok());
  EXPECT_TRUE(absl::IsNotFound(segment_result.status()));
}

// ============================================================================
// StorageFactory Tests
// ============================================================================

TEST_F(StorageTest, StorageFactoryLocalDisk) {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  auto storage_result = factory.CreateStorage(config);
  ASSERT_TRUE(storage_result.ok())
      << "CreateStorage failed: " << storage_result.status().message();

  EXPECT_NE(storage_result.value(), nullptr);
}

TEST_F(StorageTest, StorageFactoryMemory) {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::MEMORY;
  config.base_path = "/tmp/gvdb_memory_test";

  auto storage_result = factory.CreateStorage(config);
  ASSERT_TRUE(storage_result.ok());

  EXPECT_NE(storage_result.value(), nullptr);
}

TEST_F(StorageTest, StorageFactoryS3Unimplemented) {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::S3;
  config.base_path = "s3://bucket/path";

  auto storage_result = factory.CreateStorage(config);
  EXPECT_FALSE(storage_result.ok());
  EXPECT_TRUE(absl::IsUnimplemented(storage_result.status()));
}

TEST_F(StorageTest, StorageFactoryInvalidConfig) {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.base_path = "";  // Invalid: empty path

  auto storage_result = factory.CreateStorage(config);
  EXPECT_FALSE(storage_result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(storage_result.status()));
}

// ============================================================================
// Compaction Tests
// ============================================================================

TEST_F(StorageTest, CompactNoSegments) {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  // Compacting with no segments should succeed as no-op
  auto compact_status = storage.Compact();
  EXPECT_TRUE(compact_status.ok());
}

// =============================================================================
// Get Vectors Tests
// =============================================================================

TEST_F(StorageTest, SegmentGetVectors) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 10 vectors
  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Get existing vectors
  std::vector<core::VectorId> query_ids = {ids[0], ids[5], ids[9]};
  auto result = segment.GetVectors(query_ids, false);

  EXPECT_EQ(result.found_ids.size(), 3);
  EXPECT_EQ(result.found_vectors.size(), 3);
  EXPECT_EQ(result.not_found_ids.size(), 0);

  // Verify vector content
  EXPECT_EQ(result.found_ids[0], ids[0]);
  EXPECT_EQ(result.found_vectors[0].dimension(), dimension_);
}

TEST_F(StorageTest, SegmentGetVectorsPartialMatch) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 5 vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Query mix of existing and non-existing IDs
  std::vector<core::VectorId> query_ids = {
      ids[0],                      // exists
      core::MakeVectorId(999),     // doesn't exist
      ids[2],                      // exists
      core::MakeVectorId(888)      // doesn't exist
  };

  auto result = segment.GetVectors(query_ids, false);

  EXPECT_EQ(result.found_ids.size(), 2);
  EXPECT_EQ(result.found_vectors.size(), 2);
  EXPECT_EQ(result.not_found_ids.size(), 2);

  // Verify found IDs
  EXPECT_EQ(result.found_ids[0], ids[0]);
  EXPECT_EQ(result.found_ids[1], ids[2]);

  // Verify not found IDs
  EXPECT_EQ(result.not_found_ids[0], core::MakeVectorId(999));
  EXPECT_EQ(result.not_found_ids[1], core::MakeVectorId(888));
}

TEST_F(StorageTest, SegmentGetVectorsWithMetadata) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors with metadata
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);

  std::vector<core::Metadata> metadata;
  for (size_t i = 0; i < 5; ++i) {
    core::Metadata meta = {
        {"id", core::MetadataValue(static_cast<int64_t>(i))},
        {"name", core::MetadataValue(std::string("vector_") + std::to_string(i))}
    };
    metadata.push_back(meta);
  }

  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Get vectors with metadata
  std::vector<core::VectorId> query_ids = {ids[1], ids[3]};
  auto result = segment.GetVectors(query_ids, true);

  EXPECT_EQ(result.found_ids.size(), 2);
  EXPECT_EQ(result.found_vectors.size(), 2);
  EXPECT_EQ(result.found_metadata.size(), 2);
  EXPECT_EQ(result.not_found_ids.size(), 0);

  // Verify metadata
  EXPECT_EQ(result.found_metadata[0].size(), 2);
  EXPECT_EQ(std::get<std::string>(result.found_metadata[0].at("name")), "vector_1");
  EXPECT_EQ(std::get<std::string>(result.found_metadata[1].at("name")), "vector_3");
}

TEST_F(StorageTest, SegmentGetVectorsAllNotFound) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Query only non-existing IDs
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(999),
      core::MakeVectorId(888),
      core::MakeVectorId(777)
  };

  auto result = segment.GetVectors(query_ids, false);

  EXPECT_EQ(result.found_ids.size(), 0);
  EXPECT_EQ(result.found_vectors.size(), 0);
  EXPECT_EQ(result.not_found_ids.size(), 3);
}

TEST_F(StorageTest, SegmentGetVectorsEmpty) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // No vectors inserted

  // Query should return all as not found
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(1),
      core::MakeVectorId(2)
  };

  auto result = segment.GetVectors(query_ids, false);

  EXPECT_EQ(result.found_ids.size(), 0);
  EXPECT_EQ(result.found_vectors.size(), 0);
  EXPECT_EQ(result.not_found_ids.size(), 2);
}

// ============================================================================
// Delete Tests
// ============================================================================

TEST_F(StorageTest, SegmentDeleteVectors) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 5 vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  EXPECT_EQ(segment.GetVectorCount(), 5);

  // Delete 3 vectors
  std::vector<core::VectorId> delete_ids = {ids[0], ids[2], ids[4]};
  auto result = segment.DeleteVectors(delete_ids);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->deleted_count, 3);
  EXPECT_EQ(result->not_found_ids.size(), 0);
  EXPECT_EQ(segment.GetVectorCount(), 2);

  // Verify remaining vectors
  auto read_result = segment.ReadVectors({ids[1], ids[3]});
  ASSERT_TRUE(read_result.ok());
  EXPECT_EQ(read_result->size(), 2);
}

TEST_F(StorageTest, SegmentDeleteVectorsPartialMatch) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 3 vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Try to delete mix of existing and non-existing IDs
  std::vector<core::VectorId> delete_ids = {
      ids[0],                      // exists
      core::MakeVectorId(999),     // doesn't exist
      ids[2],                      // exists
      core::MakeVectorId(888)      // doesn't exist
  };

  auto result = segment.DeleteVectors(delete_ids);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->deleted_count, 2);
  EXPECT_EQ(result->not_found_ids.size(), 2);
  EXPECT_EQ(segment.GetVectorCount(), 1);

  // Verify not found IDs
  EXPECT_EQ(result->not_found_ids[0], core::MakeVectorId(999));
  EXPECT_EQ(result->not_found_ids[1], core::MakeVectorId(888));

  // Verify remaining vector
  auto read_result = segment.ReadVectors({ids[1]});
  ASSERT_TRUE(read_result.ok());
}

TEST_F(StorageTest, SegmentDeleteVectorsWithMetadata) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors with metadata
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  std::vector<core::Metadata> metadata(3);
  for (int i = 0; i < 3; ++i) {
    metadata[i]["name"] = core::MetadataValue(
        std::string("vector_") + std::to_string(i));
  }
  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Verify metadata exists
  auto meta_before = segment.GetMetadata(ids[0]);
  ASSERT_TRUE(meta_before.ok());

  // Delete vector
  auto result = segment.DeleteVectors({ids[0]});
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->deleted_count, 1);

  // Verify metadata is also deleted
  auto meta_after = segment.GetMetadata(ids[0]);
  EXPECT_FALSE(meta_after.ok());
  EXPECT_EQ(meta_after.status().code(), absl::StatusCode::kNotFound);
}

TEST_F(StorageTest, SegmentDeleteVectorsAllNotFound) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 3 vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Try to delete non-existent IDs
  std::vector<core::VectorId> delete_ids = {
      core::MakeVectorId(999),
      core::MakeVectorId(888),
      core::MakeVectorId(777)
  };

  auto result = segment.DeleteVectors(delete_ids);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->deleted_count, 0);
  EXPECT_EQ(result->not_found_ids.size(), 3);
  EXPECT_EQ(segment.GetVectorCount(), 3);  // No vectors deleted
}

TEST_F(StorageTest, SegmentDeleteVectorsSealedState) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Seal the segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // Try to delete from sealed segment - should fail
  auto result = segment.DeleteVectors({ids[0]});

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kFailedPrecondition);
}

// ============================================================================
// UpdateMetadata Tests
// ============================================================================

TEST_F(StorageTest, SegmentUpdateMetadataReplace) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector with metadata
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  core::Metadata initial_metadata;
  initial_metadata["price"] = core::MetadataValue(100.0);
  initial_metadata["brand"] = core::MetadataValue(std::string("Nike"));
  initial_metadata["in_stock"] = core::MetadataValue(true);

  std::vector<core::Metadata> metadata_vec = {initial_metadata};
  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata_vec).ok());

  // Update metadata (replace mode)
  core::Metadata new_metadata;
  new_metadata["price"] = core::MetadataValue(80.0);  // New price
  new_metadata["rating"] = core::MetadataValue(4.5);  // New field

  auto status = segment.UpdateMetadata(ids[0], new_metadata, false);
  ASSERT_TRUE(status.ok());

  // Verify metadata was replaced
  auto result = segment.GetMetadata(ids[0]);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ(result->size(), 2);  // Only 2 fields now (price, rating)
  EXPECT_EQ(std::get<double>(result->at("price")), 80.0);
  EXPECT_EQ(std::get<double>(result->at("rating")), 4.5);
  EXPECT_EQ(result->find("brand"), result->end());  // Old field removed
  EXPECT_EQ(result->find("in_stock"), result->end());  // Old field removed
}

TEST_F(StorageTest, SegmentUpdateMetadataMerge) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector with metadata
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  core::Metadata initial_metadata;
  initial_metadata["price"] = core::MetadataValue(100.0);
  initial_metadata["brand"] = core::MetadataValue(std::string("Nike"));
  initial_metadata["in_stock"] = core::MetadataValue(true);

  std::vector<core::Metadata> metadata_vec = {initial_metadata};
  ASSERT_TRUE(segment.AddVectorsWithMetadata(vectors, ids, metadata_vec).ok());

  // Update metadata (merge mode)
  core::Metadata update_metadata;
  update_metadata["price"] = core::MetadataValue(80.0);  // Update existing
  update_metadata["rating"] = core::MetadataValue(4.5);  // Add new

  auto status = segment.UpdateMetadata(ids[0], update_metadata, true);
  ASSERT_TRUE(status.ok());

  // Verify metadata was merged
  auto result = segment.GetMetadata(ids[0]);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ(result->size(), 4);  // All 4 fields present
  EXPECT_EQ(std::get<double>(result->at("price")), 80.0);  // Updated
  EXPECT_EQ(std::get<std::string>(result->at("brand")), "Nike");  // Preserved
  EXPECT_EQ(std::get<bool>(result->at("in_stock")), true);  // Preserved
  EXPECT_EQ(std::get<double>(result->at("rating")), 4.5);  // Added
}

TEST_F(StorageTest, SegmentUpdateMetadataNoExisting) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector without metadata
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Add metadata via update
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);
  metadata["brand"] = core::MetadataValue(std::string("Nike"));

  auto status = segment.UpdateMetadata(ids[0], metadata, true);
  ASSERT_TRUE(status.ok());

  // Verify metadata was added
  auto result = segment.GetMetadata(ids[0]);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), 2);
  EXPECT_EQ(std::get<double>(result->at("price")), 100.0);
}

TEST_F(StorageTest, SegmentUpdateMetadataNotFound) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Try to update metadata for non-existent vector
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);

  auto status = segment.UpdateMetadata(core::MakeVectorId(999), metadata, false);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
}

TEST_F(StorageTest, SegmentUpdateMetadataSealedState) {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Seal the segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  ASSERT_TRUE(index_result.ok());
  ASSERT_TRUE(segment.Seal(index_result.value().release()).ok());

  // Try to update metadata in sealed segment
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);

  auto status = segment.UpdateMetadata(ids[0], metadata, false);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
}

// ========== Segment Serialization Tests ==========

TEST_F(StorageTest, SegmentSerializeDeserializeEmpty) {
  auto segment_id = core::MakeSegmentId(42);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Serialize empty segment
  auto serialize_result = segment.SerializeToBytes();
  ASSERT_TRUE(serialize_result.ok()) << serialize_result.status();

  const auto& bytes = serialize_result.value();
  EXPECT_GT(bytes.size(), 0);

  // Deserialize
  auto deserialize_result = storage::Segment::DeserializeFromBytes(bytes);
  ASSERT_TRUE(deserialize_result.ok()) << deserialize_result.status();

  auto deserialized = std::move(deserialize_result.value());
  EXPECT_EQ(deserialized->GetId(), segment_id);
  EXPECT_EQ(deserialized->GetCollectionId(), collection_id_);
  EXPECT_EQ(deserialized->GetDimension(), dimension_);
  EXPECT_EQ(deserialized->GetMetric(), metric_);
  EXPECT_EQ(deserialized->GetVectorCount(), 0);
}

TEST_F(StorageTest, SegmentSerializeDeserializeWithVectors) {
  auto segment_id = core::MakeSegmentId(100);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  ASSERT_TRUE(serialize_result.ok()) << serialize_result.status();

  // Deserialize
  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  ASSERT_TRUE(deserialize_result.ok()) << deserialize_result.status();

  auto deserialized = std::move(deserialize_result.value());

  // Verify metadata
  EXPECT_EQ(deserialized->GetId(), segment_id);
  EXPECT_EQ(deserialized->GetVectorCount(), 10);

  // Verify vectors match
  auto read_result = deserialized->ReadVectors(ids);
  ASSERT_TRUE(read_result.ok());
  ASSERT_EQ(read_result->size(), 10);

  for (size_t i = 0; i < vectors.size(); ++i) {
    const auto& original = vectors[i];
    const auto& read = read_result->at(i);
    ASSERT_EQ(original.size(), read.size());
    for (size_t j = 0; j < original.size(); ++j) {
      EXPECT_FLOAT_EQ(original[j], read[j]);
    }
  }
}

TEST_F(StorageTest, SegmentSerializeDeserializeWithMetadata) {
  auto segment_id = core::MakeSegmentId(200);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Add metadata to each vector
  for (size_t i = 0; i < ids.size(); ++i) {
    core::Metadata metadata;
    metadata["index"] = core::MetadataValue(static_cast<int64_t>(i));
    metadata["price"] = core::MetadataValue(static_cast<double>(i * 10.5));
    metadata["name"] = core::MetadataValue(std::string("vector_") + std::to_string(i));
    metadata["active"] = core::MetadataValue(i % 2 == 0);

    ASSERT_TRUE(segment.UpdateMetadata(ids[i], metadata, false).ok());
  }

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  ASSERT_TRUE(serialize_result.ok()) << serialize_result.status();

  // Deserialize
  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  ASSERT_TRUE(deserialize_result.ok()) << deserialize_result.status();

  auto deserialized = std::move(deserialize_result.value());

  // Verify metadata for each vector
  for (size_t i = 0; i < ids.size(); ++i) {
    auto metadata_result = deserialized->GetMetadata(ids[i]);
    ASSERT_TRUE(metadata_result.ok());
    const auto& metadata = *metadata_result;

    EXPECT_EQ(std::get<int64_t>(metadata.at("index")), static_cast<int64_t>(i));
    EXPECT_DOUBLE_EQ(std::get<double>(metadata.at("price")), i * 10.5);
    EXPECT_EQ(std::get<std::string>(metadata.at("name")),
              std::string("vector_") + std::to_string(i));
    EXPECT_EQ(std::get<bool>(metadata.at("active")), i % 2 == 0);
  }
}

TEST_F(StorageTest, SegmentSerializeDeserializeLargeSegment) {
  auto segment_id = core::MakeSegmentId(300);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add many vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Add metadata to subset
  for (size_t i = 0; i < 50; ++i) {
    core::Metadata metadata;
    metadata["batch"] = core::MetadataValue(static_cast<int64_t>(i / 10));
    metadata["score"] = core::MetadataValue(static_cast<double>(i) * 0.1);
    ASSERT_TRUE(segment.UpdateMetadata(ids[i], metadata, false).ok());
  }

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  ASSERT_TRUE(serialize_result.ok()) << serialize_result.status();

  // Should have reasonable size
  const auto& bytes = serialize_result.value();
  size_t expected_min_size = 100 * dimension_ * sizeof(float);  // Just vectors
  EXPECT_GT(bytes.size(), expected_min_size);

  // Deserialize
  auto deserialize_result = storage::Segment::DeserializeFromBytes(bytes);
  ASSERT_TRUE(deserialize_result.ok()) << deserialize_result.status();

  auto deserialized = std::move(deserialize_result.value());
  EXPECT_EQ(deserialized->GetVectorCount(), 100);

  // Spot check a few vectors
  std::vector<core::VectorId> check_ids = {ids[0], ids[49], ids[99]};
  auto read_result = deserialized->ReadVectors(check_ids);
  ASSERT_TRUE(read_result.ok());
  EXPECT_EQ(read_result->size(), 3);
}

TEST_F(StorageTest, SegmentSerializeDeserializeStatePreservation) {
  auto segment_id = core::MakeSegmentId(400);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  ASSERT_TRUE(segment.AddVectors(vectors, ids).ok());

  // Initial state should be GROWING
  EXPECT_EQ(segment.GetState(), core::SegmentState::GROWING);

  // Serialize and deserialize
  auto serialize_result = segment.SerializeToBytes();
  ASSERT_TRUE(serialize_result.ok());

  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  ASSERT_TRUE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());

  // State should be preserved
  EXPECT_EQ(deserialized->GetState(), core::SegmentState::GROWING);
  EXPECT_EQ(deserialized->GetVectorCount(), 5);
}

TEST_F(StorageTest, SegmentDeserializeInvalidData) {
  // Empty data
  std::string empty_bytes;
  auto result1 = storage::Segment::DeserializeFromBytes(empty_bytes);
  EXPECT_FALSE(result1.ok());

  // Truncated data (just a few bytes)
  std::string truncated_bytes = "abcd";
  auto result2 = storage::Segment::DeserializeFromBytes(truncated_bytes);
  EXPECT_FALSE(result2.ok());

  // Invalid header
  std::string invalid_bytes(100, 'x');  // Random data
  auto result3 = storage::Segment::DeserializeFromBytes(invalid_bytes);
  EXPECT_FALSE(result3.ok());
}

// ============================================================================
// Persistence Round-Trip Tests
// ============================================================================

TEST_F(StorageTest, FlushAndLoadSegmentWithVectors) {
  // Use SegmentManager for proper directory creation
  auto sm = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());

  auto seg_result = sm->CreateSegment(core::CollectionId(1), 4, core::MetricType::L2);
  ASSERT_TRUE(seg_result.ok());
  core::SegmentId seg_id = *seg_result;

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 1; i <= 5; ++i) {
    std::vector<float> data = {static_cast<float>(i), 0.0f, 0.0f, 0.0f};
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i));
  }
  ASSERT_TRUE(sm->WriteVectors(seg_id, vectors, ids).ok());

  // Seal and flush
  core::IndexConfig idx_cfg;
  idx_cfg.index_type = core::IndexType::FLAT;
  idx_cfg.dimension = 4;
  idx_cfg.metric_type = core::MetricType::L2;
  ASSERT_TRUE(sm->SealSegment(seg_id, idx_cfg).ok());
  ASSERT_TRUE(sm->FlushSegment(seg_id).ok());

  // Load from disk into a NEW segment
  auto loaded = storage::Segment::Load(test_dir_, seg_id);
  ASSERT_TRUE(loaded.ok()) << loaded.status().message();

  // Verify vectors survived the round-trip
  EXPECT_EQ((*loaded)->GetVectorCount(), 5);
  EXPECT_EQ((*loaded)->GetDimension(), 4);

  auto result = (*loaded)->GetVectors(ids, false);
  EXPECT_EQ(result.found_ids.size(), 5);
  EXPECT_EQ(result.not_found_ids.size(), 0);
}

TEST_F(StorageTest, LoadAllSegmentsRecovery) {
  // Create and flush a segment via SegmentManager
  auto sm1 = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());

  auto seg_result = sm1->CreateSegment(core::CollectionId(1), 4, core::MetricType::L2);
  ASSERT_TRUE(seg_result.ok());
  core::SegmentId seg_id = *seg_result;

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 1; i <= 3; ++i) {
    std::vector<float> data = {static_cast<float>(i), 1.0f, 2.0f, 3.0f};
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i));
  }
  ASSERT_TRUE(sm1->WriteVectors(seg_id, vectors, ids).ok());

  // Seal and flush
  core::IndexConfig idx_cfg;
  idx_cfg.index_type = core::IndexType::FLAT;
  idx_cfg.dimension = 4;
  idx_cfg.metric_type = core::MetricType::L2;
  ASSERT_TRUE(sm1->SealSegment(seg_id, idx_cfg).ok());
  ASSERT_TRUE(sm1->FlushSegment(seg_id).ok());

  // Destroy the old SegmentManager (simulates process restart)
  sm1.reset();

  // Create a new SegmentManager with the same base_path
  auto sm2 = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());
  EXPECT_EQ(sm2->GetSegmentCount(), 0);  // Nothing loaded yet

  // Recover segments from disk
  ASSERT_TRUE(sm2->LoadAllSegments().ok());
  EXPECT_EQ(sm2->GetSegmentCount(), 1);

  // Verify the recovered segment has the vectors
  auto* segment = sm2->GetSegment(seg_id);
  ASSERT_NE(segment, nullptr);
  EXPECT_EQ(segment->GetVectorCount(), 3);

  auto result = segment->GetVectors(ids, false);
  EXPECT_EQ(result.found_ids.size(), 3);

  // Verify search works after recovery (index was rebuilt)
  std::vector<float> query_data = {1.0f, 1.0f, 2.0f, 3.0f};
  core::Vector query(std::move(query_data));
  auto search_result = segment->Search(query, 2);
  ASSERT_TRUE(search_result.ok()) << search_result.status().message();
  EXPECT_EQ(search_result->entries.size(), 2);
  EXPECT_EQ(search_result->entries[0].id, core::MakeVectorId(1));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
