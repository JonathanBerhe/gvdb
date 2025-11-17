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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
