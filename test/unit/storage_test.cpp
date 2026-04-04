#include <doctest/doctest.h>

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
class StorageTest {
 public:
  StorageTest() {
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

  ~StorageTest() {
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

TEST_CASE_FIXTURE(StorageTest, "SegmentCreate") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  CHECK_EQ(segment.GetId(), segment_id);
  CHECK_EQ(segment.GetCollectionId(), collection_id_);
  CHECK_EQ(segment.GetDimension(), dimension_);
  CHECK_EQ(segment.GetMetric(), metric_);
  CHECK_EQ(segment.GetState(), core::SegmentState::GROWING);
  CHECK_EQ(segment.GetVectorCount(), 0);
  CHECK(segment.CanAcceptWrites());
}

TEST_CASE_FIXTURE(StorageTest, "SegmentAddVectors") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);

  auto status = segment.AddVectors(vectors, ids);
  INFO("AddVectors failed: " << status.message());
  REQUIRE(status.ok());

  CHECK_EQ(segment.GetVectorCount(), 100);
  CHECK_GT(segment.GetMemoryUsage(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentReadVectors") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);

  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Read back specific vectors
  std::vector<core::VectorId> query_ids = {core::MakeVectorId(1),
                                             core::MakeVectorId(5),
                                             core::MakeVectorId(10)};

  auto result = segment.ReadVectors(query_ids);
  INFO("ReadVectors failed: " << result.status().message());
  REQUIRE(result.ok());

  CHECK_EQ(result.value().size(), 3);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSeal") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Create index
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());

  // Seal segment
  auto seal_status = segment.Seal(index_result.value().release());
  INFO("Seal failed: " << seal_status.message());
  REQUIRE(seal_status.ok());

  CHECK_EQ(segment.GetState(), core::SegmentState::SEALED);
  CHECK_FALSE(segment.CanAcceptWrites());
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearch") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add and seal
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // Search
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.Search(query, 10);
  INFO("Search failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  CHECK_LE(search_result.value().entries.size(), 10);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentCannotAddAfterSeal") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Seal
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // Try to add more vectors (should fail)
  auto more_vectors = CreateTestVectors(5);
  auto more_ids = CreateTestVectorIds(5, 100);
  auto status = segment.AddVectors(more_vectors, more_ids);

  CHECK_FALSE(status.ok());
  CHECK(absl::IsFailedPrecondition(status));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentKeepsVectorsAfterSeal") {
  // Test Option 1: Vectors remain in memory after sealing
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  size_t memory_before_seal = segment.GetMemoryUsage();
  CHECK_GT(memory_before_seal, 0);

  // Seal segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // CRITICAL TEST: Verify vectors are still readable after sealing
  // This proves Option 1 (in-memory) is active
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(1),
      core::MakeVectorId(50),
      core::MakeVectorId(100)
  };

  auto read_result = segment.ReadVectors(query_ids);
  INFO("ReadVectors after seal failed: " << read_result.status().message());
  REQUIRE(read_result.ok());

  CHECK_EQ(read_result.value().size(), 3);

  // Verify memory usage is still high (vectors + index in memory)
  size_t memory_after_seal = segment.GetMemoryUsage();
  CHECK_GT(memory_after_seal, 0);

  // For FLAT index: memory should be ~2x (vectors + index both store vectors)
  // This proves vectors weren't cleared
  CHECK_GT(memory_after_seal, memory_before_seal * 0.5);
}

// ============================================================================
// Metadata Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentAddVectorsWithMetadata") {
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
  INFO("AddVectorsWithMetadata failed: " << status.message());
  REQUIRE(status.ok());

  CHECK_EQ(segment.GetVectorCount(), 10);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetMetadata") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Get metadata for first vector
  auto meta_result = segment.GetMetadata(core::MakeVectorId(1));
  INFO("GetMetadata failed: " << meta_result.status().message());
  REQUIRE(meta_result.ok());

  auto& meta = meta_result.value();
  CHECK_EQ(std::get<int64_t>(meta.at("id")), 0);
  CHECK_EQ(std::get<std::string>(meta.at("name")), "item_0");
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetMetadataNotFound") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);

  std::vector<core::Metadata> metadata(5);
  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Try to get metadata for non-existent vector
  auto meta_result = segment.GetMetadata(core::MakeVectorId(999));
  CHECK_FALSE(meta_result.ok());
  CHECK(absl::IsNotFound(meta_result.status()));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterGrowing") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Search with filter: price < 150
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 5, "price < 150");
  INFO("SearchWithFilter failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  // Should only return vectors with price < 150 (first 10 vectors)
  CHECK_LE(search_result.value().Size(), 5);
  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterSealed") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Seal segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // Search with filter after sealing
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "category = 'shoes' AND rating >= 4.0");
  INFO("SearchWithFilter on sealed segment failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  CHECK_LE(search_result.value().Size(), 10);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterComplex") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Complex filter: (price < 300 OR brand = 'Nike') AND in_stock = true
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(
      query, 10, "(price < 300 OR brand = 'Nike') AND in_stock = true");
  INFO("Complex filter failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterLike") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter with LIKE pattern
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "name LIKE 'Nike%'");
  INFO("LIKE filter failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  // Should return only Nike products (first 10 vectors)
  CHECK_LE(search_result.value().Size(), 10);
  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterIn") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter with IN operator
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "brand IN ('Nike', 'Adidas')");
  INFO("IN filter failed: " << search_result.status().message());
  REQUIRE(search_result.ok());

  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithInvalidFilter") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  std::vector<core::Metadata> metadata(10);

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Invalid filter syntax
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "price <");
  CHECK_FALSE(search_result.ok());
  CHECK(absl::IsInvalidArgument(search_result.status()));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentAddVectorsWithMetadataSizeMismatch") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  std::vector<core::Metadata> metadata(5);  // Wrong size!

  auto status = segment.AddVectorsWithMetadata(vectors, ids, metadata);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentAddVectorsWithInvalidMetadata") {
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
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSearchWithFilterNoMatches") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Filter that matches nothing
  auto query = core::RandomVector(dimension_);
  auto search_result = segment.SearchWithFilter(query, 10, "price > 1000");
  REQUIRE(search_result.ok());

  // Should return empty result
  CHECK_EQ(search_result.value().Size(), 0);
}

// ============================================================================
// SegmentManager Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentManagerCreate") {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  INFO("CreateSegment failed: " << segment_id.status().message());
  REQUIRE(segment_id.ok());

  CHECK_EQ(manager.GetSegmentCount(), 1);

  auto* segment = manager.GetSegment(segment_id.value());
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetState(), core::SegmentState::GROWING);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentManagerWriteRead") {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(segment_id.ok());

  // Write vectors
  auto vectors = CreateTestVectors(50);
  auto ids = CreateTestVectorIds(50);

  auto write_status =
      manager.WriteVectors(segment_id.value(), vectors, ids);
  INFO("WriteVectors failed: " << write_status.message());
  REQUIRE(write_status.ok());

  // Read back
  std::vector<core::VectorId> query_ids = {core::MakeVectorId(1),
                                             core::MakeVectorId(25),
                                             core::MakeVectorId(50)};

  auto read_result = manager.ReadVectors(segment_id.value(), query_ids);
  INFO("ReadVectors failed: " << read_result.status().message());
  REQUIRE(read_result.ok());

  CHECK_EQ(read_result.value().size(), 3);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentManagerSealSegment") {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(segment_id.ok());

  // Add vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  REQUIRE(manager.WriteVectors(segment_id.value(), vectors, ids).ok());

  // Seal
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto seal_status = manager.SealSegment(segment_id.value(), config);
  INFO("Seal failed: " << seal_status.message());
  REQUIRE(seal_status.ok());

  auto* segment = manager.GetSegment(segment_id.value());
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetState(), core::SegmentState::SEALED);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentManagerMultipleSegments") {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  // Create 3 segments
  auto seg1 = manager.CreateSegment(collection_id_, dimension_, metric_);
  auto seg2 = manager.CreateSegment(collection_id_, dimension_, metric_);
  auto seg3 = manager.CreateSegment(collection_id_, dimension_, metric_);

  REQUIRE(seg1.ok());
  REQUIRE(seg2.ok());
  REQUIRE(seg3.ok());

  CHECK_EQ(manager.GetSegmentCount(), 3);

  auto collection_segments = manager.GetCollectionSegments(collection_id_);
  CHECK_EQ(collection_segments.size(), 3);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentManagerDropSegment") {
  storage::SegmentManager manager(test_dir_, index_factory_.get());

  auto segment_id =
      manager.CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(segment_id.ok());

  CHECK_EQ(manager.GetSegmentCount(), 1);

  auto drop_status = manager.DropSegment(segment_id.value(), false);
  INFO("Drop failed: " << drop_status.message());
  REQUIRE(drop_status.ok());

  CHECK_EQ(manager.GetSegmentCount(), 0);
  CHECK_EQ(manager.GetSegment(segment_id.value()), nullptr);
}

// ============================================================================
// LocalStorage Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "LocalStorageCreate") {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  CHECK_EQ(storage.GetStorageSize(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "LocalStorageMetadata") {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  // Put metadata
  REQUIRE(storage.PutMetadata("key1", "value1").ok());
  REQUIRE(storage.PutMetadata("key2", "value2").ok());

  // Get metadata
  auto result1 = storage.GetMetadata("key1");
  REQUIRE(result1.ok());
  CHECK_EQ(result1.value(), "value1");

  auto result2 = storage.GetMetadata("key2");
  REQUIRE(result2.ok());
  CHECK_EQ(result2.value(), "value2");

  // Delete metadata
  REQUIRE(storage.DeleteMetadata("key1").ok());

  auto result3 = storage.GetMetadata("key1");
  CHECK_FALSE(result3.ok());
  CHECK(absl::IsNotFound(result3.status()));
}

TEST_CASE_FIXTURE(StorageTest, "LocalStorageSegmentOperations") {
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
  CHECK_FALSE(segment_result.ok());
  CHECK(absl::IsNotFound(segment_result.status()));
}

// ============================================================================
// StorageFactory Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "StorageFactoryLocalDisk") {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  auto storage_result = factory.CreateStorage(config);
  INFO("CreateStorage failed: " << storage_result.status().message());
  REQUIRE(storage_result.ok());

  CHECK_NE(storage_result.value(), nullptr);
}

TEST_CASE_FIXTURE(StorageTest, "StorageFactoryMemory") {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::MEMORY;
  config.base_path = "/tmp/gvdb_memory_test";

  auto storage_result = factory.CreateStorage(config);
  REQUIRE(storage_result.ok());

  CHECK_NE(storage_result.value(), nullptr);
}

TEST_CASE_FIXTURE(StorageTest, "StorageFactoryS3Unimplemented") {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.type = core::StorageConfig::Type::S3;
  config.base_path = "s3://bucket/path";

  auto storage_result = factory.CreateStorage(config);
  CHECK_FALSE(storage_result.ok());
  CHECK(absl::IsUnimplemented(storage_result.status()));
}

TEST_CASE_FIXTURE(StorageTest, "StorageFactoryInvalidConfig") {
  storage::StorageFactory factory(index_factory_.get());

  core::StorageConfig config;
  config.base_path = "";  // Invalid: empty path

  auto storage_result = factory.CreateStorage(config);
  CHECK_FALSE(storage_result.ok());
  CHECK(absl::IsInvalidArgument(storage_result.status()));
}

// ============================================================================
// Compaction Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "CompactNoSegments") {
  core::StorageConfig config;
  config.type = core::StorageConfig::Type::LOCAL_DISK;
  config.base_path = test_dir_;

  storage::LocalStorage storage(config, index_factory_.get());

  // Compacting with no segments should succeed as no-op
  auto compact_status = storage.Compact();
  CHECK(compact_status.ok());
}

// =============================================================================
// Get Vectors Tests
// =============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentGetVectors") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 10 vectors
  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Get existing vectors
  std::vector<core::VectorId> query_ids = {ids[0], ids[5], ids[9]};
  auto result = segment.GetVectors(query_ids, false);

  CHECK_EQ(result.found_ids.size(), 3);
  CHECK_EQ(result.found_vectors.size(), 3);
  CHECK_EQ(result.not_found_ids.size(), 0);

  // Verify vector content
  CHECK_EQ(result.found_ids[0], ids[0]);
  CHECK_EQ(result.found_vectors[0].dimension(), dimension_);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetVectorsPartialMatch") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 5 vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Query mix of existing and non-existing IDs
  std::vector<core::VectorId> query_ids = {
      ids[0],                      // exists
      core::MakeVectorId(999),     // doesn't exist
      ids[2],                      // exists
      core::MakeVectorId(888)      // doesn't exist
  };

  auto result = segment.GetVectors(query_ids, false);

  CHECK_EQ(result.found_ids.size(), 2);
  CHECK_EQ(result.found_vectors.size(), 2);
  CHECK_EQ(result.not_found_ids.size(), 2);

  // Verify found IDs
  CHECK_EQ(result.found_ids[0], ids[0]);
  CHECK_EQ(result.found_ids[1], ids[2]);

  // Verify not found IDs
  CHECK_EQ(result.not_found_ids[0], core::MakeVectorId(999));
  CHECK_EQ(result.not_found_ids[1], core::MakeVectorId(888));
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetVectorsWithMetadata") {
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

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Get vectors with metadata
  std::vector<core::VectorId> query_ids = {ids[1], ids[3]};
  auto result = segment.GetVectors(query_ids, true);

  CHECK_EQ(result.found_ids.size(), 2);
  CHECK_EQ(result.found_vectors.size(), 2);
  CHECK_EQ(result.found_metadata.size(), 2);
  CHECK_EQ(result.not_found_ids.size(), 0);

  // Verify metadata
  CHECK_EQ(result.found_metadata[0].size(), 2);
  CHECK_EQ(std::get<std::string>(result.found_metadata[0].at("name")), "vector_1");
  CHECK_EQ(std::get<std::string>(result.found_metadata[1].at("name")), "vector_3");
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetVectorsAllNotFound") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Query only non-existing IDs
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(999),
      core::MakeVectorId(888),
      core::MakeVectorId(777)
  };

  auto result = segment.GetVectors(query_ids, false);

  CHECK_EQ(result.found_ids.size(), 0);
  CHECK_EQ(result.found_vectors.size(), 0);
  CHECK_EQ(result.not_found_ids.size(), 3);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentGetVectorsEmpty") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // No vectors inserted

  // Query should return all as not found
  std::vector<core::VectorId> query_ids = {
      core::MakeVectorId(1),
      core::MakeVectorId(2)
  };

  auto result = segment.GetVectors(query_ids, false);

  CHECK_EQ(result.found_ids.size(), 0);
  CHECK_EQ(result.found_vectors.size(), 0);
  CHECK_EQ(result.not_found_ids.size(), 2);
}

// ============================================================================
// Delete Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentDeleteVectors") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 5 vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  CHECK_EQ(segment.GetVectorCount(), 5);

  // Delete 3 vectors
  std::vector<core::VectorId> delete_ids = {ids[0], ids[2], ids[4]};
  auto result = segment.DeleteVectors(delete_ids);

  REQUIRE(result.ok());
  CHECK_EQ(result->deleted_count, 3);
  CHECK_EQ(result->not_found_ids.size(), 0);
  CHECK_EQ(segment.GetVectorCount(), 2);

  // Verify remaining vectors
  auto read_result = segment.ReadVectors({ids[1], ids[3]});
  REQUIRE(read_result.ok());
  CHECK_EQ(read_result->size(), 2);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentDeleteVectorsPartialMatch") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 3 vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Try to delete mix of existing and non-existing IDs
  std::vector<core::VectorId> delete_ids = {
      ids[0],                      // exists
      core::MakeVectorId(999),     // doesn't exist
      ids[2],                      // exists
      core::MakeVectorId(888)      // doesn't exist
  };

  auto result = segment.DeleteVectors(delete_ids);

  REQUIRE(result.ok());
  CHECK_EQ(result->deleted_count, 2);
  CHECK_EQ(result->not_found_ids.size(), 2);
  CHECK_EQ(segment.GetVectorCount(), 1);

  // Verify not found IDs
  CHECK_EQ(result->not_found_ids[0], core::MakeVectorId(999));
  CHECK_EQ(result->not_found_ids[1], core::MakeVectorId(888));

  // Verify remaining vector
  auto read_result = segment.ReadVectors({ids[1]});
  REQUIRE(read_result.ok());
}

TEST_CASE_FIXTURE(StorageTest, "SegmentDeleteVectorsWithMetadata") {
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
  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  // Verify metadata exists
  auto meta_before = segment.GetMetadata(ids[0]);
  REQUIRE(meta_before.ok());

  // Delete vector
  auto result = segment.DeleteVectors({ids[0]});
  REQUIRE(result.ok());
  CHECK_EQ(result->deleted_count, 1);

  // Verify metadata is also deleted
  auto meta_after = segment.GetMetadata(ids[0]);
  CHECK_FALSE(meta_after.ok());
  CHECK_EQ(meta_after.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentDeleteVectorsAllNotFound") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert 3 vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Try to delete non-existent IDs
  std::vector<core::VectorId> delete_ids = {
      core::MakeVectorId(999),
      core::MakeVectorId(888),
      core::MakeVectorId(777)
  };

  auto result = segment.DeleteVectors(delete_ids);

  REQUIRE(result.ok());
  CHECK_EQ(result->deleted_count, 0);
  CHECK_EQ(result->not_found_ids.size(), 3);
  CHECK_EQ(segment.GetVectorCount(), 3);  // No vectors deleted
}

TEST_CASE_FIXTURE(StorageTest, "SegmentDeleteVectorsSealedState") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors
  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Seal the segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // Try to delete from sealed segment - should fail
  auto result = segment.DeleteVectors({ids[0]});

  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kFailedPrecondition);
}

// ============================================================================
// UpdateMetadata Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentUpdateMetadataReplace") {
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
  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata_vec).ok());

  // Update metadata (replace mode)
  core::Metadata new_metadata;
  new_metadata["price"] = core::MetadataValue(80.0);  // New price
  new_metadata["rating"] = core::MetadataValue(4.5);  // New field

  auto status = segment.UpdateMetadata(ids[0], new_metadata, false);
  REQUIRE(status.ok());

  // Verify metadata was replaced
  auto result = segment.GetMetadata(ids[0]);
  REQUIRE(result.ok());

  CHECK_EQ(result->size(), 2);  // Only 2 fields now (price, rating)
  CHECK_EQ(std::get<double>(result->at("price")), 80.0);
  CHECK_EQ(std::get<double>(result->at("rating")), 4.5);
  CHECK_EQ(result->find("brand"), result->end());  // Old field removed
  CHECK_EQ(result->find("in_stock"), result->end());  // Old field removed
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpdateMetadataMerge") {
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
  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata_vec).ok());

  // Update metadata (merge mode)
  core::Metadata update_metadata;
  update_metadata["price"] = core::MetadataValue(80.0);  // Update existing
  update_metadata["rating"] = core::MetadataValue(4.5);  // Add new

  auto status = segment.UpdateMetadata(ids[0], update_metadata, true);
  REQUIRE(status.ok());

  // Verify metadata was merged
  auto result = segment.GetMetadata(ids[0]);
  REQUIRE(result.ok());

  CHECK_EQ(result->size(), 4);  // All 4 fields present
  CHECK_EQ(std::get<double>(result->at("price")), 80.0);  // Updated
  CHECK_EQ(std::get<std::string>(result->at("brand")), "Nike");  // Preserved
  CHECK_EQ(std::get<bool>(result->at("in_stock")), true);  // Preserved
  CHECK_EQ(std::get<double>(result->at("rating")), 4.5);  // Added
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpdateMetadataNoExisting") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector without metadata
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Add metadata via update
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);
  metadata["brand"] = core::MetadataValue(std::string("Nike"));

  auto status = segment.UpdateMetadata(ids[0], metadata, true);
  REQUIRE(status.ok());

  // Verify metadata was added
  auto result = segment.GetMetadata(ids[0]);
  REQUIRE(result.ok());
  CHECK_EQ(result->size(), 2);
  CHECK_EQ(std::get<double>(result->at("price")), 100.0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpdateMetadataNotFound") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Try to update metadata for non-existent vector
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);

  auto status = segment.UpdateMetadata(core::MakeVectorId(999), metadata, false);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpdateMetadataSealedState") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vector
  auto vectors = CreateTestVectors(1);
  auto ids = CreateTestVectorIds(1);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Seal the segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result.value().release()).ok());

  // Try to update metadata in sealed segment
  core::Metadata metadata;
  metadata["price"] = core::MetadataValue(100.0);

  auto status = segment.UpdateMetadata(ids[0], metadata, false);

  CHECK_FALSE(status.ok());
  CHECK_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
}

// ========== Segment Serialization Tests ==========

TEST_CASE_FIXTURE(StorageTest, "SegmentSerializeDeserializeEmpty") {
  auto segment_id = core::MakeSegmentId(42);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Serialize empty segment
  auto serialize_result = segment.SerializeToBytes();
  INFO(serialize_result.status());
  REQUIRE(serialize_result.ok());

  const auto& bytes = serialize_result.value();
  CHECK_GT(bytes.size(), 0);

  // Deserialize
  auto deserialize_result = storage::Segment::DeserializeFromBytes(bytes);
  INFO(deserialize_result.status());
  REQUIRE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());
  CHECK_EQ(deserialized->GetId(), segment_id);
  CHECK_EQ(deserialized->GetCollectionId(), collection_id_);
  CHECK_EQ(deserialized->GetDimension(), dimension_);
  CHECK_EQ(deserialized->GetMetric(), metric_);
  CHECK_EQ(deserialized->GetVectorCount(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSerializeDeserializeWithVectors") {
  auto segment_id = core::MakeSegmentId(100);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(10);
  auto ids = CreateTestVectorIds(10);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  INFO(serialize_result.status());
  REQUIRE(serialize_result.ok());

  // Deserialize
  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  INFO(deserialize_result.status());
  REQUIRE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());

  // Verify metadata
  CHECK_EQ(deserialized->GetId(), segment_id);
  CHECK_EQ(deserialized->GetVectorCount(), 10);

  // Verify vectors match
  auto read_result = deserialized->ReadVectors(ids);
  REQUIRE(read_result.ok());
  REQUIRE_EQ(read_result->size(), 10);

  for (size_t i = 0; i < vectors.size(); ++i) {
    const auto& original = vectors[i];
    const auto& read = read_result->at(i);
    REQUIRE_EQ(original.size(), read.size());
    for (size_t j = 0; j < original.size(); ++j) {
      CHECK(original[j] == doctest::Approx(read[j]));
    }
  }
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSerializeDeserializeWithMetadata") {
  auto segment_id = core::MakeSegmentId(200);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Add metadata to each vector
  for (size_t i = 0; i < ids.size(); ++i) {
    core::Metadata metadata;
    metadata["index"] = core::MetadataValue(static_cast<int64_t>(i));
    metadata["price"] = core::MetadataValue(static_cast<double>(i * 10.5));
    metadata["name"] = core::MetadataValue(std::string("vector_") + std::to_string(i));
    metadata["active"] = core::MetadataValue(i % 2 == 0);

    REQUIRE(segment.UpdateMetadata(ids[i], metadata, false).ok());
  }

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  INFO(serialize_result.status());
  REQUIRE(serialize_result.ok());

  // Deserialize
  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  INFO(deserialize_result.status());
  REQUIRE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());

  // Verify metadata for each vector
  for (size_t i = 0; i < ids.size(); ++i) {
    auto metadata_result = deserialized->GetMetadata(ids[i]);
    REQUIRE(metadata_result.ok());
    const auto& metadata = *metadata_result;

    CHECK_EQ(std::get<int64_t>(metadata.at("index")), static_cast<int64_t>(i));
    CHECK(std::get<double>(metadata.at("price")) == doctest::Approx(i * 10.5));
    CHECK_EQ(std::get<std::string>(metadata.at("name")),
              std::string("vector_") + std::to_string(i));
    CHECK_EQ(std::get<bool>(metadata.at("active")), i % 2 == 0);
  }
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSerializeDeserializeLargeSegment") {
  auto segment_id = core::MakeSegmentId(300);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add many vectors
  auto vectors = CreateTestVectors(100);
  auto ids = CreateTestVectorIds(100);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Add metadata to subset
  for (size_t i = 0; i < 50; ++i) {
    core::Metadata metadata;
    metadata["batch"] = core::MetadataValue(static_cast<int64_t>(i / 10));
    metadata["score"] = core::MetadataValue(static_cast<double>(i) * 0.1);
    REQUIRE(segment.UpdateMetadata(ids[i], metadata, false).ok());
  }

  // Serialize
  auto serialize_result = segment.SerializeToBytes();
  INFO(serialize_result.status());
  REQUIRE(serialize_result.ok());

  // Should have reasonable size
  const auto& bytes = serialize_result.value();
  size_t expected_min_size = 100 * dimension_ * sizeof(float);  // Just vectors
  CHECK_GT(bytes.size(), expected_min_size);

  // Deserialize
  auto deserialize_result = storage::Segment::DeserializeFromBytes(bytes);
  INFO(deserialize_result.status());
  REQUIRE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());
  CHECK_EQ(deserialized->GetVectorCount(), 100);

  // Spot check a few vectors
  std::vector<core::VectorId> check_ids = {ids[0], ids[49], ids[99]};
  auto read_result = deserialized->ReadVectors(check_ids);
  REQUIRE(read_result.ok());
  CHECK_EQ(read_result->size(), 3);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentSerializeDeserializeStatePreservation") {
  auto segment_id = core::MakeSegmentId(400);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add vectors
  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Initial state should be GROWING
  CHECK_EQ(segment.GetState(), core::SegmentState::GROWING);

  // Serialize and deserialize
  auto serialize_result = segment.SerializeToBytes();
  REQUIRE(serialize_result.ok());

  auto deserialize_result =
      storage::Segment::DeserializeFromBytes(serialize_result.value());
  REQUIRE(deserialize_result.ok());

  auto deserialized = std::move(deserialize_result.value());

  // State should be preserved
  CHECK_EQ(deserialized->GetState(), core::SegmentState::GROWING);
  CHECK_EQ(deserialized->GetVectorCount(), 5);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentDeserializeInvalidData") {
  // Empty data
  std::string empty_bytes;
  auto result1 = storage::Segment::DeserializeFromBytes(empty_bytes);
  CHECK_FALSE(result1.ok());

  // Truncated data (just a few bytes)
  std::string truncated_bytes = "abcd";
  auto result2 = storage::Segment::DeserializeFromBytes(truncated_bytes);
  CHECK_FALSE(result2.ok());

  // Invalid header
  std::string invalid_bytes(100, 'x');  // Random data
  auto result3 = storage::Segment::DeserializeFromBytes(invalid_bytes);
  CHECK_FALSE(result3.ok());
}

// ============================================================================
// Persistence Round-Trip Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "FlushAndLoadSegmentWithVectors") {
  // Use SegmentManager for proper directory creation
  auto sm = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());

  auto seg_result = sm->CreateSegment(core::CollectionId(1), 4, core::MetricType::L2);
  REQUIRE(seg_result.ok());
  core::SegmentId seg_id = *seg_result;

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 1; i <= 5; ++i) {
    std::vector<float> data = {static_cast<float>(i), 0.0f, 0.0f, 0.0f};
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i));
  }
  REQUIRE(sm->WriteVectors(seg_id, vectors, ids).ok());

  // Seal and flush
  core::IndexConfig idx_cfg;
  idx_cfg.index_type = core::IndexType::FLAT;
  idx_cfg.dimension = 4;
  idx_cfg.metric_type = core::MetricType::L2;
  REQUIRE(sm->SealSegment(seg_id, idx_cfg).ok());
  REQUIRE(sm->FlushSegment(seg_id).ok());

  // Load from disk into a NEW segment
  auto loaded = storage::Segment::Load(test_dir_, seg_id);
  INFO(loaded.status().message());
  REQUIRE(loaded.ok());

  // Verify vectors survived the round-trip
  CHECK_EQ((*loaded)->GetVectorCount(), 5);
  CHECK_EQ((*loaded)->GetDimension(), 4);

  auto result = (*loaded)->GetVectors(ids, false);
  CHECK_EQ(result.found_ids.size(), 5);
  CHECK_EQ(result.not_found_ids.size(), 0);
}

TEST_CASE_FIXTURE(StorageTest, "LoadAllSegmentsRecovery") {
  // Create and flush a segment via SegmentManager
  auto sm1 = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());

  auto seg_result = sm1->CreateSegment(core::CollectionId(1), 4, core::MetricType::L2);
  REQUIRE(seg_result.ok());
  core::SegmentId seg_id = *seg_result;

  // Insert vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 1; i <= 3; ++i) {
    std::vector<float> data = {static_cast<float>(i), 1.0f, 2.0f, 3.0f};
    vectors.push_back(core::Vector(std::move(data)));
    ids.push_back(core::MakeVectorId(i));
  }
  REQUIRE(sm1->WriteVectors(seg_id, vectors, ids).ok());

  // Seal and flush
  core::IndexConfig idx_cfg;
  idx_cfg.index_type = core::IndexType::FLAT;
  idx_cfg.dimension = 4;
  idx_cfg.metric_type = core::MetricType::L2;
  REQUIRE(sm1->SealSegment(seg_id, idx_cfg).ok());
  REQUIRE(sm1->FlushSegment(seg_id).ok());

  // Destroy the old SegmentManager (simulates process restart)
  sm1.reset();

  // Create a new SegmentManager with the same base_path
  auto sm2 = std::make_shared<storage::SegmentManager>(test_dir_, index_factory_.get());
  CHECK_EQ(sm2->GetSegmentCount(), 0);  // Nothing loaded yet

  // Recover segments from disk
  REQUIRE(sm2->LoadAllSegments().ok());
  CHECK_EQ(sm2->GetSegmentCount(), 1);

  // Verify the recovered segment has the vectors
  auto* segment = sm2->GetSegment(seg_id);
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetVectorCount(), 3);

  auto result = segment->GetVectors(ids, false);
  CHECK_EQ(result.found_ids.size(), 3);

  // Verify search works after recovery (index was rebuilt)
  std::vector<float> query_data = {1.0f, 1.0f, 2.0f, 3.0f};
  core::Vector query(std::move(query_data));
  auto search_result = segment->Search(query, 2);
  INFO(search_result.status().message());
  REQUIRE(search_result.ok());
  CHECK_EQ(search_result->entries.size(), 2);
  CHECK_EQ(search_result->entries[0].id, core::MakeVectorId(1));
}

// ============================================================================
// Upsert Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentUpsertInsertNew") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(5);
  auto ids = CreateTestVectorIds(5);
  std::vector<core::Metadata> metadata(5);
  for (size_t i = 0; i < 5; ++i) {
    metadata[i]["name"] = std::string("vec_" + std::to_string(i));
  }

  auto result = segment.UpsertVectors(vectors, ids, metadata);
  REQUIRE(result.ok());
  CHECK_EQ(result->inserted_count, 5);
  CHECK_EQ(result->updated_count, 0);
  CHECK_EQ(segment.GetVectorCount(), 5);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpsertReplaceExisting") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors = CreateTestVectors(3);
  auto ids = CreateTestVectorIds(3);
  std::vector<core::Metadata> meta1(3);
  meta1[0]["label"] = std::string("original");

  REQUIRE(segment.UpsertVectors(vectors, ids, meta1).ok());
  CHECK_EQ(segment.GetVectorCount(), 3);

  // Upsert same IDs with new data
  auto new_vectors = CreateTestVectors(3);
  std::vector<core::Metadata> meta2(3);
  meta2[0]["label"] = std::string("updated");

  auto result = segment.UpsertVectors(new_vectors, ids, meta2);
  REQUIRE(result.ok());
  CHECK_EQ(result->inserted_count, 0);
  CHECK_EQ(result->updated_count, 3);
  CHECK_EQ(segment.GetVectorCount(), 3);

  // Verify metadata was updated
  auto md = segment.GetMetadata(ids[0]);
  REQUIRE(md.ok());
  auto label = std::get<std::string>(md->at("label"));
  CHECK_EQ(label, "updated");
}

TEST_CASE_FIXTURE(StorageTest, "SegmentUpsertMixed") {
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  auto vectors1 = CreateTestVectors(3);
  auto ids1 = CreateTestVectorIds(3, 1);  // IDs 1,2,3
  std::vector<core::Metadata> meta1(3);
  REQUIRE(segment.UpsertVectors(vectors1, ids1, meta1).ok());

  // Upsert: IDs 2,3 exist, 4,5 are new
  auto vectors2 = CreateTestVectors(4);
  auto ids2 = CreateTestVectorIds(4, 2);  // IDs 2,3,4,5
  std::vector<core::Metadata> meta2(4);

  auto result = segment.UpsertVectors(vectors2, ids2, meta2);
  REQUIRE(result.ok());
  CHECK_EQ(result->inserted_count, 2);
  CHECK_EQ(result->updated_count, 2);
  CHECK_EQ(segment.GetVectorCount(), 5);  // 1 + (2,3 replaced) + 4,5
}

// ============================================================================
// Range Search Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "SegmentRangeSearchGrowing") {
  dimension_ = 4;
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Create vectors at known distances from origin
  std::vector<core::Vector> vectors;
  vectors.push_back(core::Vector({0.1f, 0.0f, 0.0f, 0.0f}));  // dist ~0.01
  vectors.push_back(core::Vector({1.0f, 0.0f, 0.0f, 0.0f}));  // dist ~1.0
  vectors.push_back(core::Vector({5.0f, 0.0f, 0.0f, 0.0f}));  // dist ~25.0
  auto ids = CreateTestVectorIds(3);

  REQUIRE(segment.AddVectors(vectors, ids).ok());

  core::Vector query({0.0f, 0.0f, 0.0f, 0.0f});

  // Radius 2.0 (L2 distance) should find first two vectors
  auto result = segment.SearchRange(query, 2.0f);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 2);
  // Should be sorted by distance
  CHECK_LT(result->entries[0].distance, result->entries[1].distance);
}

TEST_CASE_FIXTURE(StorageTest, "SegmentRangeSearchSealed") {
  dimension_ = 4;
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  std::vector<core::Vector> vectors;
  vectors.push_back(core::Vector({0.1f, 0.0f, 0.0f, 0.0f}));
  vectors.push_back(core::Vector({1.0f, 0.0f, 0.0f, 0.0f}));
  vectors.push_back(core::Vector({5.0f, 0.0f, 0.0f, 0.0f}));
  auto ids = CreateTestVectorIds(3);

  REQUIRE(segment.AddVectors(vectors, ids).ok());

  // Seal segment
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  auto index_result = index_factory_->CreateIndex(config);
  REQUIRE(index_result.ok());
  REQUIRE(segment.Seal(index_result->release()).ok());

  core::Vector query({0.0f, 0.0f, 0.0f, 0.0f});
  auto result = segment.SearchRange(query, 2.0f);
  REQUIRE(result.ok());
  CHECK_GE(result->entries.size(), 1);  // At least the close vector
}

TEST_CASE_FIXTURE(StorageTest, "SegmentRangeSearchMaxResults") {
  dimension_ = 4;
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Add 10 vectors all very close to origin
  std::vector<core::Vector> vectors;
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(core::Vector({0.01f * (i + 1), 0.0f, 0.0f, 0.0f}));
  }
  auto ids = CreateTestVectorIds(10);
  REQUIRE(segment.AddVectors(vectors, ids).ok());

  core::Vector query({0.0f, 0.0f, 0.0f, 0.0f});
  // All within radius 1.0, but limit to 3
  auto result = segment.SearchRange(query, 1.0f, 3);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 3);
}

// ============================================================================
// Scalar Index Tests
// ============================================================================

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexBasicLookup") {
  storage::ScalarIndex idx;

  idx.Add(1, core::MetadataValue(std::string("Nike")));
  idx.Add(2, core::MetadataValue(std::string("Adidas")));
  idx.Add(3, core::MetadataValue(std::string("Nike")));

  auto result = idx.LookupEqual(core::MetadataValue(std::string("Nike")));
  CHECK_EQ(result.size(), 2);
  CHECK(result.count(1));
  CHECK(result.count(3));
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexRangeLookup") {
  storage::ScalarIndex idx;

  idx.Add(1, core::MetadataValue(int64_t(10)));
  idx.Add(2, core::MetadataValue(int64_t(50)));
  idx.Add(3, core::MetadataValue(int64_t(100)));
  idx.Add(4, core::MetadataValue(int64_t(200)));

  core::MetadataValue max_val(int64_t(100));
  auto result = idx.LookupRange(nullptr, &max_val);
  CHECK_GE(result.size(), 3);  // IDs 1,2,3 (value <= 100)
  CHECK(result.count(1));
  CHECK(result.count(2));
  CHECK(result.count(3));
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexInLookup") {
  storage::ScalarIndex idx;

  idx.Add(1, core::MetadataValue(std::string("shoes")));
  idx.Add(2, core::MetadataValue(std::string("electronics")));
  idx.Add(3, core::MetadataValue(std::string("clothing")));
  idx.Add(4, core::MetadataValue(std::string("shoes")));

  std::vector<core::MetadataValue> values = {
      core::MetadataValue(std::string("shoes")),
      core::MetadataValue(std::string("clothing"))};
  auto result = idx.LookupIn(values);
  CHECK_EQ(result.size(), 3);  // IDs 1,3,4
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexPrefixLookup") {
  storage::ScalarIndex idx;

  idx.Add(1, core::MetadataValue(std::string("Nike Air Max")));
  idx.Add(2, core::MetadataValue(std::string("Nike Dunk")));
  idx.Add(3, core::MetadataValue(std::string("Adidas Ultra")));

  auto result = idx.LookupPrefix("Nike");
  CHECK_EQ(result.size(), 2);
  CHECK(result.count(1));
  CHECK(result.count(2));
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexSetBuildAndEvaluate") {
  storage::ScalarIndexSet idx_set;

  std::unordered_map<uint64_t, core::Metadata> metadata_map;
  metadata_map[1] = {{"brand", core::MetadataValue(std::string("Nike"))},
                     {"price", core::MetadataValue(int64_t(100))}};
  metadata_map[2] = {{"brand", core::MetadataValue(std::string("Adidas"))},
                     {"price", core::MetadataValue(int64_t(150))}};
  metadata_map[3] = {{"brand", core::MetadataValue(std::string("Nike"))},
                     {"price", core::MetadataValue(int64_t(200))}};

  idx_set.BuildFromMetadata(metadata_map);
  CHECK(idx_set.HasIndexes());

  // Test: brand = 'Nike'
  core::ComparisonNode filter("brand", core::ComparisonOp::EQUAL,
                               core::MetadataValue(std::string("Nike")));
  auto result = idx_set.Evaluate(filter);
  REQUIRE(result.has_value());
  CHECK_EQ(result->size(), 2);
  CHECK(result->count(1));
  CHECK(result->count(3));
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexSearchWithFilterAcceleration") {
  dimension_ = 4;
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  // Insert vectors with metadata
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  std::vector<core::Metadata> metadata;

  for (int i = 0; i < 20; ++i) {
    vectors.push_back(core::Vector({
        static_cast<float>(i) * 0.1f, 0.0f, 0.0f, 0.0f}));
    ids.push_back(core::MakeVectorId(i + 1));
    core::Metadata md;
    md["category"] = (i < 10) ? core::MetadataValue(std::string("shoes"))
                               : core::MetadataValue(std::string("electronics"));
    md["price"] = core::MetadataValue(int64_t(50 + i * 10));
    metadata.push_back(md);
  }

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  core::Vector query({0.0f, 0.0f, 0.0f, 0.0f});

  // Search with filter: category = 'shoes' — scalar index narrows to 10 vectors
  auto result = segment.SearchWithFilter(query, 5, "category = 'shoes'");
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 5);

  // All returned vectors should be in shoes category (IDs 1-10)
  for (const auto& entry : result->entries) {
    CHECK_LE(core::ToUInt64(entry.id), 10);
  }
}

TEST_CASE_FIXTURE(StorageTest, "ScalarIndexAndFilter") {
  dimension_ = 4;
  auto segment_id = core::MakeSegmentId(1);
  storage::Segment segment(segment_id, collection_id_, dimension_, metric_);

  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  std::vector<core::Metadata> metadata;

  for (int i = 0; i < 10; ++i) {
    vectors.push_back(core::Vector({
        static_cast<float>(i) * 0.1f, 0.0f, 0.0f, 0.0f}));
    ids.push_back(core::MakeVectorId(i + 1));
    core::Metadata md;
    md["brand"] = (i % 2 == 0)
                      ? core::MetadataValue(std::string("Nike"))
                      : core::MetadataValue(std::string("Adidas"));
    md["price"] = core::MetadataValue(int64_t(100 + i * 50));
    metadata.push_back(md);
  }

  REQUIRE(segment.AddVectorsWithMetadata(vectors, ids, metadata).ok());

  core::Vector query({0.0f, 0.0f, 0.0f, 0.0f});

  // AND filter: brand = 'Nike' AND price < 300
  auto result = segment.SearchWithFilter(query, 10,
      "brand = 'Nike' AND price < 300");
  REQUIRE(result.ok());
  // Nike: IDs 1,3,5,7,9 (even indices 0,2,4,6,8)
  // Price < 300: IDs with price 100,150,200,250 (indices 0,1,2,3)
  // Intersection (Nike AND price < 300): indices 0,2 -> IDs 1,3
  CHECK_GE(result->entries.size(), 2);
}
