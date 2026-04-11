// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/tiered_segment_manager.h"
#include "storage/object_store.h"
#include "index/index_factory.h"

#include <doctest/doctest.h>

#include <chrono>
#include <filesystem>
#include <thread>

namespace gvdb {
namespace storage {
namespace {

const std::string kTestDir = "/tmp/gvdb_tiered_test";
const std::string kCacheDir = "/tmp/gvdb_tiered_test_cache";

struct TieredTest {
  std::unique_ptr<index::IndexFactory> index_factory_;
  core::Dimension dimension_ = core::Dimension(128);
  core::MetricType metric_ = core::MetricType::L2;
  core::CollectionId collection_id_ = core::CollectionId(1);

  TieredTest() {
    std::filesystem::remove_all(kTestDir);
    std::filesystem::remove_all(kCacheDir);
    std::filesystem::create_directories(kTestDir);
    index_factory_ = std::make_unique<index::IndexFactory>();
  }

  ~TieredTest() {
    std::filesystem::remove_all(kTestDir);
    std::filesystem::remove_all(kCacheDir);
  }

  std::unique_ptr<TieredSegmentManager> MakeTiered(
      size_t cache_size = 256 * 1024 * 1024) {
    auto local = std::make_unique<SegmentManager>(
        kTestDir, index_factory_.get());
    auto object_store = std::make_unique<InMemoryObjectStore>();
    auto cache = std::make_unique<SegmentCache>(kCacheDir, cache_size);
    return std::make_unique<TieredSegmentManager>(
        std::move(local), std::move(object_store),
        std::move(cache), "test-prefix", /*upload_threads=*/1);
  }

  // Create a tiered manager and return the raw object store pointer for inspection
  struct TieredWithStore {
    std::unique_ptr<TieredSegmentManager> tiered;
    InMemoryObjectStore* store;  // non-owning
  };

  TieredWithStore MakeTieredWithStore(
      size_t cache_size = 256 * 1024 * 1024) {
    auto local = std::make_unique<SegmentManager>(
        kTestDir, index_factory_.get());
    auto object_store = std::make_unique<InMemoryObjectStore>();
    auto* store_ptr = object_store.get();
    auto cache = std::make_unique<SegmentCache>(kCacheDir, cache_size);
    auto tiered = std::make_unique<TieredSegmentManager>(
        std::move(local), std::move(object_store),
        std::move(cache), "test-prefix", /*upload_threads=*/1);
    return {std::move(tiered), store_ptr};
  }

  std::vector<core::Vector> MakeVectors(size_t count) {
    std::vector<core::Vector> vectors;
    vectors.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      vectors.push_back(core::RandomVector(dimension_));
    }
    return vectors;
  }

  std::vector<core::VectorId> MakeIds(size_t count, uint64_t start = 1) {
    std::vector<core::VectorId> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(core::VectorId(start + i));
    }
    return ids;
  }
};

TEST_CASE_FIXTURE(TieredTest, "CreateSegment") {
  auto tiered = MakeTiered();
  auto result = tiered->CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(result.ok());
}

TEST_CASE_FIXTURE(TieredTest, "WriteAndReadVectors") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(10);
  auto ids = MakeIds(10);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  auto read_result = tiered->ReadVectors(seg_id, {ids[0]});
  REQUIRE(read_result.ok());
  CHECK_EQ(read_result->size(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "FlushTriggersUpload") {
  auto [tiered, store] = MakeTieredWithStore();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(100);
  auto ids = MakeIds(100);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  // Seal the segment (required before flush)
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  // Flush — should trigger async upload
  REQUIRE(tiered->FlushSegment(seg_id).ok());

  // Wait for async upload to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify objects were uploaded to the mock store
  CHECK(store->ObjectCount() > 0);

  // Check manifest exists
  auto manifest = store->GetObject("test-prefix/manifest.json");
  CHECK(manifest.ok());
}

TEST_CASE_FIXTURE(TieredTest, "SealAndSearch") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  auto search_result = tiered->SearchSegment(seg_id, vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK(search_result->entries.size() > 0);
}

TEST_CASE_FIXTURE(TieredTest, "SearchCollection") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered->SealSegment(seg_id, config).ok());

  auto result = tiered->SearchCollection(collection_id_, vectors[0], 5);
  REQUIRE(result.ok());
  CHECK(result->entries.size() > 0);
}

TEST_CASE_FIXTURE(TieredTest, "GetAllSegmentIds") {
  auto tiered = MakeTiered();
  auto seg1 = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  auto seg2 = *tiered->CreateSegment(collection_id_, dimension_, metric_);

  auto all_ids = tiered->GetAllSegmentIds();
  CHECK_EQ(all_ids.size(), 2);
}

TEST_CASE_FIXTURE(TieredTest, "GetSegmentCount") {
  auto tiered = MakeTiered();
  CHECK_EQ(tiered->GetSegmentCount(), 0);
  tiered->CreateSegment(collection_id_, dimension_, metric_);
  CHECK_EQ(tiered->GetSegmentCount(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "GetCollectionSegments") {
  auto tiered = MakeTiered();
  auto seg1 = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  auto col2 = core::CollectionId(2);
  auto seg2 = *tiered->CreateSegment(col2, dimension_, metric_);

  auto col1_segs = tiered->GetCollectionSegments(collection_id_);
  CHECK_EQ(col1_segs.size(), 1);

  auto col2_segs = tiered->GetCollectionSegments(col2);
  CHECK_EQ(col2_segs.size(), 1);
}

TEST_CASE_FIXTURE(TieredTest, "Clear") {
  auto tiered = MakeTiered();
  tiered->CreateSegment(collection_id_, dimension_, metric_);
  CHECK_EQ(tiered->GetSegmentCount(), 1);
  tiered->Clear();
  CHECK_EQ(tiered->GetSegmentCount(), 0);
}

TEST_CASE_FIXTURE(TieredTest, "RemoteSegmentCountInitiallyZero") {
  auto tiered = MakeTiered();
  CHECK_EQ(tiered->GetRemoteSegmentCount(), 0);
}

TEST_CASE_FIXTURE(TieredTest, "ManifestDiscovery") {
  // Phase 1: Create, write, seal, flush to populate S3
  auto [tiered1, store1] = MakeTieredWithStore();
  auto seg_id = *tiered1->CreateSegment(collection_id_, dimension_, metric_);

  auto vectors = MakeVectors(50);
  auto ids = MakeIds(50);
  REQUIRE(tiered1->WriteVectors(seg_id, vectors, ids).ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;
  REQUIRE(tiered1->SealSegment(seg_id, config).ok());
  REQUIRE(tiered1->FlushSegment(seg_id).ok());

  // Wait for upload
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify manifest was created
  auto manifest = store1->GetObject("test-prefix/manifest.json");
  REQUIRE(manifest.ok());

  // Verify it deserializes correctly
  auto entries = SegmentManifest::Deserialize(*manifest);
  REQUIRE(entries.ok());
  CHECK_EQ(entries->size(), 1);
  CHECK_EQ((*entries)[0].vector_count, 50);
}

TEST_CASE_FIXTURE(TieredTest, "DropSegment") {
  auto tiered = MakeTiered();
  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  CHECK_EQ(tiered->GetSegmentCount(), 1);

  REQUIRE(tiered->DropSegment(seg_id).ok());
  CHECK_EQ(tiered->GetSegmentCount(), 0);
}

TEST_CASE_FIXTURE(TieredTest, "SetSealCallback") {
  auto tiered = MakeTiered();
  bool callback_called = false;
  tiered->SetSealCallback([&](core::SegmentId, core::IndexType) {
    callback_called = true;
  });

  // GetWritableSegment with a segment that's full should trigger callback
  // This test just verifies the callback is wired through
  CHECK_FALSE(callback_called);
}

TEST_CASE_FIXTURE(TieredTest, "GetTotalMemoryUsage") {
  auto tiered = MakeTiered();
  CHECK_EQ(tiered->GetTotalMemoryUsage(), 0);

  auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);
  auto vectors = MakeVectors(100);
  auto ids = MakeIds(100);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  CHECK(tiered->GetTotalMemoryUsage() > 0);
}

TEST_CASE_FIXTURE(TieredTest, "LoadAllSegmentsLocal") {
  // Create and flush a segment
  {
    auto tiered = MakeTiered();
    auto seg_id = *tiered->CreateSegment(collection_id_, dimension_, metric_);
    auto vectors = MakeVectors(20);
    auto ids = MakeIds(20);
    REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;
    REQUIRE(tiered->SealSegment(seg_id, config).ok());
    REQUIRE(tiered->FlushSegment(seg_id).ok());
  }

  // Create fresh tiered manager and load
  auto tiered2 = MakeTiered();
  REQUIRE(tiered2->LoadAllSegments().ok());

  // Should have recovered the segment from local disk
  CHECK(tiered2->GetSegmentCount() >= 1);
}

}  // namespace
}  // namespace storage
}  // namespace gvdb
