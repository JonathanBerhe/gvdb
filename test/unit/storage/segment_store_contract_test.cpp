// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Contract tests for ISegmentStore. Each case runs against SegmentManager
// (local disk only) and TieredSegmentManager (local disk with an object
// store behind it), so the two stores cannot drift apart. Behavior that only
// one store has stays in storage_test.cpp and tiered_segment_manager_test.cpp.

#include <doctest/doctest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "index/index_factory.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_cache.h"
#include "storage/segment/segment_manager.h"
#include "storage/segment/segment_store.h"
#include "storage/segment/tiered_segment_manager.h"
#include "support/temp_dir.h"
#include "support/vectors.h"

namespace gvdb::storage {
namespace {

// Each fixture owns its directories and can reopen the store on them, which
// is what a process restart does.
struct SegmentManagerFixture {
  SegmentManagerFixture() { Reopen(); }

  ISegmentStore& store() { return *store_; }

  void Reopen() {
    store_.reset();
    store_ = std::make_unique<SegmentManager>(local_dir_, index_factory_.get());
  }

  test::ScopedTempDir temp_dir_{"gvdb_segment_store_contract"};
  const std::string local_dir_ = temp_dir_.str();
  std::unique_ptr<index::IndexFactory> index_factory_ = std::make_unique<index::IndexFactory>();
  std::unique_ptr<ISegmentStore> store_;
};

// The object store is a fresh in-memory store on each open, so a reopened
// store finds its segments on local disk.
struct TieredSegmentManagerFixture {
  TieredSegmentManagerFixture() {
    std::filesystem::create_directories(local_dir_);
    Reopen();
  }

  ISegmentStore& store() { return *store_; }

  void Reopen() {
    store_.reset();
    auto local = std::make_unique<SegmentManager>(local_dir_, index_factory_.get());
    auto cache = std::make_unique<SegmentCache>(cache_dir_, 256 * 1024 * 1024);
    // A healthy in-memory store never fails an upload, so no retry runs and
    // the retry delay does not matter.
    store_ = std::make_unique<TieredSegmentManager>(
        std::move(local), std::make_unique<InMemoryObjectStore>(), std::move(cache), "test-prefix",
        /*upload_threads=*/1, std::chrono::seconds(3));
  }

  test::ScopedTempDir temp_dir_{"gvdb_tiered_segment_store_contract"};
  const std::string local_dir_ = (temp_dir_ / "local").string();
  const std::string cache_dir_ = (temp_dir_ / "cache").string();
  std::unique_ptr<index::IndexFactory> index_factory_ = std::make_unique<index::IndexFactory>();
  std::unique_ptr<ISegmentStore> store_;
};

constexpr core::Dimension kDimension = 128;
constexpr core::MetricType kMetric = core::MetricType::L2;
const core::CollectionId kCollection{1};

core::IndexConfig FlatIndexConfig(core::Dimension dimension = kDimension) {
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension;
  config.metric_type = kMetric;
  return config;
}

}  // namespace
}  // namespace gvdb::storage

TYPE_TO_STRING_AS("SegmentManager", gvdb::storage::SegmentManagerFixture);
TYPE_TO_STRING_AS("TieredSegmentManager", gvdb::storage::TieredSegmentManagerFixture);

namespace gvdb::storage {
namespace {

#define SEGMENT_STORE_FIXTURES SegmentManagerFixture, TieredSegmentManagerFixture

TEST_CASE_TEMPLATE("SegmentStore contract: CreateSegment adds a growing segment", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  auto segment_id = f.store().CreateSegment(kCollection, kDimension, kMetric);
  REQUIRE(segment_id.ok());
  CHECK_EQ(f.store().GetSegmentCount(), 1);

  auto* segment = f.store().GetSegment(*segment_id);
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetState(), core::SegmentState::GROWING);
}

TEST_CASE_TEMPLATE("SegmentStore contract: WriteVectors then ReadVectors", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  auto segment_id = *f.store().CreateSegment(kCollection, kDimension, kMetric);
  auto vectors = test::RandomVectors(50, kDimension, 42);
  auto ids = test::SequentialIds(50);
  REQUIRE(f.store().WriteVectors(segment_id, vectors, ids).ok());

  auto* segment = f.store().GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  auto read =
      segment->ReadVectors({core::MakeVectorId(1), core::MakeVectorId(25), core::MakeVectorId(50)});
  REQUIRE(read.ok());
  CHECK_EQ(read->size(), 3);
}

TEST_CASE_TEMPLATE("SegmentStore contract: SealSegment makes the segment sealed and searchable", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  auto segment_id = *f.store().CreateSegment(kCollection, kDimension, kMetric);
  auto vectors = test::RandomVectors(100, kDimension, 42);
  REQUIRE(f.store().WriteVectors(segment_id, vectors, test::SequentialIds(100)).ok());
  REQUIRE(f.store().SealSegment(segment_id, FlatIndexConfig()).ok());

  auto* segment = f.store().GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetState(), core::SegmentState::SEALED);

  auto result = segment->Search(vectors[0], 5);
  REQUIRE(result.ok());
  REQUIRE_FALSE(result->entries.empty());
  CHECK_EQ(result->entries[0].id, core::MakeVectorId(1));
}

TEST_CASE_TEMPLATE("SegmentStore contract: SearchCollection finds a vector of the collection", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  auto segment_id = *f.store().CreateSegment(kCollection, kDimension, kMetric);
  auto vectors = test::RandomVectors(50, kDimension, 42);
  REQUIRE(f.store().WriteVectors(segment_id, vectors, test::SequentialIds(50)).ok());
  REQUIRE(f.store().SealSegment(segment_id, FlatIndexConfig()).ok());

  auto result = f.store().SearchCollection(kCollection, vectors[0], 5);
  REQUIRE(result.ok());
  REQUIRE_FALSE(result->entries.empty());
  CHECK_EQ(result->entries[0].id, core::MakeVectorId(1));
}

TEST_CASE_TEMPLATE("SegmentStore contract: GetCollectionSegments lists each collection", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  const core::CollectionId other{2};
  for (int i = 0; i < 3; ++i) {
    REQUIRE(f.store().CreateSegment(kCollection, kDimension, kMetric).ok());
  }
  REQUIRE(f.store().CreateSegment(other, kDimension, kMetric).ok());

  CHECK_EQ(f.store().GetSegmentCount(), 4);
  CHECK_EQ(f.store().GetCollectionSegments(kCollection).size(), 3);
  CHECK_EQ(f.store().GetCollectionSegments(other).size(), 1);
}

TEST_CASE_TEMPLATE("SegmentStore contract: DropSegment removes the segment", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  auto segment_id = *f.store().CreateSegment(kCollection, kDimension, kMetric);
  CHECK_EQ(f.store().GetSegmentCount(), 1);

  REQUIRE(f.store().DropSegment(segment_id).ok());
  CHECK_EQ(f.store().GetSegmentCount(), 0);
  CHECK_EQ(f.store().GetSegment(segment_id), nullptr);
}

TEST_CASE_TEMPLATE("SegmentStore contract: Clear removes every segment", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  REQUIRE(f.store().CreateSegment(kCollection, kDimension, kMetric).ok());
  REQUIRE(f.store().CreateSegment(core::CollectionId(2), kDimension, kMetric).ok());
  CHECK_EQ(f.store().GetSegmentCount(), 2);

  f.store().Clear();
  CHECK_EQ(f.store().GetSegmentCount(), 0);
}

TEST_CASE_TEMPLATE("SegmentStore contract: LoadAllSegments recovers a flushed segment", F,
                   SEGMENT_STORE_FIXTURES) {
  F f;
  constexpr core::Dimension kSmall = 4;
  auto segment_id = *f.store().CreateSegment(kCollection, kSmall, kMetric);

  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  for (int i = 1; i <= 3; ++i) {
    vectors.emplace_back(std::vector<float>{static_cast<float>(i), 1.0F, 2.0F, 3.0F});
    ids.push_back(core::MakeVectorId(i));
  }
  REQUIRE(f.store().WriteVectors(segment_id, vectors, ids).ok());
  REQUIRE(f.store().SealSegment(segment_id, FlatIndexConfig(kSmall)).ok());
  REQUIRE(f.store().FlushSegment(segment_id).ok());

  // A restart: a new store on the same directories loads nothing until
  // LoadAllSegments runs.
  f.Reopen();
  CHECK_EQ(f.store().GetSegmentCount(), 0);
  REQUIRE(f.store().LoadAllSegments().ok());
  CHECK_EQ(f.store().GetSegmentCount(), 1);

  auto* segment = f.store().GetSegment(segment_id);
  REQUIRE_NE(segment, nullptr);
  CHECK_EQ(segment->GetVectorCount(), 3);
  CHECK_EQ(segment->GetVectors(ids, false).found_ids.size(), 3);

  // The index is rebuilt, so search works after the load.
  auto result = segment->Search(core::Vector(std::vector<float>{1.0F, 1.0F, 2.0F, 3.0F}), 2);
  REQUIRE(result.ok());
  REQUIRE_EQ(result->entries.size(), 2);
  CHECK_EQ(result->entries[0].id, core::MakeVectorId(1));
}

#undef SEGMENT_STORE_FIXTURES

}  // namespace
}  // namespace gvdb::storage
