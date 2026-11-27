// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <filesystem>

#include "cluster/datanode/data_node.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/objstore/object_store.h"
#include "storage/segment/segment_cache.h"
#include "storage/segment/segment_manager.h"
#include "storage/segment/tiered_segment_manager.h"
#include "support/temp_dir.h"
#include "support/vectors.h"
#include "support/wait.h"

namespace {

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// DataNode Tests
// ============================================================================

class DataNodeTest {
 public:
  DataNodeTest() {
    auto factory = std::make_unique<index::IndexFactory>();
    factory_ptr_ = factory.get();
    segment_store_ = std::make_shared<storage::SegmentManager>(test_dir_.str(), factory_ptr_);
    data_node_ = std::make_unique<DataNode>(std::move(factory), segment_store_);

    collection_id_ = core::MakeCollectionId(1);
    dimension_ = 32;
    metric_ = core::MetricType::L2;
  }

  // Create a GROWING segment with vectors
  core::SegmentId CreateSegmentWithVectors(size_t count) {
    auto seg_result = segment_store_->CreateSegment(collection_id_, dimension_, metric_);
    auto seg_id = *seg_result;

    auto vectors = gvdb::test::RandomVectors(count, dimension_);
    auto ids = gvdb::test::SequentialIds(count, next_id_);
    next_id_ += count;

    auto* segment = segment_store_->GetSegment(seg_id);
    REQUIRE(segment->AddVectors(vectors, ids).ok());
    return seg_id;
  }

  // Declared first, so the folder is removed after every other member is gone.
  gvdb::test::ScopedTempDir test_dir_{"gvdb_data_node_test"};
  index::IndexFactory* factory_ptr_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::unique_ptr<DataNode> data_node_;
  core::CollectionId collection_id_;
  core::Dimension dimension_;
  core::MetricType metric_;
  uint64_t next_id_ = 1;
};

// ============================================================================
// Task Queue Tests
// ============================================================================

TEST_CASE_FIXTURE(DataNodeTest, "ScheduleBuildTask returns OK") {
  BuildTask task{core::MakeSegmentId(1), core::IndexType::HNSW, 10};
  auto status = data_node_->ScheduleBuildTask(task);
  CHECK(status.ok());
}

TEST_CASE_FIXTURE(DataNodeTest, "GetPendingTaskCount starts at zero") {
  CHECK_EQ(data_node_->GetPendingTaskCount(), 0);
}

TEST_CASE_FIXTURE(DataNodeTest, "HasPendingTasks false when empty") {
  CHECK_FALSE(data_node_->HasPendingTasks());
}

TEST_CASE_FIXTURE(DataNodeTest, "HasPendingTasks true after scheduling") {
  BuildTask task{core::MakeSegmentId(1), core::IndexType::FLAT, 5};
  REQUIRE(data_node_->ScheduleBuildTask(task).ok());
  CHECK(data_node_->HasPendingTasks());
}

TEST_CASE_FIXTURE(DataNodeTest, "MultipleTasks accumulate") {
  for (uint32_t i = 1; i <= 5; ++i) {
    BuildTask task{core::MakeSegmentId(i), core::IndexType::HNSW, uint64_t{i} * 10};
    REQUIRE(data_node_->ScheduleBuildTask(task).ok());
  }
  CHECK_EQ(data_node_->GetPendingTaskCount(), 5);
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildTask priority ordering") {
  BuildTask low{core::MakeSegmentId(1), core::IndexType::FLAT, 1};
  BuildTask high{core::MakeSegmentId(3), core::IndexType::FLAT, 100};
  CHECK(low < high);
  CHECK_FALSE(high < low);
}

// ============================================================================
// BuildIndex Tests
// ============================================================================

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex seals growing segment") {
  auto seg_id = CreateSegmentWithVectors(50);
  auto* segment = segment_store_->GetSegment(seg_id);
  CHECK_EQ(segment->GetState(), core::SegmentState::GROWING);

  auto status = data_node_->BuildIndex(seg_id, core::IndexType::FLAT);
  INFO("BuildIndex failed: " << status.message());
  REQUIRE(status.ok());

  CHECK_EQ(segment->GetState(), core::SegmentState::SEALED);
  CHECK_EQ(segment->GetVectorCount(), 50);
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex with HNSW") {
  auto seg_id = CreateSegmentWithVectors(100);

  auto status = data_node_->BuildIndex(seg_id, core::IndexType::HNSW);
  REQUIRE(status.ok());

  auto* segment = segment_store_->GetSegment(seg_id);
  CHECK_EQ(segment->GetState(), core::SegmentState::SEALED);

  // Verify search works on sealed segment
  auto query = gvdb::test::RandomVectors(1, dimension_, /*seed=*/7).front();
  auto result = segment->Search(query, 5);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 5);
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex rejects empty segment") {
  auto seg_result = segment_store_->CreateSegment(collection_id_, dimension_, metric_);
  auto seg_id = *seg_result;

  auto status = data_node_->BuildIndex(seg_id, core::IndexType::FLAT);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsFailedPrecondition(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex rejects already sealed") {
  auto seg_id = CreateSegmentWithVectors(50);
  REQUIRE(data_node_->BuildIndex(seg_id, core::IndexType::FLAT).ok());

  // Try to build again
  auto status = data_node_->BuildIndex(seg_id, core::IndexType::FLAT);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsFailedPrecondition(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex rejects nonexistent segment") {
  auto status = data_node_->BuildIndex(core::MakeSegmentId(999), core::IndexType::FLAT);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

// Regression: BuildIndex must flush the sealed segment so tiered storage
// uploads it to the object store. Sealing alone leaves the segment memory-only;
// the cold tier stays empty (segments never reach S3/GCS/filesystem).
TEST_CASE("BuildIndex flushes sealed segment to the tiered object store") {
  // Declared first, so the folder is removed after the data node and the
  // tiered store are gone.
  gvdb::test::ScopedTempDir tmp("gvdb_data_node_tiered_test");
  const std::string dir = (tmp / "segments").string();
  const std::string cache_dir = (tmp / "cache").string();
  std::filesystem::create_directories(dir);

  auto factory = std::make_unique<index::IndexFactory>();
  auto* factory_ptr = factory.get();

  auto local = std::make_unique<storage::SegmentManager>(dir, factory_ptr);
  auto object_store = std::make_unique<storage::InMemoryObjectStore>();
  auto* store = object_store.get();  // non-owning, for inspection
  auto cache = std::make_unique<storage::SegmentCache>(cache_dir, 256 * 1024 * 1024);
  std::shared_ptr<storage::ISegmentStore> tiered = std::make_shared<storage::TieredSegmentManager>(
      std::move(local), std::move(object_store), std::move(cache), "test-prefix",
      /*upload_threads=*/1);

  cluster::DataNode data_node(std::move(factory), tiered);

  auto collection_id = core::MakeCollectionId(1);
  auto dimension = static_cast<core::Dimension>(32);
  auto seg_id = *tiered->CreateSegment(collection_id, dimension, core::MetricType::L2);
  auto vectors = gvdb::test::RandomVectors(50, dimension);
  auto ids = gvdb::test::SequentialIds(50);
  REQUIRE(tiered->WriteVectors(seg_id, vectors, ids).ok());

  // Before BuildIndex, nothing has been uploaded.
  CHECK_EQ(store->ObjectCount(), 0);

  REQUIRE(data_node.BuildIndex(seg_id, core::IndexType::FLAT).ok());

  // BuildIndex seals + flushes; the flush schedules an async upload. Wait
  // until the upload has written the segment files and the manifest.
  REQUIRE(gvdb::test::WaitFor([store] {
    return store->ObjectCount() > 0 && store->GetObject("test-prefix/manifest.json").ok();
  }));

  CHECK(store->ObjectCount() > 0);
  CHECK(store->GetObject("test-prefix/manifest.json").ok());
}

// ============================================================================
// ProcessBuildQueue Tests
// ============================================================================

TEST_CASE_FIXTURE(DataNodeTest, "ProcessBuildQueue processes all tasks") {
  auto seg1 = CreateSegmentWithVectors(50);
  auto seg2 = CreateSegmentWithVectors(50);

  REQUIRE(data_node_->ScheduleBuildTask({seg1, core::IndexType::FLAT, 10}).ok());
  REQUIRE(data_node_->ScheduleBuildTask({seg2, core::IndexType::FLAT, 20}).ok());

  size_t processed = data_node_->ProcessBuildQueue();
  CHECK_EQ(processed, 2);
  CHECK_FALSE(data_node_->HasPendingTasks());

  // Both segments should be SEALED
  CHECK_EQ(segment_store_->GetSegment(seg1)->GetState(), core::SegmentState::SEALED);
  CHECK_EQ(segment_store_->GetSegment(seg2)->GetState(), core::SegmentState::SEALED);
}

TEST_CASE_FIXTURE(DataNodeTest, "ProcessBuildQueue empty queue returns 0") {
  CHECK_EQ(data_node_->ProcessBuildQueue(), 0);
}

TEST_CASE_FIXTURE(DataNodeTest, "ProcessBuildQueue handles failed tasks") {
  // Schedule a task for a nonexistent segment
  REQUIRE(
      data_node_->ScheduleBuildTask({core::MakeSegmentId(999), core::IndexType::FLAT, 10}).ok());

  // Also schedule a valid one
  auto seg = CreateSegmentWithVectors(50);
  REQUIRE(data_node_->ScheduleBuildTask({seg, core::IndexType::FLAT, 5}).ok());

  size_t processed = data_node_->ProcessBuildQueue();
  CHECK_EQ(processed, 1);  // Only the valid one succeeds
}

}  // namespace
