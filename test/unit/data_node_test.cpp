// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include <filesystem>

#include "cluster/data_node.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"
#include "core/types.h"
#include "core/vector.h"

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// DataNode Tests
// ============================================================================

class DataNodeTest {
 public:
  DataNodeTest() {
    test_dir_ = "/tmp/gvdb_data_node_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    auto factory = std::make_unique<index::IndexFactory>();
    factory_ptr_ = factory.get();
    segment_store_ = std::make_shared<storage::SegmentManager>(
        test_dir_, factory_ptr_);
    data_node_ = std::make_unique<DataNode>(std::move(factory), segment_store_);

    collection_id_ = core::MakeCollectionId(1);
    dimension_ = 32;
    metric_ = core::MetricType::L2;
  }

  ~DataNodeTest() {
    std::filesystem::remove_all(test_dir_);
  }

  // Create a GROWING segment with vectors
  core::SegmentId CreateSegmentWithVectors(size_t count) {
    auto seg_result = segment_store_->CreateSegment(collection_id_, dimension_, metric_);
    auto seg_id = *seg_result;

    std::vector<core::Vector> vectors;
    std::vector<core::VectorId> ids;
    for (size_t i = 0; i < count; ++i) {
      vectors.push_back(core::RandomVector(dimension_));
      ids.push_back(core::MakeVectorId(next_id_++));
    }

    auto* segment = segment_store_->GetSegment(seg_id);
    segment->AddVectors(vectors, ids);
    return seg_id;
  }

  // Create a GROWING segment with vectors and metadata
  core::SegmentId CreateSegmentWithMetadata(size_t count) {
    auto seg_result = segment_store_->CreateSegment(collection_id_, dimension_, metric_);
    auto seg_id = *seg_result;

    std::vector<core::Vector> vectors;
    std::vector<core::VectorId> ids;
    std::vector<core::Metadata> metadata;
    for (size_t i = 0; i < count; ++i) {
      vectors.push_back(core::RandomVector(dimension_));
      ids.push_back(core::MakeVectorId(next_id_));
      core::Metadata md;
      md["name"] = core::MetadataValue(std::string("vec_" + std::to_string(next_id_)));
      md["value"] = core::MetadataValue(static_cast<int64_t>(next_id_));
      metadata.push_back(md);
      next_id_++;
    }

    auto* segment = segment_store_->GetSegment(seg_id);
    segment->AddVectorsWithMetadata(vectors, ids, metadata);
    return seg_id;
  }

  // Seal a segment via SegmentManager
  void SealSegment(core::SegmentId seg_id) {
    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;
    auto status = segment_store_->SealSegment(seg_id, config);
    REQUIRE(status.ok());
  }

  std::string test_dir_;
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
    BuildTask task{core::MakeSegmentId(i), core::IndexType::HNSW, i * 10};
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
  auto query = core::RandomVector(dimension_);
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

// ============================================================================
// ProcessBuildQueue Tests
// ============================================================================

TEST_CASE_FIXTURE(DataNodeTest, "ProcessBuildQueue processes all tasks") {
  auto seg1 = CreateSegmentWithVectors(50);
  auto seg2 = CreateSegmentWithVectors(50);

  data_node_->ScheduleBuildTask({seg1, core::IndexType::FLAT, 10});
  data_node_->ScheduleBuildTask({seg2, core::IndexType::FLAT, 20});

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
  data_node_->ScheduleBuildTask({core::MakeSegmentId(999), core::IndexType::FLAT, 10});

  // Also schedule a valid one
  auto seg = CreateSegmentWithVectors(50);
  data_node_->ScheduleBuildTask({seg, core::IndexType::FLAT, 5});

  size_t processed = data_node_->ProcessBuildQueue();
  CHECK_EQ(processed, 1);  // Only the valid one succeeds
}

// ============================================================================
// CompactSegments Tests
// ============================================================================

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments merges two segments") {
  auto seg1 = CreateSegmentWithVectors(30);
  auto seg2 = CreateSegmentWithVectors(20);
  SealSegment(seg1);
  SealSegment(seg2);

  auto status = data_node_->CompactSegments({seg1, seg2});
  INFO("Compaction failed: " << status.message());
  REQUIRE(status.ok());

  // Source segments should be dropped
  CHECK(segment_store_->GetSegment(seg1) == nullptr);
  CHECK(segment_store_->GetSegment(seg2) == nullptr);

  // New segment should exist with 50 vectors
  auto seg_ids = segment_store_->GetCollectionSegments(collection_id_);
  REQUIRE_EQ(seg_ids.size(), 1);

  auto* new_seg = segment_store_->GetSegment(seg_ids[0]);
  REQUIRE(new_seg != nullptr);
  CHECK_EQ(new_seg->GetVectorCount(), 50);
  CHECK_EQ(new_seg->GetState(), core::SegmentState::SEALED);
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments preserves metadata") {
  auto seg1 = CreateSegmentWithMetadata(10);
  auto seg2 = CreateSegmentWithMetadata(10);
  SealSegment(seg1);
  SealSegment(seg2);

  auto status = data_node_->CompactSegments({seg1, seg2});
  REQUIRE(status.ok());

  auto seg_ids = segment_store_->GetCollectionSegments(collection_id_);
  REQUIRE_EQ(seg_ids.size(), 1);

  auto* new_seg = segment_store_->GetSegment(seg_ids[0]);
  CHECK_EQ(new_seg->GetVectorCount(), 20);

  // Check metadata is preserved for first vector
  auto md = new_seg->GetMetadata(core::MakeVectorId(1));
  REQUIRE(md.ok());
  CHECK(md->count("name") > 0);
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments rejects single segment") {
  auto seg = CreateSegmentWithVectors(50);
  SealSegment(seg);

  auto status = data_node_->CompactSegments({seg});
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments rejects growing segments") {
  auto seg1 = CreateSegmentWithVectors(50);
  auto seg2 = CreateSegmentWithVectors(50);
  SealSegment(seg1);
  // seg2 is still GROWING

  auto status = data_node_->CompactSegments({seg1, seg2});
  CHECK_FALSE(status.ok());
  CHECK(absl::IsFailedPrecondition(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments rejects empty list") {
  auto status = data_node_->CompactSegments({});
  CHECK_FALSE(status.ok());
  CHECK(absl::IsInvalidArgument(status));
}
