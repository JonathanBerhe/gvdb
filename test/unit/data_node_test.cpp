// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include "cluster/data_node.h"
#include "index/index_factory.h"
#include "core/types.h"

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// DataNode Tests
// ============================================================================

class DataNodeTest {
 public:
  DataNodeTest() {
    auto factory = std::make_unique<index::IndexFactory>();
    data_node_ = std::make_unique<DataNode>(std::move(factory));
  }

  std::unique_ptr<DataNode> data_node_;
};

TEST_CASE_FIXTURE(DataNodeTest, "ScheduleBuildTask returns OK") {
  BuildTask task{
      core::MakeSegmentId(1),
      core::IndexType::HNSW,
      10
  };

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
  BuildTask task{
      core::MakeSegmentId(1),
      core::IndexType::FLAT,
      5
  };

  auto status = data_node_->ScheduleBuildTask(task);
  REQUIRE(status.ok());

  CHECK(data_node_->HasPendingTasks());
}

TEST_CASE_FIXTURE(DataNodeTest, "MultipleTasks accumulate") {
  for (uint32_t i = 1; i <= 5; ++i) {
    BuildTask task{
        core::MakeSegmentId(i),
        core::IndexType::HNSW,
        i * 10
    };
    auto status = data_node_->ScheduleBuildTask(task);
    REQUIRE(status.ok());
  }

  CHECK_EQ(data_node_->GetPendingTaskCount(), 5);
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildTask priority ordering") {
  BuildTask low{core::MakeSegmentId(1), core::IndexType::FLAT, 1};
  BuildTask mid{core::MakeSegmentId(2), core::IndexType::FLAT, 50};
  BuildTask high{core::MakeSegmentId(3), core::IndexType::FLAT, 100};

  // operator< returns true when this.priority < other.priority
  // std::priority_queue uses operator< and places the "greatest" on top
  CHECK(low < mid);
  CHECK(mid < high);
  CHECK(low < high);
  CHECK_FALSE(high < low);
  CHECK_FALSE(mid < low);

  // Equal priority should not satisfy strict less-than
  BuildTask same{core::MakeSegmentId(4), core::IndexType::FLAT, 50};
  CHECK_FALSE(mid < same);
  CHECK_FALSE(same < mid);
}

TEST_CASE_FIXTURE(DataNodeTest, "BuildIndex returns Unimplemented") {
  auto status = data_node_->BuildIndex(
      core::MakeSegmentId(1),
      core::IndexType::HNSW);

  CHECK_FALSE(status.ok());
  CHECK(absl::IsUnimplemented(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments returns Unimplemented") {
  std::vector<core::SegmentId> segments = {
      core::MakeSegmentId(1),
      core::MakeSegmentId(2),
      core::MakeSegmentId(3)
  };

  auto status = data_node_->CompactSegments(segments);

  CHECK_FALSE(status.ok());
  CHECK(absl::IsUnimplemented(status));
}

TEST_CASE_FIXTURE(DataNodeTest, "CompactSegments with empty list no crash") {
  std::vector<core::SegmentId> empty_segments;

  auto status = data_node_->CompactSegments(empty_segments);

  // Should still return Unimplemented, but must not crash
  CHECK_FALSE(status.ok());
  CHECK(absl::IsUnimplemented(status));
}
