// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "cluster/query_node.h"
#include "compute/query_executor.h"
#include "core/config.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"

using namespace gvdb;
using namespace gvdb::cluster;

// ============================================================================
// Test Fixture
// ============================================================================

class QueryNodeTest {
 public:
  QueryNodeTest()
      : collection_id_(core::MakeCollectionId(1)),
        dimension_(32),
        metric_(core::MetricType::L2),
        memory_limit_(64 * 1024 * 1024) {  // 64 MB

    index_factory_ = std::make_unique<index::IndexFactory>();

    segment_store_ = std::make_shared<storage::SegmentManager>(
        "/tmp/gvdb_query_node_test", index_factory_.get());

    query_executor_ = std::make_shared<compute::QueryExecutor>(
        segment_store_.get());

    query_node_ = std::make_unique<QueryNode>(
        segment_store_, query_executor_, memory_limit_);
  }

  ~QueryNodeTest() {
    query_node_.reset();
    query_executor_.reset();
    segment_store_.reset();
    index_factory_.reset();
  }

  // Helper: create a segment with vectors and return its ID
  core::SegmentId CreatePopulatedSegment(size_t num_vectors) {
    auto segment_result = segment_store_->CreateSegment(
        collection_id_, dimension_, metric_);
    REQUIRE(segment_result.ok());
    auto segment_id = segment_result.value();

    auto vectors = CreateTestVectors(num_vectors);
    auto ids = CreateTestVectorIds(num_vectors, next_vector_id_offset_);
    next_vector_id_offset_ += num_vectors;

    auto status = segment_store_->WriteVectors(segment_id, vectors, ids);
    REQUIRE(status.ok());

    return segment_id;
  }

  // Helper: create test vectors
  std::vector<core::Vector> CreateTestVectors(size_t count) {
    std::vector<core::Vector> vectors;
    vectors.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      std::vector<float> data(dimension_);
      for (int32_t d = 0; d < dimension_; ++d) {
        data[d] = static_cast<float>(i * dimension_ + d);
      }
      vectors.emplace_back(std::move(data));
    }
    return vectors;
  }

  // Helper: create vector IDs
  std::vector<core::VectorId> CreateTestVectorIds(size_t count, size_t offset) {
    std::vector<core::VectorId> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(core::MakeVectorId(static_cast<uint64_t>(offset + i + 1)));
    }
    return ids;
  }

  core::CollectionId collection_id_;
  int32_t dimension_;
  core::MetricType metric_;
  size_t memory_limit_;
  size_t next_vector_id_offset_ = 0;

  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  std::shared_ptr<compute::QueryExecutor> query_executor_;
  std::unique_ptr<QueryNode> query_node_;
};

// ============================================================================
// QueryNode Tests
// ============================================================================

TEST_CASE_FIXTURE(QueryNodeTest, "Initial state has no loaded segments") {
  auto loaded = query_node_->GetLoadedSegments();
  CHECK(loaded.empty());
}

TEST_CASE_FIXTURE(QueryNodeTest, "GetMemoryLimit returns configured value") {
  CHECK_EQ(query_node_->GetMemoryLimit(), memory_limit_);
}

TEST_CASE_FIXTURE(QueryNodeTest, "GetMemoryUtilization is zero initially") {
  CHECK_EQ(query_node_->GetMemoryUtilization(), doctest::Approx(0.0f));
  CHECK_EQ(query_node_->GetMemoryUsage(), 0);
}

TEST_CASE_FIXTURE(QueryNodeTest, "LoadSegment succeeds for existing segment") {
  auto segment_id = CreatePopulatedSegment(10);

  auto status = query_node_->LoadSegment(segment_id);
  CHECK(status.ok());
  CHECK(query_node_->IsSegmentLoaded(segment_id));
}

TEST_CASE_FIXTURE(QueryNodeTest, "LoadSegment fails for nonexistent segment") {
  auto fake_id = core::MakeSegmentId(9999);

  auto status = query_node_->LoadSegment(fake_id);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(QueryNodeTest, "LoadSegment twice returns AlreadyExists") {
  auto segment_id = CreatePopulatedSegment(10);

  auto status1 = query_node_->LoadSegment(segment_id);
  REQUIRE(status1.ok());

  auto status2 = query_node_->LoadSegment(segment_id);
  CHECK_FALSE(status2.ok());
  CHECK(absl::IsAlreadyExists(status2));
}

TEST_CASE_FIXTURE(QueryNodeTest, "UnloadSegment succeeds") {
  auto segment_id = CreatePopulatedSegment(10);

  auto load_status = query_node_->LoadSegment(segment_id);
  REQUIRE(load_status.ok());
  CHECK(query_node_->IsSegmentLoaded(segment_id));

  auto unload_status = query_node_->UnloadSegment(segment_id);
  CHECK(unload_status.ok());
  CHECK_FALSE(query_node_->IsSegmentLoaded(segment_id));
}

TEST_CASE_FIXTURE(QueryNodeTest, "UnloadSegment fails for not-loaded segment") {
  auto fake_id = core::MakeSegmentId(9999);

  auto status = query_node_->UnloadSegment(fake_id);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsNotFound(status));
}

TEST_CASE_FIXTURE(QueryNodeTest, "GetLoadedSegments reflects loaded set") {
  auto seg1 = CreatePopulatedSegment(5);
  auto seg2 = CreatePopulatedSegment(5);
  auto seg3 = CreatePopulatedSegment(5);

  REQUIRE(query_node_->LoadSegment(seg1).ok());
  REQUIRE(query_node_->LoadSegment(seg2).ok());
  REQUIRE(query_node_->LoadSegment(seg3).ok());

  auto loaded = query_node_->GetLoadedSegments();
  CHECK_EQ(loaded.size(), 3);

  // Verify all segment IDs are present
  CHECK(std::find(loaded.begin(), loaded.end(), seg1) != loaded.end());
  CHECK(std::find(loaded.begin(), loaded.end(), seg2) != loaded.end());
  CHECK(std::find(loaded.begin(), loaded.end(), seg3) != loaded.end());

  // Unload one and verify
  REQUIRE(query_node_->UnloadSegment(seg2).ok());

  loaded = query_node_->GetLoadedSegments();
  CHECK_EQ(loaded.size(), 2);
  CHECK(std::find(loaded.begin(), loaded.end(), seg2) == loaded.end());
}

TEST_CASE_FIXTURE(QueryNodeTest, "Memory tracking increases on load") {
  CHECK_EQ(query_node_->GetMemoryUsage(), 0);

  auto segment_id = CreatePopulatedSegment(10);
  auto* segment = segment_store_->GetSegment(segment_id);
  REQUIRE(segment != nullptr);
  size_t expected_usage = segment->GetMemoryUsage();

  auto status = query_node_->LoadSegment(segment_id);
  REQUIRE(status.ok());

  CHECK_EQ(query_node_->GetMemoryUsage(), expected_usage);
  CHECK_GT(query_node_->GetMemoryUtilization(), 0.0f);
}

TEST_CASE_FIXTURE(QueryNodeTest, "Memory tracking decreases on unload") {
  auto segment_id = CreatePopulatedSegment(10);

  auto status = query_node_->LoadSegment(segment_id);
  REQUIRE(status.ok());
  CHECK_GT(query_node_->GetMemoryUsage(), 0);

  status = query_node_->UnloadSegment(segment_id);
  REQUIRE(status.ok());

  CHECK_EQ(query_node_->GetMemoryUsage(), 0);
  CHECK_EQ(query_node_->GetMemoryUtilization(), doctest::Approx(0.0f));
}

TEST_CASE_FIXTURE(QueryNodeTest, "Eviction when memory limit reached") {
  // Use a very small memory limit to trigger eviction
  auto small_limit_node = std::make_unique<QueryNode>(
      segment_store_, query_executor_, 1);  // 1 byte limit

  auto seg1 = CreatePopulatedSegment(10);
  auto seg2 = CreatePopulatedSegment(10);

  // First segment load should evict nothing (starts empty, but segment > 1 byte)
  // The eviction logic tries to free space, but with 0 loaded segments
  // it will either succeed (because after eviction loop it still can't fit)
  // or return ResourceExhausted.
  // Based on the implementation: if no segments are loaded and the new segment
  // exceeds the limit, it returns ResourceExhausted.
  auto status1 = small_limit_node->LoadSegment(seg1);
  CHECK_FALSE(status1.ok());
  CHECK(absl::IsResourceExhausted(status1));

  // Now use a limit that can fit one segment but not two
  auto* seg_ptr = segment_store_->GetSegment(seg1);
  REQUIRE(seg_ptr != nullptr);
  size_t one_seg_size = seg_ptr->GetMemoryUsage();

  // Limit can hold exactly one segment (plus a small margin to avoid rounding)
  auto medium_limit_node = std::make_unique<QueryNode>(
      segment_store_, query_executor_, one_seg_size + 1);

  auto load1 = medium_limit_node->LoadSegment(seg1);
  REQUIRE(load1.ok());
  CHECK(medium_limit_node->IsSegmentLoaded(seg1));

  // Loading second segment should trigger eviction of first
  auto load2 = medium_limit_node->LoadSegment(seg2);
  REQUIRE(load2.ok());

  // seg1 should have been evicted, seg2 should be loaded
  CHECK_FALSE(medium_limit_node->IsSegmentLoaded(seg1));
  CHECK(medium_limit_node->IsSegmentLoaded(seg2));
  CHECK_EQ(medium_limit_node->GetLoadedSegments().size(), 1);
}
