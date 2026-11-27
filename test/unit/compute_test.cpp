// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "compute/query_executor.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/segment/segment_manager.h"
#include "support/temp_dir.h"
#include "support/vectors.h"

namespace gvdb {
namespace compute {
namespace test {

// ============================================================================
// Test Fixture
// ============================================================================

class ComputeTest {
 public:
  ComputeTest() {
    collection_id_ = core::MakeCollectionId(1);
    dimension_ = 128;
    metric_ = core::MetricType::L2;

    // Create index factory
    index_factory_ = std::make_unique<index::IndexFactory>();

    // Create segment manager in a unique scratch folder
    segment_manager_ =
        std::make_unique<storage::SegmentManager>(data_dir_.str(), index_factory_.get());
  }

  // Helper: Create test vectors
  std::vector<core::Vector> CreateTestVectors(size_t count, uint32_t dimension) {
    std::vector<core::Vector> vectors;
    vectors.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      std::vector<float> data(dimension);
      for (uint32_t d = 0; d < dimension; ++d) {
        data[d] = static_cast<float>(i * dimension + d);
      }
      vectors.emplace_back(std::move(data));
    }

    return vectors;
  }

  // Helper: Create test vector IDs
  std::vector<core::VectorId> CreateTestVectorIds(size_t count, size_t offset = 0) {
    return gvdb::test::SequentialIds(count, static_cast<uint64_t>(offset + 1));
  }

  // Helper: Populate segment manager with test data
  void PopulateSegmentManager(size_t num_vectors) {
    auto vectors = CreateTestVectors(num_vectors, dimension_);
    auto ids = CreateTestVectorIds(num_vectors);

    auto segment_result = segment_manager_->CreateSegment(collection_id_, dimension_, metric_);
    REQUIRE(segment_result.ok());
    auto segment_id = segment_result.value();

    auto status = segment_manager_->WriteVectors(segment_id, vectors, ids);
    REQUIRE(status.ok());

    // Seal the segment
    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;

    status = segment_manager_->SealSegment(segment_id, config);
    REQUIRE(status.ok());
  }

  // Declared first, so the folder is removed after the segment manager is
  // destroyed. The namespace is spelled out because this file has its own
  // gvdb::compute::test.
  gvdb::test::ScopedTempDir data_dir_{"gvdb_compute_test"};
  core::CollectionId collection_id_;
  uint32_t dimension_;
  core::MetricType metric_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::unique_ptr<storage::SegmentManager> segment_manager_;
};

// ============================================================================
// QueryExecutor Tests
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "SearchEmptyCollection") {
  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 10);

  // Searching a collection with no segments returns NotFound error
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchWithData") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 10);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 10);

  // Verify results are sorted by distance (ascending)
  for (size_t i = 1; i < result.value().entries.size(); ++i) {
    CHECK_LE(result.value().entries[i - 1].distance, result.value().entries[i].distance);
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchTopKLargerThanDataset") {
  PopulateSegmentManager(50);

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 100);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 50);  // Only 50 vectors available
}

TEST_CASE_FIXTURE(ComputeTest, "SearchInvalidTopK") {
  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];

  auto result = executor.Search(collection_id_, query, 0);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);

  result = executor.Search(collection_id_, query, -1);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchMultipleSegments") {
  // Create and populate multiple segments
  for (int i = 0; i < 3; ++i) {
    auto vectors = CreateTestVectors(30, dimension_);
    auto ids = CreateTestVectorIds(30, i * 30);

    auto segment_result = segment_manager_->CreateSegment(collection_id_, dimension_, metric_);
    REQUIRE(segment_result.ok());
    auto segment_id = segment_result.value();

    auto status = segment_manager_->WriteVectors(segment_id, vectors, ids);
    REQUIRE(status.ok());

    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;

    status = segment_manager_->SealSegment(segment_id, config);
    REQUIRE(status.ok());
  }

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 20);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 20);

  // Verify results are merged and sorted
  for (size_t i = 1; i < result.value().entries.size(); ++i) {
    CHECK_LE(result.value().entries[i - 1].distance, result.value().entries[i].distance);
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchExactMatch") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(*segment_manager_);

  // Query with the exact vector that was inserted (should have distance ~0)
  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 5);

  INFO(result.status().message());
  REQUIRE(result.ok());
  REQUIRE_GT(result.value().entries.size(), 0);

  // First result should be the exact match with distance ~0
  CHECK(result.value().entries[0].distance == doctest::Approx(0.0f).epsilon(1e-5));
}

// Search latency is measured by test/bench/search_bench.cpp (make
// bench-search), not here: a wall-clock limit in a unit test fails on a
// loaded machine and proves nothing about correctness.
TEST_CASE_FIXTURE(ComputeTest, "SearchMergesTopKAcrossFiveSegments") {
  // Five sealed segments hold the same 200 vectors under different ids, so
  // every distance appears exactly five times in the collection.
  constexpr size_t kSegments = 5;
  constexpr size_t kPerSegment = 200;
  for (size_t i = 0; i < kSegments; ++i) {
    auto vectors = CreateTestVectors(kPerSegment, dimension_);
    auto ids = CreateTestVectorIds(kPerSegment, i * kPerSegment);

    auto segment_result = segment_manager_->CreateSegment(collection_id_, dimension_, metric_);
    REQUIRE(segment_result.ok());
    auto segment_id = segment_result.value();

    auto status = segment_manager_->WriteVectors(segment_id, vectors, ids);
    REQUIRE(status.ok());

    core::IndexConfig config;
    config.index_type = core::IndexType::FLAT;
    config.dimension = dimension_;
    config.metric_type = metric_;

    status = segment_manager_->SealSegment(segment_id, config);
    REQUIRE(status.ok());
  }

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 50);

  INFO(result.status().message());
  REQUIRE(result.ok());
  const auto& entries = result.value().entries;
  REQUIRE_EQ(entries.size(), 50);

  // Ascending by distance after the merge
  for (size_t i = 1; i < entries.size(); ++i) {
    CHECK_LE(entries[i - 1].distance, entries[i].distance);
  }

  // Ids are unique: no entry is returned twice
  std::set<uint64_t> seen;
  for (const auto& entry : entries) {
    CHECK(seen.insert(core::ToUInt64(entry.id)).second);
  }

  // The 50 nearest are the 10 nearest distinct vectors, each from all five
  // segments. The checks do not depend on the distance scale (squared or
  // plain L2): the copies of a vector tie, the exact match is at 0, and each
  // group is farther than the one before.
  float previous_group_distance = -1.0f;
  for (size_t rank = 0; rank < 10; ++rank) {
    const float group_distance = entries[rank * kSegments].distance;
    if (rank == 0) {
      CHECK(group_distance == doctest::Approx(0.0));
    } else {
      CHECK_GT(group_distance, previous_group_distance);
    }
    previous_group_distance = group_distance;
    std::set<uint64_t> group_ids;
    for (size_t copy = 0; copy < kSegments; ++copy) {
      const auto& entry = entries[rank * kSegments + copy];
      CHECK(entry.distance == doctest::Approx(group_distance).epsilon(1e-5));
      group_ids.insert(core::ToUInt64(entry.id));
    }
    std::set<uint64_t> expected_ids;
    for (size_t copy = 0; copy < kSegments; ++copy) {
      expected_ids.insert(copy * kPerSegment + rank + 1);
    }
    CHECK_EQ(group_ids, expected_ids);
  }
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "SearchAfterSegmentDrop") {
  // Create a segment and populate it
  auto segment_result = segment_manager_->CreateSegment(collection_id_, dimension_, metric_);
  REQUIRE(segment_result.ok());
  auto segment_id = segment_result.value();

  auto vectors = CreateTestVectors(100, dimension_);
  auto ids = CreateTestVectorIds(100);

  auto status = segment_manager_->WriteVectors(segment_id, vectors, ids);
  REQUIRE(status.ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  status = segment_manager_->SealSegment(segment_id, config);
  REQUIRE(status.ok());

  // Drop the segment
  status = segment_manager_->DropSegment(segment_id);
  REQUIRE(status.ok());

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 10);

  // After dropping all segments, collection exists but has no results
  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 0);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchDifferentCollections") {
  // Create data for collection 1
  PopulateSegmentManager(50);

  // Create data for collection 2
  auto collection_id_2 = core::MakeCollectionId(2);

  auto segment_result = segment_manager_->CreateSegment(collection_id_2, dimension_, metric_);
  REQUIRE(segment_result.ok());
  auto segment_id_2 = segment_result.value();

  auto vectors = CreateTestVectors(30, dimension_);
  auto ids = CreateTestVectorIds(30, 100);

  auto status = segment_manager_->WriteVectors(segment_id_2, vectors, ids);
  REQUIRE(status.ok());

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = dimension_;
  config.metric_type = metric_;

  status = segment_manager_->SealSegment(segment_id_2, config);
  REQUIRE(status.ok());

  compute::QueryExecutor executor(*segment_manager_);

  auto query = CreateTestVectors(1, dimension_)[0];

  // Search collection 1
  auto result1 = executor.Search(collection_id_, query, 10);
  REQUIRE(result1.ok());
  CHECK_EQ(result1.value().entries.size(), 10);

  // Search collection 2
  auto result2 = executor.Search(collection_id_2, query, 10);
  REQUIRE(result2.ok());
  CHECK_EQ(result2.value().entries.size(), 10);
}

}  // namespace test
}  // namespace compute
}  // namespace gvdb
