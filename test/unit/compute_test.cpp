#include <doctest/doctest.h>

#include <memory>
#include <vector>

#include "compute/query_executor.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"
#include "utils/thread_pool.h"

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

    // Create segment manager
    segment_manager_ = std::make_unique<storage::SegmentManager>(
        "./test_compute_data", index_factory_.get());
  }

  ~ComputeTest() {
    segment_manager_.reset();
    index_factory_.reset();
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
    std::vector<core::VectorId> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(core::MakeVectorId(static_cast<uint64_t>(offset + i + 1)));
    }
    return ids;
  }

  // Helper: Populate segment manager with test data
  void PopulateSegmentManager(size_t num_vectors) {
    auto vectors = CreateTestVectors(num_vectors, dimension_);
    auto ids = CreateTestVectorIds(num_vectors);

    auto segment_result = segment_manager_->CreateSegment(
        collection_id_, dimension_, metric_);
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

  core::CollectionId collection_id_;
  uint32_t dimension_;
  core::MetricType metric_;
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::unique_ptr<storage::SegmentManager> segment_manager_;
};

// ============================================================================
// QueryExecutor Tests
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "CreateQueryExecutor") {
  compute::QueryExecutor executor(segment_manager_.get());
  CHECK_GT(executor.thread_count(), 0);
}

TEST_CASE_FIXTURE(ComputeTest, "CreateQueryExecutorWithThreadPool") {
  utils::ThreadPool pool(4);
  compute::QueryExecutor executor(segment_manager_.get(), &pool);
  CHECK_EQ(executor.thread_count(), 4);
}

TEST_CASE_FIXTURE(ComputeTest, "CreateQueryExecutorNullSegmentManager") {
  CHECK_THROWS_AS(
      compute::QueryExecutor executor(nullptr),
      std::invalid_argument
  );
}

TEST_CASE_FIXTURE(ComputeTest, "SearchEmptyCollection") {
  compute::QueryExecutor executor(segment_manager_.get());

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 10);

  // Searching a collection with no segments returns NotFound error
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchWithData") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(segment_manager_.get());

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 10);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 10);

  // Verify results are sorted by distance (ascending)
  for (size_t i = 1; i < result.value().entries.size(); ++i) {
    CHECK_LE(result.value().entries[i - 1].distance,
              result.value().entries[i].distance);
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchTopKLargerThanDataset") {
  PopulateSegmentManager(50);

  compute::QueryExecutor executor(segment_manager_.get());

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 100);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 50);  // Only 50 vectors available
}

TEST_CASE_FIXTURE(ComputeTest, "SearchInvalidTopK") {
  compute::QueryExecutor executor(segment_manager_.get());

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

    auto segment_result = segment_manager_->CreateSegment(
        collection_id_, dimension_, metric_);
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

  compute::QueryExecutor executor(segment_manager_.get());

  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 20);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 20);

  // Verify results are merged and sorted
  for (size_t i = 1; i < result.value().entries.size(); ++i) {
    CHECK_LE(result.value().entries[i - 1].distance,
              result.value().entries[i].distance);
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchExactMatch") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(segment_manager_.get());

  // Query with the exact vector that was inserted (should have distance ~0)
  auto query = CreateTestVectors(1, dimension_)[0];
  auto result = executor.Search(collection_id_, query, 5);

  INFO(result.status().message());
  REQUIRE(result.ok());
  REQUIRE_GT(result.value().entries.size(), 0);

  // First result should be the exact match with distance ~0
  CHECK(result.value().entries[0].distance == doctest::Approx(0.0f).epsilon(1e-5));
}

// ============================================================================
// Batch Search Tests
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "SearchBatchEmpty") {
  compute::QueryExecutor executor(segment_manager_.get());

  std::vector<core::Vector> queries;
  auto result = executor.SearchBatch(collection_id_, queries, 10);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().size(), 0);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchBatchSingleQuery") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(segment_manager_.get());

  auto queries = CreateTestVectors(1, dimension_);
  auto result = executor.SearchBatch(collection_id_, queries, 10);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().size(), 1);
  CHECK_EQ(result.value()[0].entries.size(), 10);
}

TEST_CASE_FIXTURE(ComputeTest, "SearchBatchMultipleQueries") {
  PopulateSegmentManager(100);

  compute::QueryExecutor executor(segment_manager_.get());

  auto queries = CreateTestVectors(10, dimension_);
  auto result = executor.SearchBatch(collection_id_, queries, 5);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().size(), 10);

  for (const auto& search_result : result.value()) {
    CHECK_EQ(search_result.entries.size(), 5);
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchBatchParallelExecution") {
  PopulateSegmentManager(100);

  utils::ThreadPool pool(4);
  compute::QueryExecutor executor(segment_manager_.get(), &pool);

  // Large batch to ensure parallel execution
  auto queries = CreateTestVectors(20, dimension_);
  auto result = executor.SearchBatch(collection_id_, queries, 10);

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().size(), 20);

  // Each result should have 10 entries
  for (const auto& search_result : result.value()) {
    CHECK_EQ(search_result.entries.size(), 10);

    // Verify sorted
    for (size_t i = 1; i < search_result.entries.size(); ++i) {
      CHECK_LE(search_result.entries[i - 1].distance,
                search_result.entries[i].distance);
    }
  }
}

TEST_CASE_FIXTURE(ComputeTest, "SearchBatchInvalidTopK") {
  compute::QueryExecutor executor(segment_manager_.get());

  auto queries = CreateTestVectors(5, dimension_);

  auto result = executor.SearchBatch(collection_id_, queries, 0);
  CHECK_FALSE(result.ok());
  CHECK_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

// ============================================================================
// Performance Tests
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "SearchPerformance") {
  // Populate with larger dataset
  for (int i = 0; i < 5; ++i) {
    auto vectors = CreateTestVectors(200, dimension_);
    auto ids = CreateTestVectorIds(200, i * 200);

    auto segment_result = segment_manager_->CreateSegment(
        collection_id_, dimension_, metric_);
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

  compute::QueryExecutor executor(segment_manager_.get());

  auto query = CreateTestVectors(1, dimension_)[0];

  auto start = std::chrono::steady_clock::now();
  auto result = executor.Search(collection_id_, query, 50);
  auto end = std::chrono::steady_clock::now();

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().entries.size(), 50);

  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start).count();

  // Search should complete in reasonable time (< 100ms for 1000 vectors)
  CHECK_LT(duration_ms, 100);
}

TEST_CASE_FIXTURE(ComputeTest, "BatchSearchPerformance") {
  PopulateSegmentManager(500);

  utils::ThreadPool pool(4);
  compute::QueryExecutor executor(segment_manager_.get(), &pool);

  auto queries = CreateTestVectors(50, dimension_);

  auto start = std::chrono::steady_clock::now();
  auto result = executor.SearchBatch(collection_id_, queries, 10);
  auto end = std::chrono::steady_clock::now();

  INFO(result.status().message());
  REQUIRE(result.ok());
  CHECK_EQ(result.value().size(), 50);

  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start).count();

  // Batch search should benefit from parallelism
  CHECK_LT(duration_ms, 500);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_CASE_FIXTURE(ComputeTest, "SearchAfterSegmentDrop") {
  // Create a segment and populate it
  auto segment_result = segment_manager_->CreateSegment(
      collection_id_, dimension_, metric_);
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

  compute::QueryExecutor executor(segment_manager_.get());

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

  auto segment_result = segment_manager_->CreateSegment(
      collection_id_2, dimension_, metric_);
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

  compute::QueryExecutor executor(segment_manager_.get());

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
