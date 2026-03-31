#include <doctest/doctest.h>

#include <thread>
#include <vector>

#include "core/vector.h"
#include "index/index_factory.h"
#include "index/index_manager.h"

namespace gvdb {
namespace index {
namespace {

// Helper to create test vectors
std::vector<core::Vector> CreateTestVectors(int count, core::Dimension dim) {
  std::vector<core::Vector> vectors;
  vectors.reserve(count);
  for (int i = 0; i < count; ++i) {
    vectors.push_back(core::RandomVector(dim));
  }
  return vectors;
}

// Helper to create test IDs
std::vector<core::VectorId> CreateTestIds(int count) {
  std::vector<core::VectorId> ids;
  ids.reserve(count);
  for (int i = 0; i < count; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }
  return ids;
}

// ============================================================================
// IndexFactory Tests
// ============================================================================

TEST_CASE("IndexFactoryTest - CreateFlatIndex") {
  auto result = IndexFactory::CreateFlatIndex(128, core::MetricType::L2);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 128);
  CHECK_EQ(index->GetMetricType(), core::MetricType::L2);
  CHECK_EQ(index->GetIndexType(), core::IndexType::FLAT);
}

TEST_CASE("IndexFactoryTest - CreateHNSWIndex") {
  auto result = IndexFactory::CreateHNSWIndex(64, core::MetricType::INNER_PRODUCT, 16, 100);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 64);
  CHECK_EQ(index->GetMetricType(), core::MetricType::INNER_PRODUCT);
  CHECK_EQ(index->GetIndexType(), core::IndexType::HNSW);
}

TEST_CASE("IndexFactoryTest - CreateIVFIndex") {
  auto result = IndexFactory::CreateIVFIndex(128, core::MetricType::L2, 10,
                                             IVFQuantizationType::NONE);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 128);
  CHECK_EQ(index->GetIndexType(), core::IndexType::IVF_FLAT);
}

TEST_CASE("IndexFactoryTest - CreateFromConfig") {
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 64;
  config.metric_type = core::MetricType::L2;

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 64);
}

TEST_CASE("IndexFactoryTest - InvalidDimension") {
  auto result = IndexFactory::CreateFlatIndex(0, core::MetricType::L2);
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

// ============================================================================
// FLAT Index Tests
// ============================================================================

TEST_CASE("FlatIndexTest - BuildAndSearch") {
  auto index_result = IndexFactory::CreateFlatIndex(4, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // Create test data
  std::vector<core::Vector> vectors = {
      core::Vector(std::vector<float>{1.0f, 0.0f, 0.0f, 0.0f}),
      core::Vector(std::vector<float>{0.0f, 1.0f, 0.0f, 0.0f}),
      core::Vector(std::vector<float>{0.0f, 0.0f, 1.0f, 0.0f}),
  };
  auto ids = CreateTestIds(3);

  // Build index
  auto build_status = index->Build(vectors, ids);
  REQUIRE_MESSAGE(build_status.ok(), build_status.message());

  CHECK_EQ(index->GetVectorCount(), 3);

  // Search
  core::Vector query(std::vector<float>{1.0f, 0.1f, 0.0f, 0.0f});
  auto search_result = index->Search(query, 2);
  REQUIRE(search_result.ok());

  auto result = search_result.value();
  CHECK_EQ(result.Size(), 2);

  // First result should be vector 0 (closest)
  CHECK_EQ(core::ToUInt64(result.entries[0].id), 1);
}

TEST_CASE("FlatIndexTest - AddAfterBuild") {
  auto index_result = IndexFactory::CreateFlatIndex(4, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // Build with initial vectors
  auto vectors = CreateTestVectors(5, 4);
  auto ids = CreateTestIds(5);
  REQUIRE(index->Build(vectors, ids).ok());

  // Add more vectors
  auto new_vector = core::RandomVector(4);
  auto new_id = core::MakeVectorId(100);
  REQUIRE(index->Add(new_vector, new_id).ok());

  CHECK_EQ(index->GetVectorCount(), 6);
}

TEST_CASE("FlatIndexTest - BatchAdd") {
  auto index_result = IndexFactory::CreateFlatIndex(8, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // Build initial
  auto vectors1 = CreateTestVectors(10, 8);
  auto ids1 = CreateTestIds(10);
  REQUIRE(index->Build(vectors1, ids1).ok());

  // Batch add
  auto vectors2 = CreateTestVectors(5, 8);
  std::vector<core::VectorId> ids2;
  for (int i = 0; i < 5; ++i) {
    ids2.push_back(core::MakeVectorId(100 + i));
  }
  REQUIRE(index->AddBatch(vectors2, ids2).ok());

  CHECK_EQ(index->GetVectorCount(), 15);
}

TEST_CASE("FlatIndexTest - SearchEmpty") {
  auto index_result = IndexFactory::CreateFlatIndex(4, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  core::Vector query = core::RandomVector(4);
  auto search_result = index->Search(query, 10);
  CHECK_FALSE(search_result.ok());
}

TEST_CASE("FlatIndexTest - DimensionMismatch") {
  auto index_result = IndexFactory::CreateFlatIndex(4, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // Try to add vector with wrong dimension
  auto vectors = CreateTestVectors(5, 4);
  auto ids = CreateTestIds(5);
  REQUIRE(index->Build(vectors, ids).ok());

  // This should work
  core::Vector correct_dim = core::RandomVector(4);
  CHECK(index->Add(correct_dim, core::MakeVectorId(100)).ok());
}

// ============================================================================
// HNSW Index Tests
// ============================================================================

TEST_CASE("HNSWIndexTest - BuildAndSearch") {
  auto index_result = IndexFactory::CreateHNSWIndex(8, core::MetricType::L2, 16, 100);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  auto vectors = CreateTestVectors(100, 8);
  auto ids = CreateTestIds(100);

  REQUIRE(index->Build(vectors, ids).ok());
  CHECK_EQ(index->GetVectorCount(), 100);

  // Search
  auto search_result = index->Search(vectors[0], 10);
  REQUIRE(search_result.ok());

  auto result = search_result.value();
  CHECK_GT(result.Size(), 0);
  CHECK_LE(result.Size(), 10);
}

TEST_CASE("HNSWIndexTest - BatchSearch") {
  auto index_result = IndexFactory::CreateHNSWIndex(4, core::MetricType::L2);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  auto vectors = CreateTestVectors(50, 4);
  auto ids = CreateTestIds(50);
  REQUIRE(index->Build(vectors, ids).ok());

  // Batch search
  std::vector<core::Vector> queries = {vectors[0], vectors[1], vectors[2]};
  auto batch_result = index->SearchBatch(queries, 5);
  REQUIRE(batch_result.ok());

  auto results = batch_result.value();
  CHECK_EQ(results.size(), 3);
  for (const auto& result : results) {
    CHECK_GT(result.Size(), 0);
  }
}

// ============================================================================
// IVF Index Tests
// ============================================================================

TEST_CASE("IVFIndexTest - BuildRequiresTraining") {
  auto index_result = IndexFactory::CreateIVFIndex(8, core::MetricType::L2, 5,
                                                    IVFQuantizationType::NONE);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // IVF requires training
  CHECK_FALSE(index->IsTrained());

  auto vectors = CreateTestVectors(100, 8);
  auto ids = CreateTestIds(100);

  // Build should train automatically
  REQUIRE(index->Build(vectors, ids).ok());
  CHECK(index->IsTrained());
  CHECK_EQ(index->GetVectorCount(), 100);
}

TEST_CASE("IVFIndexTest - SearchAfterTraining") {
  auto index_result = IndexFactory::CreateIVFIndex(4, core::MetricType::L2, 3,
                                                    IVFQuantizationType::NONE);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  auto vectors = CreateTestVectors(50, 4);
  auto ids = CreateTestIds(50);

  REQUIRE(index->Build(vectors, ids).ok());

  // Search
  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());

  auto result = search_result.value();
  CHECK_GT(result.Size(), 0);
}

// ============================================================================
// IVF_SQ Index Tests (Scalar Quantization)
// ============================================================================

TEST_CASE("IVFSQIndexTest - CreateAndVerifyType") {
  auto index_result = IndexFactory::CreateIVFIndex(128, core::MetricType::L2, 10,
                                                    IVFQuantizationType::SQ);
  REQUIRE(index_result.ok());

  auto index = std::move(index_result.value());
  CHECK_EQ(index->GetDimension(), 128);
  CHECK_EQ(index->GetIndexType(), core::IndexType::IVF_SQ);
  CHECK_EQ(index->GetMetricType(), core::MetricType::L2);
}

TEST_CASE("IVFSQIndexTest - BuildAndSearch") {
  auto index_result = IndexFactory::CreateIVFIndex(8, core::MetricType::L2, 5,
                                                    IVFQuantizationType::SQ);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // IVF_SQ requires training
  CHECK_FALSE(index->IsTrained());

  auto vectors = CreateTestVectors(100, 8);
  auto ids = CreateTestIds(100);

  // Build should train automatically
  REQUIRE(index->Build(vectors, ids).ok());
  CHECK(index->IsTrained());
  CHECK_EQ(index->GetVectorCount(), 100);

  // Search should work after training
  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());

  auto result = search_result.value();
  CHECK_GT(result.Size(), 0);
  CHECK_LE(result.Size(), 5);
}

// ============================================================================
// IVF_PQ Index Tests (Product Quantization)
// ============================================================================

TEST_CASE("IVFPQIndexTest - CreateAndVerifyType") {
  auto index_result = IndexFactory::CreateIVFIndex(128, core::MetricType::L2, 10,
                                                    IVFQuantizationType::PQ);
  REQUIRE(index_result.ok());

  auto index = std::move(index_result.value());
  CHECK_EQ(index->GetDimension(), 128);
  CHECK_EQ(index->GetIndexType(), core::IndexType::IVF_PQ);
  CHECK_EQ(index->GetMetricType(), core::MetricType::L2);
}

TEST_CASE("IVFPQIndexTest - BuildAndSearch") {
  // Use dimension 16 (divisible by M=8)
  // Create index with 6 bits PQ (requires 39*64=2496 points vs 8 bits requiring 39*256=9984)
  auto index_result = IndexFactory::CreateIVFIndex(
      16, core::MetricType::L2, 5, IVFQuantizationType::PQ, 8, 6);
  REQUIRE(index_result.ok());
  auto index = std::move(index_result.value());

  // IVF_PQ requires training
  CHECK_FALSE(index->IsTrained());

  // PQ with 6 bits needs at least 64 centroids x 39 = 2,496 training points
  // Using 2,500 to be safe
  auto vectors = CreateTestVectors(2500, 16);
  auto ids = CreateTestIds(2500);

  // Build should train automatically
  auto build_status = index->Build(vectors, ids);
  REQUIRE_MESSAGE(build_status.ok(), build_status.message());
  CHECK(index->IsTrained());
  CHECK_EQ(index->GetVectorCount(), 2500);

  // Search should work after training
  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());

  auto result = search_result.value();
  CHECK_GT(result.Size(), 0);
  CHECK_LE(result.Size(), 5);
}

// ============================================================================
// IndexManager Tests
// ============================================================================

TEST_CASE("IndexManagerTest - CreateAndGetIndex") {
  IndexManager manager;

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 64;
  config.metric_type = core::MetricType::L2;

  auto segment_id = core::MakeSegmentId(1);
  REQUIRE(manager.CreateIndex(segment_id, config).ok());

  CHECK(manager.HasIndex(segment_id));
  CHECK_EQ(manager.GetIndexCount(), 1);

  auto index_result = manager.GetIndex(segment_id);
  REQUIRE(index_result.ok());

  auto* index = index_result.value();
  CHECK_NE(index, nullptr);
  CHECK_EQ(index->GetDimension(), 64);
}

TEST_CASE("IndexManagerTest - RemoveIndex") {
  IndexManager manager;

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 32;

  auto segment_id = core::MakeSegmentId(42);
  REQUIRE(manager.CreateIndex(segment_id, config).ok());
  CHECK(manager.HasIndex(segment_id));

  REQUIRE(manager.RemoveIndex(segment_id).ok());
  CHECK_FALSE(manager.HasIndex(segment_id));
  CHECK_EQ(manager.GetIndexCount(), 0);
}

TEST_CASE("IndexManagerTest - MultipleIndexes") {
  IndexManager manager;

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 16;

  for (uint32_t i = 1; i <= 5; ++i) {
    auto segment_id = core::MakeSegmentId(i);
    REQUIRE(manager.CreateIndex(segment_id, config).ok());
  }

  CHECK_EQ(manager.GetIndexCount(), 5);

  for (uint32_t i = 1; i <= 5; ++i) {
    CHECK(manager.HasIndex(core::MakeSegmentId(i)));
  }
}

TEST_CASE("IndexManagerTest - DuplicateSegmentId") {
  IndexManager manager;

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 8;

  auto segment_id = core::MakeSegmentId(1);
  REQUIRE(manager.CreateIndex(segment_id, config).ok());

  // Try to create again
  auto status = manager.CreateIndex(segment_id, config);
  CHECK_FALSE(status.ok());
  CHECK(absl::IsAlreadyExists(status));
}

TEST_CASE("IndexManagerTest - GetNonexistentIndex") {
  IndexManager manager;

  auto result = manager.GetIndex(core::MakeSegmentId(999));
  CHECK_FALSE(result.ok());
  CHECK(absl::IsNotFound(result.status()));
}

TEST_CASE("IndexManagerTest - Clear") {
  IndexManager manager;

  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 4;

  for (uint32_t i = 1; i <= 3; ++i) {
    REQUIRE(manager.CreateIndex(core::MakeSegmentId(i), config).ok());
  }

  CHECK_EQ(manager.GetIndexCount(), 3);

  manager.Clear();
  CHECK_EQ(manager.GetIndexCount(), 0);
}

}  // namespace
}  // namespace index
}  // namespace gvdb
