#include <doctest/doctest.h>

#include <thread>
#include <vector>

#include "core/config.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "index/index_manager.h"
#include "index/bm25_index.h"

// Internal headers — test target adds src/index to its include path so we can
// verify adaptive parameters propagate into the underlying Faiss structures.
#include "faiss_hnsw.h"
#include "faiss_ivf.h"

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

// ============================================================================
// BM25 Index Tests
// ============================================================================

TEST_CASE("BM25Index: AddAndSearch") {
  BM25Index idx;
  REQUIRE(idx.AddDocument(core::MakeVectorId(1), "the quick brown fox jumps over the lazy dog").ok());
  REQUIRE(idx.AddDocument(core::MakeVectorId(2), "the quick brown fox").ok());
  REQUIRE(idx.AddDocument(core::MakeVectorId(3), "a lazy dog sleeps all day").ok());
  REQUIRE(idx.AddDocument(core::MakeVectorId(4), "cats and birds in the garden").ok());

  auto result = idx.Search("quick fox", 3);
  REQUIRE(result.ok());
  REQUIRE(result->entries.size() >= 2);

  // IDs 1 and 2 contain both "quick" and "fox" — they must be in top 2
  uint64_t id0 = core::ToUInt64(result->entries[0].id);
  uint64_t id1 = core::ToUInt64(result->entries[1].id);
  CHECK(((id0 == 1 && id1 == 2) || (id0 == 2 && id1 == 1)));

  // Both should have positive BM25 scores
  CHECK(result->entries[0].distance > 0.0f);
  CHECK(result->entries[1].distance > 0.0f);

  // Top result should score higher than second
  CHECK(result->entries[0].distance >= result->entries[1].distance);
}

TEST_CASE("BM25Index: EmptyIndex") {
  BM25Index idx;
  auto result = idx.Search("anything", 5);
  REQUIRE(result.ok());
  CHECK(result->entries.empty());
}

TEST_CASE("BM25Index: NoMatchingTerms") {
  BM25Index idx;
  REQUIRE(idx.AddDocument(core::MakeVectorId(1), "apples oranges bananas").ok());
  auto result = idx.Search("cars trucks", 5);
  REQUIRE(result.ok());
  CHECK(result->entries.empty());
}

TEST_CASE("BM25Index: SingleDocument") {
  BM25Index idx;
  REQUIRE(idx.AddDocument(core::MakeVectorId(42), "machine learning deep neural networks").ok());
  auto result = idx.Search("machine learning", 1);
  REQUIRE(result.ok());
  REQUIRE(result->entries.size() == 1);
  CHECK(result->entries[0].id == core::MakeVectorId(42));
  CHECK(result->entries[0].distance > 0.0f);  // BM25 score should be positive
}

TEST_CASE("BM25Index: CaseInsensitive") {
  BM25Index idx;
  REQUIRE(idx.AddDocument(core::MakeVectorId(1), "Hello World UPPERCASE lowercase").ok());
  auto result = idx.Search("hello WORLD", 1);
  REQUIRE(result.ok());
  REQUIRE(result->entries.size() == 1);
  CHECK(result->entries[0].id == core::MakeVectorId(1));
}

TEST_CASE("BM25Index: DocumentCount") {
  BM25Index idx;
  CHECK(idx.GetDocumentCount() == 0);
  REQUIRE(idx.AddDocument(core::MakeVectorId(1), "first document").ok());
  CHECK(idx.GetDocumentCount() == 1);
  REQUIRE(idx.AddDocument(core::MakeVectorId(2), "second document").ok());
  CHECK(idx.GetDocumentCount() == 2);
}

TEST_CASE("BM25Index: MemoryUsage") {
  BM25Index idx;
  CHECK(idx.GetMemoryUsage() == 0);
  REQUIRE(idx.AddDocument(core::MakeVectorId(1), "some text with several words to index").ok());
  CHECK(idx.GetMemoryUsage() > 0);
}

TEST_CASE("BM25Index: TopKLimit") {
  BM25Index idx;
  for (int i = 1; i <= 20; ++i) {
    REQUIRE(idx.AddDocument(core::MakeVectorId(i), "common shared terms everywhere").ok());
  }
  auto result = idx.Search("common terms", 5);
  REQUIRE(result.ok());
  CHECK(result->entries.size() <= 5);
}

// ============================================================================
// Auto-Index Selection Tests
// ============================================================================

TEST_CASE("ResolveAutoIndexType - selects FLAT for small counts") {
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 0),
           core::IndexType::FLAT);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 1),
           core::IndexType::FLAT);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 9'999),
           core::IndexType::FLAT);
}

TEST_CASE("ResolveAutoIndexType - selects HNSW for medium counts") {
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 10'000),
           core::IndexType::HNSW);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 100'000),
           core::IndexType::HNSW);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 499'999),
           core::IndexType::HNSW);
}

TEST_CASE("ResolveAutoIndexType - selects IVF_SQ for 500K-1M") {
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 500'000),
           core::IndexType::IVF_SQ);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 750'000),
           core::IndexType::IVF_SQ);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 999'999),
           core::IndexType::IVF_SQ);
}

TEST_CASE("ResolveAutoIndexType - selects IVF_TURBOQUANT for large counts") {
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 1'000'000),
           core::IndexType::IVF_TURBOQUANT);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::AUTO, 10'000'000),
           core::IndexType::IVF_TURBOQUANT);
}

TEST_CASE("ResolveAutoIndexType - passes through explicit types unchanged") {
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::FLAT, 1'000'000),
           core::IndexType::FLAT);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::HNSW, 5),
           core::IndexType::HNSW);
  CHECK_EQ(core::ResolveAutoIndexType(core::IndexType::IVF_TURBOQUANT, 100),
           core::IndexType::IVF_TURBOQUANT);
}

// ============================================================================
// ResolveAutoIndexConfig Tests — Adaptive Parameters
// ============================================================================

TEST_CASE("ResolveAutoIndexConfig - FLAT tier uses default params") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 5'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::FLAT);
  CHECK_EQ(config.dimension, 128);
  CHECK_EQ(config.metric_type, core::MetricType::L2);
}

TEST_CASE("ResolveAutoIndexConfig - small HNSW tier") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 50'000, 128, core::MetricType::COSINE);
  CHECK_EQ(config.index_type, core::IndexType::HNSW);
  CHECK_EQ(config.hnsw_params.M, core::kSmallHnswM);
  CHECK_EQ(config.hnsw_params.ef_construction, core::kSmallHnswEfConstruction);
  CHECK_EQ(config.hnsw_params.ef_search, core::kSmallHnswEfSearch);
}

TEST_CASE("ResolveAutoIndexConfig - large HNSW tier") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::HNSW);
  CHECK_EQ(config.hnsw_params.M, core::kLargeHnswM);
  CHECK_EQ(config.hnsw_params.ef_construction, core::kLargeHnswEfConstruction);
  CHECK_EQ(config.hnsw_params.ef_search, core::kLargeHnswEfSearch);
}

TEST_CASE("ResolveAutoIndexConfig - high-dim increases HNSW M (large tier)") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000, 768, core::MetricType::COSINE);
  CHECK_EQ(config.index_type, core::IndexType::HNSW);
  CHECK_EQ(config.hnsw_params.M, core::kHighDimHnswM);
  CHECK_EQ(config.hnsw_params.ef_construction, core::kLargeHnswEfConstruction);
  CHECK_EQ(config.hnsw_params.ef_search, core::kLargeHnswEfSearch);
}

TEST_CASE("ResolveAutoIndexConfig - high-dim increases HNSW M (small tier)") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 50'000, 1536, core::MetricType::COSINE);
  CHECK_EQ(config.index_type, core::IndexType::HNSW);
  CHECK_EQ(config.hnsw_params.M, core::kHighDimHnswM);
  CHECK_EQ(config.hnsw_params.ef_construction, core::kSmallHnswEfConstruction);
  CHECK_EQ(config.hnsw_params.ef_search, core::kSmallHnswEfSearch);
}

TEST_CASE("ResolveAutoIndexConfig - dim at threshold stays standard M") {
  // kHighDimThreshold uses strict '>', so exactly 512 stays at standard M.
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000, core::kHighDimThreshold,
      core::MetricType::L2);
  CHECK_EQ(config.hnsw_params.M, core::kLargeHnswM);
}

TEST_CASE("ResolveAutoIndexConfig - low-dim keeps standard HNSW M") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000, 128, core::MetricType::L2);
  CHECK_EQ(config.hnsw_params.M, core::kLargeHnswM);
}

TEST_CASE("ResolveAutoIndexConfig - IVF_SQ tier with adaptive nlist/nprobe") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 750'000, 256, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::IVF_SQ);
  const auto expected = core::ComputeIvfParams(750'000);
  CHECK_EQ(config.ivf_params.nlist, expected.nlist);
  CHECK_EQ(config.ivf_params.nprobe, expected.nprobe);
}

TEST_CASE("ResolveAutoIndexConfig - IVF_TURBOQUANT 4-bit tier") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 2'000'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::IVF_TURBOQUANT);
  CHECK_EQ(config.ivf_turboquant_params.bit_width, 4);
  const auto expected = core::ComputeIvfParams(2'000'000);
  CHECK_EQ(config.ivf_turboquant_params.nlist, expected.nlist);
  CHECK_EQ(config.ivf_turboquant_params.nprobe, expected.nprobe);
}

TEST_CASE("ResolveAutoIndexConfig - IVF_TURBOQUANT 4-bit stays at 50M") {
  // Reassure the 2-bit pivot is at 100M, not 10M.
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 50'000'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::IVF_TURBOQUANT);
  CHECK_EQ(config.ivf_turboquant_params.bit_width, 4);
}

TEST_CASE("ResolveAutoIndexConfig - IVF_TURBOQUANT 2-bit for 100M+") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::IVF_TURBOQUANT);
  CHECK_EQ(config.ivf_turboquant_params.bit_width, 2);
  const auto expected = core::ComputeIvfParams(200'000'000);
  CHECK_EQ(config.ivf_turboquant_params.nlist, expected.nlist);
  CHECK_EQ(config.ivf_turboquant_params.nprobe, expected.nprobe);
}

TEST_CASE("ResolveAutoIndexConfig - explicit type keeps defaults, adaptive applies only to AUTO") {
  // Callers passing an explicit type receive struct defaults — any pre-tuned
  // sub-params must be set by constructing IndexConfig directly.
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::HNSW, 5'000'000, 128, core::MetricType::L2);
  CHECK_EQ(config.index_type, core::IndexType::HNSW);
  CHECK_EQ(config.hnsw_params.M, 16);
  CHECK_EQ(config.hnsw_params.ef_construction, 200);
  CHECK_EQ(config.hnsw_params.ef_search, 100);
}

TEST_CASE("ComputeIvfParams - floor at 1 and deterministic sqrt") {
  CHECK_EQ(core::ComputeIvfParams(0).nlist, 1);
  CHECK_EQ(core::ComputeIvfParams(0).nprobe, 1);
  CHECK_EQ(core::ComputeIvfParams(10'000).nlist, 100);
  CHECK_EQ(core::ComputeIvfParams(10'000).nprobe, 10);
}

TEST_CASE("ResolveAutoIndexConfig - TurboQuant tier boundary at 100M") {
  // Exactly at kAutoTierTQ4Max → 2-bit (not 4-bit)
  auto at_100m = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, core::kAutoTierTQ4Max, 128, core::MetricType::L2);
  CHECK_EQ(at_100m.index_type, core::IndexType::IVF_TURBOQUANT);
  CHECK_EQ(at_100m.ivf_turboquant_params.bit_width, 2);

  // Just below → 4-bit
  auto below = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, core::kAutoTierTQ4Max - 1, 128,
      core::MetricType::L2);
  CHECK_EQ(below.ivf_turboquant_params.bit_width, 4);
}

TEST_CASE("ResolveAutoIndexConfig - tier boundaries exact") {
  // Exactly at kAutoTierFlatMax → HNSW (not FLAT)
  auto at_10k = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, core::kAutoTierFlatMax, 128, core::MetricType::L2);
  CHECK_EQ(at_10k.index_type, core::IndexType::HNSW);

  // Exactly at kAutoTierHnswMax → IVF_SQ (not HNSW)
  auto at_500k = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, core::kAutoTierHnswMax, 128, core::MetricType::L2);
  CHECK_EQ(at_500k.index_type, core::IndexType::IVF_SQ);

  // Exactly at kAutoTierSqMax → IVF_TURBOQUANT (not IVF_SQ)
  auto at_1m = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, core::kAutoTierSqMax, 128, core::MetricType::L2);
  CHECK_EQ(at_1m.index_type, core::IndexType::IVF_TURBOQUANT);
}

// ============================================================================
// Integration: adaptive parameters must reach the underlying Faiss index
// ============================================================================

TEST_CASE("IndexFactory - adaptive ef_search reaches the Faiss HNSW graph") {
  // Small HNSW tier: ResolveAutoIndexConfig produces ef_search=64.
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 50'000, 128, core::MetricType::L2);
  REQUIRE_EQ(config.index_type, core::IndexType::HNSW);
  REQUIRE_EQ(config.hnsw_params.ef_search, core::kSmallHnswEfSearch);

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());

  auto* hnsw = dynamic_cast<FaissHNSWIndex*>(result.value().get());
  REQUIRE(hnsw != nullptr);
  // Must read through the IndexIDMap wrapper down to faiss::IndexHNSW.
  CHECK_EQ(hnsw->GetEfSearch(), core::kSmallHnswEfSearch);
}

TEST_CASE("IndexFactory - adaptive ef_search differs for large HNSW tier") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 200'000, 128, core::MetricType::L2);
  REQUIRE_EQ(config.hnsw_params.ef_search, core::kLargeHnswEfSearch);

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());

  auto* hnsw = dynamic_cast<FaissHNSWIndex*>(result.value().get());
  REQUIRE(hnsw != nullptr);
  CHECK_EQ(hnsw->GetEfSearch(), core::kLargeHnswEfSearch);
}

TEST_CASE("IndexFactory - adaptive nprobe reaches IVF_SQ") {
  auto config = core::ResolveAutoIndexConfig(
      core::IndexType::AUTO, 750'000, 128, core::MetricType::L2);
  REQUIRE_EQ(config.index_type, core::IndexType::IVF_SQ);
  const auto expected = core::ComputeIvfParams(750'000);
  REQUIRE_EQ(config.ivf_params.nprobe, expected.nprobe);

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());

  auto* ivf = dynamic_cast<FaissIVFIndex*>(result.value().get());
  REQUIRE(ivf != nullptr);
  CHECK_EQ(ivf->GetNProbe(), expected.nprobe);
}

TEST_CASE("IndexFactory - CreateHNSWIndex plumbs ef_search directly") {
  auto result = IndexFactory::CreateHNSWIndex(
      64, core::MetricType::L2, /*M=*/16, /*ef_construction=*/100,
      /*ef_search=*/77);
  REQUIRE(result.ok());

  auto* hnsw = dynamic_cast<FaissHNSWIndex*>(result.value().get());
  REQUIRE(hnsw != nullptr);
  CHECK_EQ(hnsw->GetEfSearch(), 77);
}

TEST_CASE("IndexFactory - CreateIVFIndex plumbs nprobe directly") {
  auto result = IndexFactory::CreateIVFIndex(
      64, core::MetricType::L2, /*nlist=*/50, IVFQuantizationType::NONE,
      /*pq_m=*/8, /*pq_nbits=*/8, /*nprobe=*/17);
  REQUIRE(result.ok());

  auto* ivf = dynamic_cast<FaissIVFIndex*>(result.value().get());
  REQUIRE(ivf != nullptr);
  CHECK_EQ(ivf->GetNProbe(), 17);
}

TEST_CASE("IndexFactory - rejects invalid ef_search") {
  auto result = IndexFactory::CreateHNSWIndex(64, core::MetricType::L2, 16, 100, 0);
  CHECK_FALSE(result.ok());
}

TEST_CASE("IndexFactory - rejects non-positive nprobe") {
  auto bad_zero = IndexFactory::CreateIVFIndex(
      64, core::MetricType::L2, 10, IVFQuantizationType::NONE, 8, 8, /*nprobe=*/0);
  CHECK_FALSE(bad_zero.ok());
}

TEST_CASE("IndexFactory - rejects AUTO index type") {
  core::IndexConfig config;
  config.index_type = core::IndexType::AUTO;
  config.dimension = 128;
  config.metric_type = core::MetricType::L2;

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  CHECK_FALSE(result.ok());
}

}  // namespace
}  // namespace index
}  // namespace gvdb
