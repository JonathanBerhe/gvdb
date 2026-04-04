#include <doctest/doctest.h>

#include <cmath>
#include <filesystem>
#include <vector>

#include "core/vector.h"
#include "index/index_factory.h"

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

std::vector<core::VectorId> CreateTestIds(int count) {
  std::vector<core::VectorId> ids;
  ids.reserve(count);
  for (int i = 0; i < count; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }
  return ids;
}

// ============================================================================
// Factory Tests
// ============================================================================

TEST_CASE("TurboQuantFactory - CreateIndex") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      128, core::MetricType::INNER_PRODUCT);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 128);
  CHECK_EQ(index->GetMetricType(), core::MetricType::INNER_PRODUCT);
  CHECK_EQ(index->GetIndexType(), core::IndexType::TURBOQUANT);
}

TEST_CASE("TurboQuantFactory - CreateFromConfig") {
  core::IndexConfig config;
  config.index_type = core::IndexType::TURBOQUANT;
  config.dimension = 64;
  config.metric_type = core::MetricType::L2;
  config.turboquant_params.bit_width = 4;
  config.turboquant_params.use_qjl = true;

  IndexFactory factory;
  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());

  auto index = std::move(result.value());
  CHECK_EQ(index->GetDimension(), 64);
  CHECK_EQ(index->GetIndexType(), core::IndexType::TURBOQUANT);
}

TEST_CASE("TurboQuantFactory - InvalidDimension") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      0, core::MetricType::L2);
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("TurboQuantFactory - InvalidBitWidth") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      64, core::MetricType::L2, 3);  // 3 is not supported
  CHECK_FALSE(result.ok());
  CHECK(absl::IsInvalidArgument(result.status()));
}

TEST_CASE("TurboQuantFactory - AllValidBitWidths") {
  for (int bw : {1, 2, 4, 8}) {
    auto result = IndexFactory::CreateTurboQuantIndex(
        64, core::MetricType::L2, bw);
    REQUIRE_MESSAGE(result.ok(),
                    "bit_width=", bw, " failed: ", result.status().message());
  }
}

// ============================================================================
// Training (data-oblivious — always trained)
// ============================================================================

TEST_CASE("TurboQuant - AlwaysTrained") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  CHECK(index->IsTrained());

  // Train is a no-op but should succeed
  auto vectors = CreateTestVectors(10, 32);
  CHECK(index->Train(vectors).ok());
  CHECK(index->IsTrained());
}

// ============================================================================
// Build and Search (L2 metric)
// ============================================================================

TEST_CASE("TurboQuant - BuildAndSearchL2") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      8, core::MetricType::L2, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  // Build with known vectors
  std::vector<core::Vector> vectors = {
      core::Vector(std::vector<float>{1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}),
      core::Vector(std::vector<float>{0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}),
      core::Vector(std::vector<float>{0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}),
      core::Vector(std::vector<float>{0.9f, 0.1f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}),
  };
  auto ids = CreateTestIds(4);

  auto build_status = index->Build(vectors, ids);
  REQUIRE_MESSAGE(build_status.ok(), build_status.message());
  CHECK_EQ(index->GetVectorCount(), 4);

  // Query close to vector 0
  core::Vector query(std::vector<float>{0.95f, 0.05f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f});
  auto search_result = index->Search(query, 2);
  REQUIRE(search_result.ok());

  auto& result_entries = search_result.value().entries;
  CHECK_EQ(result_entries.size(), 2);

  // The two closest should be vectors 0 and 3 (both near [1,0,...])
  bool found_v1 = false, found_v4 = false;
  for (auto& entry : result_entries) {
    if (core::ToUInt64(entry.id) == 1) found_v1 = true;
    if (core::ToUInt64(entry.id) == 4) found_v4 = true;
  }
  CHECK(found_v1);
  CHECK(found_v4);
}

// ============================================================================
// Build and Search (Inner Product metric)
// ============================================================================

TEST_CASE("TurboQuant - BuildAndSearchIP") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      16, core::MetricType::INNER_PRODUCT, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(200, 16);
  auto ids = CreateTestIds(200);

  REQUIRE(index->Build(vectors, ids).ok());
  CHECK_EQ(index->GetVectorCount(), 200);

  // Search for vector 0 itself — should appear in top results
  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());

  auto& entries = search_result.value().entries;
  CHECK_GT(entries.size(), 0);
  CHECK_LE(entries.size(), 5);

  // The query vector's own ID should be in top results
  bool found_self = false;
  for (auto& e : entries) {
    if (core::ToUInt64(e.id) == 1) found_self = true;
  }
  CHECK(found_self);
}

// ============================================================================
// Build and Search (Cosine metric)
// ============================================================================

TEST_CASE("TurboQuant - BuildAndSearchCosine") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::COSINE, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(100, 32);
  auto ids = CreateTestIds(100);

  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 3);
  REQUIRE(search_result.ok());

  auto& entries = search_result.value().entries;
  CHECK_GT(entries.size(), 0);

  // Cosine distances should be in [0, 2]
  for (auto& e : entries) {
    CHECK_GE(e.distance, -0.1f);  // Small tolerance for quantization
    CHECK_LE(e.distance, 2.1f);
  }
}

// ============================================================================
// Add and AddBatch
// ============================================================================

TEST_CASE("TurboQuant - AddSingle") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      16, core::MetricType::L2, 4, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  // Add vectors one by one (no Build needed — always trained)
  for (int i = 0; i < 20; i++) {
    auto vec = core::RandomVector(16);
    REQUIRE(index->Add(vec, core::MakeVectorId(i + 1)).ok());
  }
  CHECK_EQ(index->GetVectorCount(), 20);

  auto query = core::RandomVector(16);
  auto search_result = index->Search(query, 5);
  REQUIRE(search_result.ok());
  CHECK_EQ(search_result.value().Size(), 5);
}

TEST_CASE("TurboQuant - AddBatch") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(50, 32);
  auto ids = CreateTestIds(50);

  REQUIRE(index->AddBatch(vectors, ids).ok());
  CHECK_EQ(index->GetVectorCount(), 50);
}

TEST_CASE("TurboQuant - DimensionMismatch") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      16, core::MetricType::L2);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto wrong_vec = core::RandomVector(32);
  CHECK_FALSE(index->Add(wrong_vec, core::MakeVectorId(1)).ok());
}

// ============================================================================
// Remove
// ============================================================================

TEST_CASE("TurboQuant - Remove") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      8, core::MetricType::L2, 4, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(10, 8);
  auto ids = CreateTestIds(10);
  REQUIRE(index->Build(vectors, ids).ok());
  CHECK_EQ(index->GetVectorCount(), 10);

  // Remove vector with id=5
  REQUIRE(index->Remove(core::MakeVectorId(5)).ok());
  CHECK_EQ(index->GetVectorCount(), 9);

  // Remove non-existent
  CHECK_FALSE(index->Remove(core::MakeVectorId(999)).ok());
}

// ============================================================================
// Empty index search
// ============================================================================

TEST_CASE("TurboQuant - SearchEmpty") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      8, core::MetricType::L2);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto query = core::RandomVector(8);
  auto search_result = index->Search(query, 5);
  REQUIRE(search_result.ok());
  CHECK_EQ(search_result.value().Size(), 0);
}

// ============================================================================
// Serialization / Deserialization
// ============================================================================

TEST_CASE("TurboQuant - SerializeDeserialize") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      16, core::MetricType::INNER_PRODUCT, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(50, 16);
  auto ids = CreateTestIds(50);
  REQUIRE(index->Build(vectors, ids).ok());

  // Serialize
  std::string path = "/tmp/gvdb_turboquant_test.bin";
  REQUIRE(index->Serialize(path).ok());

  // Create new index and deserialize
  auto result2 = IndexFactory::CreateTurboQuantIndex(
      16, core::MetricType::INNER_PRODUCT, 4, true);
  REQUIRE(result2.ok());
  auto index2 = std::move(result2.value());
  REQUIRE(index2->Deserialize(path).ok());

  CHECK_EQ(index2->GetVectorCount(), 50);
  CHECK_EQ(index2->GetDimension(), 16);

  // Search results should be similar
  auto query = core::RandomVector(16);
  auto r1 = index->Search(query, 5);
  auto r2 = index2->Search(query, 5);
  REQUIRE(r1.ok());
  REQUIRE(r2.ok());

  CHECK_EQ(r1.value().Size(), r2.value().Size());

  // Same top result
  if (r1.value().Size() > 0 && r2.value().Size() > 0) {
    CHECK_EQ(core::ToUInt64(r1.value().entries[0].id),
             core::ToUInt64(r2.value().entries[0].id));
  }

  // Cleanup
  std::filesystem::remove(path);
}

// ============================================================================
// Bit width variations
// ============================================================================

TEST_CASE("TurboQuant - BitWidth1") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2, 1, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(100, 32);
  auto ids = CreateTestIds(100);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE("TurboQuant - BitWidth2") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2, 2, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(100, 32);
  auto ids = CreateTestIds(100);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE("TurboQuant - BitWidth8") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2, 8, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(100, 32);
  auto ids = CreateTestIds(100);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK_GT(search_result.value().Size(), 0);

  // 8-bit should have good recall — query itself should be #1
  bool found_self = false;
  for (auto& e : search_result.value().entries) {
    if (core::ToUInt64(e.id) == 1) found_self = true;
  }
  CHECK(found_self);
}

// ============================================================================
// QJL toggle
// ============================================================================

TEST_CASE("TurboQuant - WithoutQJL") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      64, core::MetricType::INNER_PRODUCT, 4, false);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(200, 64);
  auto ids = CreateTestIds(200);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK_GT(search_result.value().Size(), 0);
}

TEST_CASE("TurboQuant - WithQJL") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      64, core::MetricType::INNER_PRODUCT, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(200, 64);
  auto ids = CreateTestIds(200);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 5);
  REQUIRE(search_result.ok());
  CHECK_GT(search_result.value().Size(), 0);
}

// ============================================================================
// Memory usage
// ============================================================================

TEST_CASE("TurboQuant - MemoryUsage") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      128, core::MetricType::L2, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  size_t empty_mem = index->GetMemoryUsage();

  auto vectors = CreateTestVectors(100, 128);
  auto ids = CreateTestIds(100);
  REQUIRE(index->Build(vectors, ids).ok());

  size_t full_mem = index->GetMemoryUsage();
  CHECK_GT(full_mem, empty_mem);

  // 4-bit quantization: ~128 dims * 4 bits / 8 = 64 bytes per vector for codes
  // Plus norms, ids, etc. Should be much less than FLAT (128 * 4 = 512 bytes)
  size_t per_vector = (full_mem - empty_mem) / 100;
  CHECK_LT(per_vector, 512);  // Must be less than FLAT
}

// ============================================================================
// SearchBatch
// ============================================================================

TEST_CASE("TurboQuant - SearchBatch") {
  auto result = IndexFactory::CreateTurboQuantIndex(
      32, core::MetricType::L2, 4);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(100, 32);
  auto ids = CreateTestIds(100);
  REQUIRE(index->Build(vectors, ids).ok());

  std::vector<core::Vector> queries = {
      core::RandomVector(32),
      core::RandomVector(32),
      core::RandomVector(32)};

  auto batch_result = index->SearchBatch(queries, 5);
  REQUIRE(batch_result.ok());
  CHECK_EQ(batch_result.value().size(), 3);

  for (auto& r : batch_result.value()) {
    CHECK_GT(r.Size(), 0);
    CHECK_LE(r.Size(), 5);
  }
}

// ============================================================================
// High-dimensional (typical embedding scenario)
// ============================================================================

TEST_CASE("TurboQuant - HighDimensional") {
  // 768-d simulating a typical transformer embedding
  auto result = IndexFactory::CreateTurboQuantIndex(
      768, core::MetricType::INNER_PRODUCT, 4, true);
  REQUIRE(result.ok());
  auto index = std::move(result.value());

  auto vectors = CreateTestVectors(500, 768);
  auto ids = CreateTestIds(500);
  REQUIRE(index->Build(vectors, ids).ok());

  auto search_result = index->Search(vectors[0], 10);
  REQUIRE(search_result.ok());
  CHECK_EQ(search_result.value().Size(), 10);

  // Self-search: vector 0 should be in top 10
  bool found_self = false;
  for (auto& e : search_result.value().entries) {
    if (core::ToUInt64(e.id) == 1) found_self = true;
  }
  CHECK(found_self);
}

}  // namespace
}  // namespace index
}  // namespace gvdb
