// test/unit/sparse_index_test.cpp
// Unit tests for SparseVector and SparseIndex

#include <doctest/doctest.h>
#include <cmath>

#include "core/sparse_vector.h"
#include "index/sparse_index.h"

using namespace gvdb;

// Helper to construct SparseVector from initializer list without ambiguity
core::SparseVector MakeSparse(std::vector<core::SparseVector::Entry> entries) {
  return core::SparseVector(std::move(entries));
}

// ============================================================================
// SparseVector Tests
// ============================================================================

TEST_CASE("SparseVector_Construction") {
  SUBCASE("Empty") {
    core::SparseVector sv;
    CHECK(sv.empty());
    CHECK_EQ(sv.nnz(), 0);
    CHECK_EQ(sv.byte_size(), 0);
    CHECK(sv.IsValid());
  }

  SUBCASE("From entries") {
    std::vector<core::SparseVector::Entry> entries = {{0, 1.0f}, {5, 2.0f}, {10, 3.0f}};
    core::SparseVector sv(entries);
    CHECK_EQ(sv.nnz(), 3);
    CHECK_FALSE(sv.empty());
    CHECK(sv.IsValid());
  }

  SUBCASE("From indices and values") {
    std::vector<uint32_t> indices = {1, 3, 7};
    std::vector<float> values = {0.5f, 1.5f, 2.5f};
    auto sv = core::SparseVector::FromArrays(indices, values);
    CHECK_EQ(sv.nnz(), 3);
    CHECK(sv.IsValid());
    CHECK_EQ(sv.entries()[0].first, 1);
    CHECK_EQ(sv.entries()[2].second, doctest::Approx(2.5f));
  }

  SUBCASE("FromUnsorted deduplicates and sorts") {
    std::vector<core::SparseVector::Entry> entries = {{10, 1.0f}, {2, 2.0f}, {10, 3.0f}, {5, 4.0f}};
    auto sv = core::SparseVector::FromUnsorted(std::move(entries));
    CHECK(sv.IsValid());
    CHECK_LE(sv.nnz(), 3);  // Duplicate index 10 removed
  }
}

TEST_CASE("SparseVector_Validation") {
  SUBCASE("Valid sorted entries") {
    core::SparseVector sv({{0, 1.0f}, {5, 2.0f}, {100, 3.0f}});
    CHECK(sv.IsValid());
    CHECK(sv.Validate().ok());
  }

  SUBCASE("Invalid: unsorted") {
    core::SparseVector sv({{5, 1.0f}, {2, 2.0f}});
    CHECK_FALSE(sv.IsValid());
    CHECK_FALSE(sv.Validate().ok());
  }

  SUBCASE("Invalid: duplicate indices") {
    core::SparseVector sv({{3, 1.0f}, {3, 2.0f}});
    CHECK_FALSE(sv.IsValid());
  }

  SUBCASE("Invalid: NaN value") {
    core::SparseVector sv({{0, std::nanf("")}});
    CHECK_FALSE(sv.IsValid());
  }
}

TEST_CASE("SparseVector_DotProduct") {
  SUBCASE("Matching dimensions") {
    core::SparseVector a({{0, 1.0f}, {2, 3.0f}, {5, 2.0f}});
    core::SparseVector b({{0, 2.0f}, {2, 1.0f}, {5, 4.0f}});
    // 1*2 + 3*1 + 2*4 = 2 + 3 + 8 = 13
    CHECK_EQ(a.DotProduct(b), doctest::Approx(13.0f));
  }

  SUBCASE("Orthogonal (no overlap)") {
    core::SparseVector a({{0, 1.0f}, {1, 2.0f}});
    core::SparseVector b({{3, 3.0f}, {4, 4.0f}});
    CHECK_EQ(a.DotProduct(b), doctest::Approx(0.0f));
  }

  SUBCASE("Partial overlap") {
    core::SparseVector a({{0, 1.0f}, {2, 3.0f}});
    core::SparseVector b({{1, 5.0f}, {2, 2.0f}});
    // Only index 2 matches: 3*2 = 6
    CHECK_EQ(a.DotProduct(b), doctest::Approx(6.0f));
  }

  SUBCASE("Empty vector") {
    core::SparseVector a({{0, 1.0f}});
    core::SparseVector b;
    CHECK_EQ(a.DotProduct(b), doctest::Approx(0.0f));
  }
}

TEST_CASE("SparseVector_CosineSimilarity") {
  SUBCASE("Identical vectors") {
    core::SparseVector a({{0, 3.0f}, {1, 4.0f}});
    CHECK_EQ(a.CosineSimilarity(a), doctest::Approx(1.0f));
  }

  SUBCASE("Orthogonal vectors") {
    core::SparseVector a({{0, 1.0f}});
    core::SparseVector b({{1, 1.0f}});
    CHECK_EQ(a.CosineSimilarity(b), doctest::Approx(0.0f));
  }

  SUBCASE("Empty returns zero") {
    core::SparseVector a({{0, 1.0f}});
    core::SparseVector b;
    CHECK_EQ(a.CosineSimilarity(b), doctest::Approx(0.0f));
  }
}

TEST_CASE("SparseVector_ByteSize") {
  core::SparseVector sv({{0, 1.0f}, {5, 2.0f}, {10, 3.0f}});
  // 3 entries * sizeof(pair<uint32_t, float>) = 3 * 8 = 24
  CHECK_EQ(sv.byte_size(), 3 * sizeof(core::SparseVector::Entry));
}

// ============================================================================
// SparseIndex Tests
// ============================================================================

TEST_CASE("SparseIndex_AddAndSearch") {
  index::SparseIndex idx;

  // Add 5 vectors with known sparse patterns
  idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}, {1, 0.0f}, {2, 3.0f}}));
  idx.AddVector(core::MakeVectorId(2), MakeSparse({{0, 2.0f}, {3, 1.0f}}));
  idx.AddVector(core::MakeVectorId(3), MakeSparse({{2, 5.0f}, {3, 2.0f}}));
  idx.AddVector(core::MakeVectorId(4), MakeSparse({{0, 0.5f}, {2, 1.0f}}));
  idx.AddVector(core::MakeVectorId(5), MakeSparse({{1, 4.0f}, {3, 3.0f}}));

  CHECK_EQ(idx.GetDocumentCount(), 5);

  // Query: dimensions 0 and 2 with weights 1.0 each
  // Scores: vec1 = 1*1 + 3*1 = 4, vec2 = 2*1 = 2, vec3 = 5*1 = 5, vec4 = 0.5 + 1 = 1.5
  core::SparseVector query({{0, 1.0f}, {2, 1.0f}});
  auto result = idx.Search(query, 3);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 3);
  // Top result should be vec3 (score 5), then vec1 (score 4), then vec2 (score 2)
  CHECK_EQ(core::ToUInt64(result->entries[0].id), 3);
  CHECK_EQ(core::ToUInt64(result->entries[1].id), 1);
  CHECK_EQ(core::ToUInt64(result->entries[2].id), 2);
}

TEST_CASE("SparseIndex_NoMatchingDimensions") {
  index::SparseIndex idx;
  idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}}));

  // Query on dimension 99 — no postings
  core::SparseVector query({{99, 1.0f}});
  auto result = idx.Search(query, 10);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 0);
}

TEST_CASE("SparseIndex_EmptyIndex") {
  index::SparseIndex idx;

  core::SparseVector query({{0, 1.0f}});
  auto result = idx.Search(query, 10);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 0);
}

TEST_CASE("SparseIndex_MemoryTracking") {
  index::SparseIndex idx;
  size_t initial = idx.GetMemoryUsage();

  idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}, {1, 2.0f}}));
  CHECK_GT(idx.GetMemoryUsage(), initial);
}

TEST_CASE("SparseIndex_LargeK") {
  index::SparseIndex idx;
  idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}}));
  idx.AddVector(core::MakeVectorId(2), MakeSparse({{0, 2.0f}}));

  // k=100 but only 2 documents
  core::SparseVector query({{0, 1.0f}});
  auto result = idx.Search(query, 100);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 2);
  // Highest score first
  CHECK_EQ(core::ToUInt64(result->entries[0].id), 2);
}
