// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Unit tests for SparseVector and SparseIndex

#include <doctest/doctest.h>
#include <cmath>

#include "core/sparse_vector.h"
#include "index/sparse_index.h"

namespace {

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
    CHECK(sv.Validate().ok());
  }

  SUBCASE("From entries") {
    std::vector<core::SparseVector::Entry> entries = {{0, 1.0f}, {5, 2.0f}, {10, 3.0f}};
    core::SparseVector sv(entries);
    CHECK_EQ(sv.nnz(), 3);
    CHECK_FALSE(sv.empty());
    CHECK(sv.Validate().ok());
  }

  SUBCASE("From indices and values") {
    std::vector<uint32_t> indices = {1, 3, 7};
    std::vector<float> values = {0.5f, 1.5f, 2.5f};
    auto sv = core::SparseVector::FromArrays(indices, values);
    CHECK_EQ(sv.nnz(), 3);
    CHECK(sv.Validate().ok());
    CHECK_EQ(sv.entries()[0].first, 1);
    CHECK_EQ(sv.entries()[2].second, doctest::Approx(2.5f));
  }
}

TEST_CASE("SparseVector_Validation") {
  SUBCASE("Valid sorted entries") {
    core::SparseVector sv({{0, 1.0f}, {5, 2.0f}, {100, 3.0f}});
    CHECK(sv.Validate().ok());
  }

  SUBCASE("Invalid: unsorted") {
    core::SparseVector sv({{5, 1.0f}, {2, 2.0f}});
    CHECK_FALSE(sv.Validate().ok());
  }

  SUBCASE("Invalid: duplicate indices") {
    core::SparseVector sv({{3, 1.0f}, {3, 2.0f}});
    CHECK_FALSE(sv.Validate().ok());
  }

  SUBCASE("Invalid: NaN value") {
    core::SparseVector sv({{0, std::nanf("")}});
    CHECK_FALSE(sv.Validate().ok());
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
  REQUIRE(idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}, {1, 0.0f}, {2, 3.0f}})).ok());
  REQUIRE(idx.AddVector(core::MakeVectorId(2), MakeSparse({{0, 2.0f}, {3, 1.0f}})).ok());
  REQUIRE(idx.AddVector(core::MakeVectorId(3), MakeSparse({{2, 5.0f}, {3, 2.0f}})).ok());
  REQUIRE(idx.AddVector(core::MakeVectorId(4), MakeSparse({{0, 0.5f}, {2, 1.0f}})).ok());
  REQUIRE(idx.AddVector(core::MakeVectorId(5), MakeSparse({{1, 4.0f}, {3, 3.0f}})).ok());

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
  REQUIRE(idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}})).ok());

  // Query on dimension 99: no postings
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

TEST_CASE("SparseIndex_LargeK") {
  index::SparseIndex idx;
  REQUIRE(idx.AddVector(core::MakeVectorId(1), MakeSparse({{0, 1.0f}})).ok());
  REQUIRE(idx.AddVector(core::MakeVectorId(2), MakeSparse({{0, 2.0f}})).ok());

  // k=100 but only 2 documents
  core::SparseVector query({{0, 1.0f}});
  auto result = idx.Search(query, 100);
  REQUIRE(result.ok());
  CHECK_EQ(result->entries.size(), 2);
  // Highest score first
  CHECK_EQ(core::ToUInt64(result->entries[0].id), 2);
}

}  // namespace
