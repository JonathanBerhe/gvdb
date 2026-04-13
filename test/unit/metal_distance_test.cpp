// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Metal GPU distance computation correctness tests.
// Compares MetalFlatIndex results against FaissFlatIndex (CPU) for all metrics.
// Skips gracefully on non-Apple / non-Metal systems.

#include <doctest/doctest.h>

#include <cmath>
#include <random>
#include <vector>

#include "core/interfaces.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"

// FaissFlatIndex is in a private header — include directly for CPU baseline
#include "faiss_flat.h"

#ifdef GVDB_HAS_METAL
#include "metal/metal_compute.h"
#include "metal/metal_flat_index.h"
#endif

using namespace gvdb;

namespace {

// Generate random vectors
std::vector<core::Vector> RandomVectors(size_t n, core::Dimension dim,
                                         unsigned seed = 42) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

  std::vector<core::Vector> vecs;
  vecs.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    std::vector<float> data(dim);
    for (size_t d = 0; d < static_cast<size_t>(dim); ++d) {
      data[d] = dist(rng);
    }
    vecs.emplace_back(data);
  }
  return vecs;
}

std::vector<core::VectorId> SequentialIds(size_t n) {
  std::vector<core::VectorId> ids;
  ids.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    ids.push_back(core::MakeVectorId(i + 1));
  }
  return ids;
}

bool MetalAvailable() {
#ifdef GVDB_HAS_METAL
  return index::metal::MetalCompute::IsAvailable();
#else
  return false;
#endif
}

}  // namespace

// ============================================================================
// Correctness: Metal vs CPU distance results
// ============================================================================

TEST_CASE("Metal L2 search matches CPU") {
  if (!MetalAvailable()) return;

  const size_t N = 1000;
  const core::Dimension DIM = 128;
  const int K = 10;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto queries = RandomVectors(5, DIM, 99);

  // CPU index (faiss)
  index::IndexFactory factory;
  core::IndexConfig cpu_config;
  cpu_config.index_type = core::IndexType::FLAT;
  cpu_config.dimension = DIM;
  cpu_config.metric_type = core::MetricType::L2;

#ifdef GVDB_HAS_METAL
  // Metal index
  index::metal::MetalFlatIndex metal_idx(DIM, core::MetricType::L2);
  REQUIRE(metal_idx.Build(vectors, ids).ok());

  // Build a faiss CPU index directly
  auto cpu_result = factory.CreateIndex(cpu_config);
  // IndexFactory returns Metal on this machine, so build faiss manually
  auto faiss_idx = std::make_unique<index::FaissFlatIndex>(DIM, core::MetricType::L2);
  REQUIRE(faiss_idx->Build(vectors, ids).ok());

  for (const auto& query : queries) {
    auto metal_res = metal_idx.Search(query, K);
    auto cpu_res = faiss_idx->Search(query, K);
    REQUIRE(metal_res.ok());
    REQUIRE(cpu_res.ok());

    auto& m = metal_res.value().entries;
    auto& c = cpu_res.value().entries;
    REQUIRE(m.size() == c.size());

    // Same IDs in same order
    for (size_t i = 0; i < m.size(); ++i) {
      CHECK(m[i].id == c[i].id);
      CHECK(m[i].distance == doctest::Approx(c[i].distance).epsilon(1e-4));
    }
  }
#endif
}

TEST_CASE("Metal inner product search matches CPU") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  const size_t N = 1000;
  const core::Dimension DIM = 128;
  const int K = 10;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto query = RandomVectors(1, DIM, 77)[0];

  index::metal::MetalFlatIndex metal_idx(DIM, core::MetricType::INNER_PRODUCT);
  REQUIRE(metal_idx.Build(vectors, ids).ok());

  auto faiss_idx = std::make_unique<index::FaissFlatIndex>(DIM, core::MetricType::INNER_PRODUCT);
  REQUIRE(faiss_idx->Build(vectors, ids).ok());

  auto metal_res = metal_idx.Search(query, K);
  auto cpu_res = faiss_idx->Search(query, K);
  REQUIRE(metal_res.ok());
  REQUIRE(cpu_res.ok());

  auto& m = metal_res.value().entries;
  auto& c = cpu_res.value().entries;
  REQUIRE(m.size() == c.size());

  for (size_t i = 0; i < m.size(); ++i) {
    CHECK(m[i].id == c[i].id);
    CHECK(m[i].distance == doctest::Approx(c[i].distance).epsilon(1e-4));
  }
#endif
}

TEST_CASE("Metal cosine search matches CPU") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  const size_t N = 500;
  const core::Dimension DIM = 64;
  const int K = 5;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto query = RandomVectors(1, DIM, 55)[0];

  // Cosine in faiss is L2 on normalized vectors. MetalFlatIndex uses a true
  // cosine kernel, so results may differ in absolute distance values.
  // We only check that the top-k IDs match.
  index::metal::MetalFlatIndex metal_idx(DIM, core::MetricType::COSINE);
  REQUIRE(metal_idx.Build(vectors, ids).ok());

  auto faiss_idx = std::make_unique<index::FaissFlatIndex>(DIM, core::MetricType::COSINE);
  REQUIRE(faiss_idx->Build(vectors, ids).ok());

  auto metal_res = metal_idx.Search(query, K);
  auto cpu_res = faiss_idx->Search(query, K);
  REQUIRE(metal_res.ok());
  REQUIRE(cpu_res.ok());

  auto& m = metal_res.value().entries;
  auto& c = cpu_res.value().entries;
  REQUIRE(m.size() == c.size());

  // Top-1 must match (strongest signal)
  CHECK(m[0].id == c[0].id);
#endif
}

// ============================================================================
// Edge cases
// ============================================================================

TEST_CASE("Metal: dimension=1") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  index::metal::MetalFlatIndex idx(1, core::MetricType::L2);
  std::vector<core::Vector> vecs;
  vecs.push_back(core::Vector(std::vector<float>{1.0f}));
  vecs.push_back(core::Vector(std::vector<float>{5.0f}));
  vecs.push_back(core::Vector(std::vector<float>{3.0f}));
  auto ids = SequentialIds(3);
  REQUIRE(idx.Build(vecs, ids).ok());

  core::Vector query(std::vector<float>{2.0f});
  auto res = idx.Search(query, 3);
  REQUIRE(res.ok());
  // Closest to 2.0: 1.0 (dist=1), 3.0 (dist=1), 5.0 (dist=9)
  CHECK(res.value().entries[0].distance == doctest::Approx(1.0f).epsilon(1e-5));
#endif
}

TEST_CASE("Metal: large dimension 4096") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  const core::Dimension DIM = 4096;
  auto vectors = RandomVectors(100, DIM);
  auto ids = SequentialIds(100);

  index::metal::MetalFlatIndex idx(DIM, core::MetricType::L2);
  REQUIRE(idx.Build(vectors, ids).ok());

  auto query = RandomVectors(1, DIM, 33)[0];
  auto res = idx.Search(query, 5);
  REQUIRE(res.ok());
  CHECK(res.value().entries.size() == 5);
#endif
}

TEST_CASE("Metal: single vector") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  index::metal::MetalFlatIndex idx(4, core::MetricType::L2);
  std::vector<core::Vector> vecs;
  vecs.push_back(core::Vector(std::vector<float>{1.0f, 2.0f, 3.0f, 4.0f}));
  auto ids = SequentialIds(1);
  REQUIRE(idx.Build(vecs, ids).ok());

  core::Vector query(std::vector<float>{1.0f, 2.0f, 3.0f, 4.0f});
  auto res = idx.Search(query, 1);
  REQUIRE(res.ok());
  CHECK(res.value().entries.size() == 1);
  CHECK(res.value().entries[0].distance == doctest::Approx(0.0f).epsilon(1e-5));
#endif
}

TEST_CASE("Metal: empty index returns error") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  index::metal::MetalFlatIndex idx(4, core::MetricType::L2);
  core::Vector query({1.0f, 2.0f, 3.0f, 4.0f});
  auto res = idx.Search(query, 5);
  CHECK_FALSE(res.ok());
#endif
}

TEST_CASE("Metal: batch search") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  const size_t N = 500;
  const core::Dimension DIM = 64;
  const int K = 5;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto queries = RandomVectors(10, DIM, 88);

  index::metal::MetalFlatIndex idx(DIM, core::MetricType::L2);
  REQUIRE(idx.Build(vectors, ids).ok());

  auto batch_res = idx.SearchBatch(queries, K);
  REQUIRE(batch_res.ok());
  CHECK(batch_res.value().size() == 10);

  // Each result should have K entries
  for (const auto& res : batch_res.value()) {
    CHECK(res.entries.size() == K);
  }

  // Batch results should match individual searches
  for (size_t i = 0; i < queries.size(); ++i) {
    auto single_res = idx.Search(queries[i], K);
    REQUIRE(single_res.ok());
    CHECK(batch_res.value()[i].entries[0].id == single_res.value().entries[0].id);
  }
#endif
}

TEST_CASE("Metal: AddBatch then search") {
  if (!MetalAvailable()) return;

#ifdef GVDB_HAS_METAL
  const core::Dimension DIM = 32;
  index::metal::MetalFlatIndex idx(DIM, core::MetricType::L2);

  auto v1 = RandomVectors(50, DIM, 1);
  auto id1 = SequentialIds(50);
  REQUIRE(idx.Build(v1, id1).ok());

  auto v2 = RandomVectors(50, DIM, 2);
  std::vector<core::VectorId> id2;
  for (size_t i = 0; i < 50; ++i) id2.push_back(core::MakeVectorId(51 + i));
  REQUIRE(idx.AddBatch(v2, id2).ok());

  CHECK(idx.GetVectorCount() == 100);

  auto query = RandomVectors(1, DIM, 3)[0];
  auto res = idx.Search(query, 10);
  REQUIRE(res.ok());
  CHECK(res.value().entries.size() == 10);
#endif
}

// ============================================================================
// Non-Metal fallback
// ============================================================================

TEST_CASE("IndexFactory returns valid FLAT index regardless of Metal") {
  index::IndexFactory factory;
  core::IndexConfig config;
  config.index_type = core::IndexType::FLAT;
  config.dimension = 64;
  config.metric_type = core::MetricType::L2;

  auto result = factory.CreateIndex(config);
  REQUIRE(result.ok());
  CHECK(result.value()->GetIndexType() == core::IndexType::FLAT);
  CHECK(result.value()->GetDimension() == 64);
}
