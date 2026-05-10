// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// CUDA GPU smoke test for faiss-gpu integration.
// Builds a small IVF_FLAT index with the GPU clone path and asserts the
// search returns sensible top-k IDs and ordered distances. Skips gracefully
// without CUDA.

#include <doctest/doctest.h>

#include <random>
#include <unordered_set>
#include <vector>

#include "core/types.h"
#include "core/vector.h"
#include "faiss_ivf.h"

#ifdef GVDB_HAS_CUDA
#include "cuda/cuda_compute.h"
#endif

using namespace gvdb;

namespace {

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

bool CudaAvailable() {
#ifdef GVDB_HAS_CUDA
  return index::cuda::IsAvailable();
#else
  return false;
#endif
}

}  // namespace

// CTest label: cuda — `ctest -L cuda` selects only this suite.
TEST_CASE("CUDA IVF_FLAT smoke: build + search returns sensible top-k") {
  if (!CudaAvailable()) return;

  constexpr size_t N = 10'000;
  constexpr core::Dimension DIM = 128;
  constexpr int NLIST = 64;
  constexpr int NPROBE = 16;
  constexpr int K = 10;
  constexpr size_t NUM_QUERIES = 10;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto queries = RandomVectors(NUM_QUERIES, DIM, /*seed=*/123);

  index::FaissIVFIndex idx(DIM, core::MetricType::L2, NLIST, NPROBE);
  REQUIRE(idx.Build(vectors, ids).ok());

  for (size_t q = 0; q < NUM_QUERIES; ++q) {
    auto result = idx.Search(queries[q], K);
    REQUIRE(result.ok());

    const auto& entries = result->entries;
    CHECK_LE(entries.size(), static_cast<size_t>(K));
    CHECK_GT(entries.size(), 0u);

    // All returned IDs must be in the valid range we inserted.
    std::unordered_set<uint64_t> seen;
    for (const auto& e : entries) {
      const uint64_t v = core::ToUInt64(e.id);
      CHECK_GE(v, 1u);
      CHECK_LE(v, static_cast<uint64_t>(N));
      CHECK(seen.insert(v).second);  // no duplicates
    }

    // Distances must be non-negative (L2) and non-decreasing.
    for (size_t i = 0; i < entries.size(); ++i) {
      CHECK_GE(entries[i].distance, 0.0f);
      if (i > 0) {
        CHECK_GE(entries[i].distance, entries[i - 1].distance);
      }
    }
  }
}

// Round-trip serialize → deserialize on a GPU-resident index. Verifies
// the GPU→CPU clone path in Serialize() and the post-load CPU→GPU clone
// in Deserialize(). Also a regression guard for the StandardGpuResources
// destruction ordering.
TEST_CASE("CUDA IVF_FLAT smoke: serialize/deserialize round-trip") {
  if (!CudaAvailable()) return;

  constexpr size_t N = 2'000;
  constexpr core::Dimension DIM = 64;
  constexpr int NLIST = 16;
  constexpr int NPROBE = 4;
  constexpr int K = 5;

  auto vectors = RandomVectors(N, DIM);
  auto ids = SequentialIds(N);
  auto query = RandomVectors(1, DIM, /*seed=*/7).front();

  const std::string path = "/tmp/gvdb-cuda-smoke-roundtrip.faiss";

  // Build + serialize from a GPU-resident index.
  {
    index::FaissIVFIndex idx(DIM, core::MetricType::L2, NLIST, NPROBE);
    REQUIRE(idx.Build(vectors, ids).ok());
    REQUIRE(idx.Serialize(path).ok());
  }

  // Deserialize into a fresh index, search, expect plausible top-k.
  index::FaissIVFIndex loaded(DIM, core::MetricType::L2, NLIST, NPROBE);
  REQUIRE(loaded.Deserialize(path).ok());

  auto result = loaded.Search(query, K);
  REQUIRE(result.ok());
  CHECK_GT(result->entries.size(), 0u);
  for (const auto& e : result->entries) {
    const uint64_t v = core::ToUInt64(e.id);
    CHECK_GE(v, 1u);
    CHECK_LE(v, static_cast<uint64_t>(N));
  }
}
