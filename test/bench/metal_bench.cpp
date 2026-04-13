// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Benchmark: Metal GPU vs CPU (faiss) distance computation.
// Measures wall-clock latency for FLAT search at various scales.

#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"

#ifdef GVDB_HAS_METAL
#include "metal/metal_compute.h"
#include "metal/metal_flat_index.h"
#endif

// Directly include faiss flat to bypass Metal selection in IndexFactory
#include "faiss_flat.h"

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
    for (size_t d = 0; d < static_cast<size_t>(dim); ++d) data[d] = dist(rng);
    vecs.emplace_back(data);
  }
  return vecs;
}

std::vector<core::VectorId> SequentialIds(size_t n) {
  std::vector<core::VectorId> ids;
  ids.reserve(n);
  for (size_t i = 0; i < n; ++i) ids.push_back(core::MakeVectorId(i + 1));
  return ids;
}

// Returns median of N runs in milliseconds
double BenchSearch(core::IVectorIndex* idx, const core::Vector& query,
                   int k, int runs) {
  // Warm up
  for (int i = 0; i < 3; ++i) idx->Search(query, k);

  std::vector<double> times;
  times.reserve(runs);
  for (int i = 0; i < runs; ++i) {
    auto start = std::chrono::steady_clock::now();
    auto res = idx->Search(query, k);
    auto end = std::chrono::steady_clock::now();
    (void)res;
    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    times.push_back(ms);
  }

  std::sort(times.begin(), times.end());
  return times[runs / 2];  // median
}

void PrintRow(const char* label, double cpu_ms, double metal_ms) {
  double speedup = (metal_ms > 0) ? cpu_ms / metal_ms : 0;
  const char* arrow = (speedup < 1.0) ? " <-- CPU faster" : "";
  std::printf("  %12s  |  %8.2f ms  |  %8.2f ms  |  %5.1fx%s\n",
              label, cpu_ms, metal_ms, speedup, arrow);
}

}  // namespace

int main() {
#ifdef GVDB_HAS_METAL
  if (!index::metal::MetalCompute::IsAvailable()) {
    std::printf("Metal GPU not available on this system.\n");
    return 1;
  }
  std::printf("GVDB Metal Benchmark\n");
  std::printf("====================\n\n");
#else
  std::printf("Built without Metal support (-DGVDB_WITH_METAL=OFF).\n");
  return 1;
#endif

#ifdef GVDB_HAS_METAL
  const int K = 10;
  const int RUNS = 10;

  // --- Sweep vector count at dim=768 ---
  {
    const core::Dimension DIM = 768;
    std::printf("FLAT L2 Search (top-k=%d, dim=%d)\n", K, DIM);
    std::printf("  %12s  |  %13s  |  %11s  |  %s\n",
                "Vectors", "CPU (faiss)", "Metal GPU", "Speedup");
    std::printf("  %12s--+--%13s--+--%11s--+--%s\n",
                "------------", "-------------", "-----------", "-------");

    size_t counts[] = {1000, 10000, 50000, 100000, 500000, 1000000, 2000000};
    for (size_t n : counts) {
      auto vectors = RandomVectors(n, DIM);
      auto ids = SequentialIds(n);
      auto query = RandomVectors(1, DIM, 99)[0];

      // CPU
      index::FaissFlatIndex cpu_idx(DIM, core::MetricType::L2);
      cpu_idx.Build(vectors, ids);
      double cpu_ms = BenchSearch(&cpu_idx, query, K, RUNS);

      // Metal
      index::metal::MetalFlatIndex metal_idx(DIM, core::MetricType::L2);
      metal_idx.Build(vectors, ids);
      double metal_ms = BenchSearch(&metal_idx, query, K, RUNS);

      char label[32];
      std::snprintf(label, sizeof(label), "%zu", n);
      PrintRow(label, cpu_ms, metal_ms);
    }
    std::printf("\n");
  }

  // --- Sweep dimension at 100K vectors ---
  {
    const size_t N = 100000;
    std::printf("FLAT L2 Search (top-k=%d, vectors=%zu, varying dim)\n", K, N);
    std::printf("  %12s  |  %13s  |  %11s  |  %s\n",
                "Dimension", "CPU (faiss)", "Metal GPU", "Speedup");
    std::printf("  %12s--+--%13s--+--%11s--+--%s\n",
                "------------", "-------------", "-----------", "-------");

    core::Dimension dims[] = {128, 384, 768, 1536};
    for (core::Dimension dim : dims) {
      auto vectors = RandomVectors(N, dim);
      auto ids = SequentialIds(N);
      auto query = RandomVectors(1, dim, 99)[0];

      index::FaissFlatIndex cpu_idx(dim, core::MetricType::L2);
      cpu_idx.Build(vectors, ids);
      double cpu_ms = BenchSearch(&cpu_idx, query, K, RUNS);

      index::metal::MetalFlatIndex metal_idx(dim, core::MetricType::L2);
      metal_idx.Build(vectors, ids);
      double metal_ms = BenchSearch(&metal_idx, query, K, RUNS);

      char label[32];
      std::snprintf(label, sizeof(label), "%d", dim);
      PrintRow(label, cpu_ms, metal_ms);
    }
    std::printf("\n");
  }

  std::printf("Done.\n");
#endif
  return 0;
}
