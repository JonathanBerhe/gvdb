// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Benchmark: NVIDIA CUDA GPU vs CPU (faiss) FLAT L2 search.
//
// Deliberately aligned with metal_bench.cpp — same index type (exhaustive
// FLAT L2), same top-k, and the same vector-count and dimension sweeps — so
// the two GPU backends can be compared head-to-head. FLAT (not IVF) is the
// common denominator: the Metal backend only implements a FLAT kernel.
//
// The GPU side clones a raw faiss::IndexFlatL2 to the device via
// index_cpu_to_gpu. It uses sequential ids (plain add), which is fine for a
// pure search-latency measurement and sidesteps the add_with_ids limitation
// that keeps GpuIndexFlat out of the production FaissFlatIndex path.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <vector>

#include <faiss/IndexFlat.h>

#include "core/types.h"
#include "core/vector.h"

#ifdef GVDB_HAS_CUDA
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/StandardGpuResources.h>

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
    for (size_t d = 0; d < static_cast<size_t>(dim); ++d) data[d] = dist(rng);
    vecs.emplace_back(data);
  }
  return vecs;
}

// Median of `runs` search latencies (ms) against a raw faiss index, after 3
// warm-up searches. Matches metal_bench.cpp's methodology.
double BenchFaissRaw(faiss::Index* idx, const float* query, int k, int runs) {
  std::vector<float> distances(k);
  std::vector<faiss::idx_t> labels(k);
  for (int i = 0; i < 3; ++i) idx->search(1, query, k, distances.data(), labels.data());

  std::vector<double> times;
  times.reserve(runs);
  for (int i = 0; i < runs; ++i) {
    auto start = std::chrono::steady_clock::now();
    idx->search(1, query, k, distances.data(), labels.data());
    auto end = std::chrono::steady_clock::now();
    times.push_back(
        std::chrono::duration<double, std::milli>(end - start).count());
  }
  std::sort(times.begin(), times.end());
  return times[runs / 2];
}

void PrintRow(const char* label, double cpu_ms, double gpu_ms) {
  double speedup = (gpu_ms > 0) ? cpu_ms / gpu_ms : 0;
  const char* arrow = (speedup < 1.0) ? " <-- CPU faster" : "";
  std::printf("  %12s  |  %8.2f ms  |  %8.2f ms  |  %5.1fx%s\n",
              label, cpu_ms, gpu_ms, speedup, arrow);
}

#ifdef GVDB_HAS_CUDA
// Runs one sweep point: builds a CPU FLAT index and its GPU clone over `n`
// random `dim`-D vectors, benchmarks both, prints the row. Frees intermediate
// buffers aggressively so the 2M-vector point stays within host/device memory.
void BenchPoint(const char* label, size_t n, core::Dimension dim, int k,
                int runs, faiss::gpu::StandardGpuResources* res) {
  auto vectors = RandomVectors(n, dim);
  auto query = RandomVectors(1, dim, 99)[0];

  // Pack into a contiguous buffer, then release the per-vector copies.
  std::vector<float> flat(n * static_cast<size_t>(dim));
  for (size_t i = 0; i < n; ++i) {
    std::memcpy(flat.data() + i * dim, vectors[i].data(),
                static_cast<size_t>(dim) * sizeof(float));
  }
  vectors.clear();
  vectors.shrink_to_fit();

  faiss::IndexFlatL2 cpu_flat(dim);
  cpu_flat.add(static_cast<faiss::idx_t>(n), flat.data());
  flat.clear();
  flat.shrink_to_fit();

  double cpu_ms = BenchFaissRaw(&cpu_flat, query.data(), k, runs);

  std::unique_ptr<faiss::Index> gpu_idx(
      faiss::gpu::index_cpu_to_gpu(res, 0, &cpu_flat));
  double gpu_ms = BenchFaissRaw(gpu_idx.get(), query.data(), k, runs);

  PrintRow(label, cpu_ms, gpu_ms);
}
#endif

}  // namespace

int main() {
#ifdef GVDB_HAS_CUDA
  if (!index::cuda::IsAvailable()) {
    std::printf("CUDA GPU not available on this system.\n");
    return 1;
  }
  std::printf("GVDB CUDA Benchmark (FLAT, faiss-gpu)\n");
  std::printf("=====================================\n\n");

  const int K = 10;
  const int RUNS = 10;
  faiss::gpu::StandardGpuResources res;

  // --- Sweep vector count at dim=768 (mirrors metal_bench.cpp) ---
  {
    const core::Dimension DIM = 768;
    std::printf("FLAT L2 Search (top-k=%d, dim=%d)\n", K, DIM);
    std::printf("  %12s  |  %13s  |  %11s  |  %s\n",
                "Vectors", "CPU (faiss)", "CUDA GPU", "Speedup");
    std::printf("  %12s--+--%13s--+--%11s--+--%s\n",
                "------------", "-------------", "-----------", "-------");

    size_t counts[] = {1000, 10000, 50000, 100000, 500000, 1000000, 2000000};
    for (size_t n : counts) {
      char label[32];
      std::snprintf(label, sizeof(label), "%zu", n);
      BenchPoint(label, n, DIM, K, RUNS, &res);
    }
    std::printf("\n");
  }

  // --- Sweep dimension at 100K vectors (mirrors metal_bench.cpp) ---
  {
    const size_t N = 100000;
    std::printf("FLAT L2 Search (top-k=%d, vectors=%zu, varying dim)\n", K, N);
    std::printf("  %12s  |  %13s  |  %11s  |  %s\n",
                "Dimension", "CPU (faiss)", "CUDA GPU", "Speedup");
    std::printf("  %12s--+--%13s--+--%11s--+--%s\n",
                "------------", "-------------", "-----------", "-------");

    core::Dimension dims[] = {128, 384, 768, 1536};
    for (core::Dimension dim : dims) {
      char label[32];
      std::snprintf(label, sizeof(label), "%d", dim);
      BenchPoint(label, N, dim, K, RUNS, &res);
    }
    std::printf("\n");
  }

  std::printf("Done.\n");
  return 0;
#else
  std::printf("Built without CUDA support (-DGVDB_WITH_CUDA=OFF).\n");
  return 1;
#endif
}
