// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Benchmark: NVIDIA CUDA GPU vs CPU (faiss) IVF_FLAT search.
// Reports median latency and CPU/GPU speedup at varying scales.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
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

double BenchSearch(core::IVectorIndex* idx, const core::Vector& query,
                   int k, int runs) {
  for (int i = 0; i < 3; ++i) (void)idx->Search(query, k);
  std::vector<double> times;
  times.reserve(runs);
  for (int i = 0; i < runs; ++i) {
    auto start = std::chrono::steady_clock::now();
    auto res = idx->Search(query, k);
    auto end = std::chrono::steady_clock::now();
    (void)res;
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

int IvfNlist(size_t n) {
  // Faiss heuristic: nlist ≈ sqrt(n), capped so each cluster has at least 39
  // training points (faiss-gpu requirement).
  int nlist = static_cast<int>(std::sqrt(static_cast<double>(n)));
  return std::max(16, std::min(nlist, static_cast<int>(n / 64)));
}

}  // namespace

int main() {
#ifdef GVDB_HAS_CUDA
  if (!index::cuda::IsAvailable()) {
    std::printf("CUDA GPU not available on this system.\n");
    return 1;
  }
  std::printf("GVDB CUDA Benchmark (IVF_FLAT, faiss-gpu)\n");
  std::printf("=========================================\n\n");

  const int K = 10;
  const int RUNS = 10;

  // Cheap default sweep: small vector counts at dim=768. Total runtime
  // ~1 min on A10G. To get production-class numbers, expand the counts
  // array and/or add a dimension sweep below.
  const core::Dimension DIM = 768;
  std::printf("IVF_FLAT L2 Search (top-k=%d, dim=%d)\n", K, DIM);
  std::printf("  %12s  |  %13s  |  %11s  |  %s\n",
              "Vectors", "CPU (faiss)", "CUDA GPU", "Speedup");
  std::printf("  %12s--+--%13s--+--%11s--+--%s\n",
              "------------", "-------------", "-----------", "-------");

  size_t counts[] = {10000, 50000, 100000};
  for (size_t n : counts) {
    auto vectors = RandomVectors(n, DIM);
    auto ids = SequentialIds(n);
    auto query = RandomVectors(1, DIM, 99)[0];
    const int nlist = IvfNlist(n);
    const int nprobe = std::max(1, nlist / 8);

    index::FaissIVFIndex cpu_idx(DIM, core::MetricType::L2, nlist, nprobe);
    cpu_idx.DisableGpu();
    cpu_idx.Build(vectors, ids);
    double cpu_ms = BenchSearch(&cpu_idx, query, K, RUNS);

    index::FaissIVFIndex gpu_idx(DIM, core::MetricType::L2, nlist, nprobe);
    gpu_idx.Build(vectors, ids);
    double gpu_ms = BenchSearch(&gpu_idx, query, K, RUNS);

    char label[32];
    std::snprintf(label, sizeof(label), "%zu", n);
    PrintRow(label, cpu_ms, gpu_ms);
  }
  std::printf("\n");

  std::printf("Done.\n");
  return 0;
#else
  std::printf("Built without CUDA support (-DGVDB_WITH_CUDA=OFF).\n");
  return 1;
#endif
}
