// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Benchmark: CPU search latency of compute::QueryExecutor over sealed FAISS
// segments (FLAT and HNSW, L2). Each row builds one segment, then prints the
// median wall-clock latency of a top-k search over RUNS queries.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <system_error>
#include <vector>

#include "compute/query_executor.h"
#include "core/config.h"
#include "core/types.h"
#include "core/vector.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"
#include "utils/logger.h"

using namespace gvdb;

namespace {

constexpr int kTopK = 10;
constexpr int kRuns = 30;
constexpr int kWarmup = 3;
constexpr int kDistinctQueries = 16;

// Scratch folder for the segment managers, removed on every exit path.
std::filesystem::path g_scratch_dir;

// Removes the scratch folder and stops the async logger while its thread pool
// still exists, so static destruction does not report a failed flush.
void Cleanup() {
  utils::Logger::Shutdown();
  if (g_scratch_dir.empty()) return;
  std::error_code ec;
  std::filesystem::remove_all(g_scratch_dir, ec);
}

// Stops the benchmark: timings of a segment or index that failed to build
// would be meaningless.
[[noreturn]] void Fail(const std::string& what, const core::Status& status) {
  std::fprintf(stderr, "search_bench: %s failed: %s\n", what.c_str(),
               std::string(status.message()).c_str());
  Cleanup();
  std::exit(1);
}

[[noreturn]] void Fail(const std::string& what) {
  std::fprintf(stderr, "search_bench: %s\n", what.c_str());
  Cleanup();
  std::exit(1);
}

std::filesystem::path MakeScratchDir() {
  std::error_code ec;
  auto tmp = std::filesystem::temp_directory_path(ec);
  if (ec) Fail("finding the temp folder: " + ec.message());
  std::random_device rd;
  auto dir = tmp / ("gvdb_search_bench_" + std::to_string(rd()) + "_" +
                    std::to_string(rd()));
  std::filesystem::create_directories(dir, ec);
  if (ec) Fail("creating scratch folder " + dir.string() + ": " + ec.message());
  return dir;
}

// Uniform floats in [-1, 1) from a xorshift generator. It is much faster than
// std::uniform_real_distribution in a Debug build, which matters at 77M
// floats per row.
std::vector<core::Vector> RandomVectors(size_t n, core::Dimension dim,
                                        uint64_t seed) {
  uint64_t state = seed * 0x9E3779B97F4A7C15ULL + 1;
  std::vector<core::Vector> vecs;
  vecs.reserve(n);
  std::vector<float> data(static_cast<size_t>(dim));
  for (size_t i = 0; i < n; ++i) {
    for (auto& value : data) {
      state ^= state << 13;
      state ^= state >> 7;
      state ^= state << 17;
      value = static_cast<float>(state >> 40) * (2.0f / 16777216.0f) - 1.0f;
    }
    vecs.emplace_back(data);
  }
  return vecs;
}

std::vector<core::VectorId> SequentialIds(size_t n) {
  std::vector<core::VectorId> ids;
  ids.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    ids.push_back(core::MakeVectorId(static_cast<uint64_t>(i + 1)));
  }
  return ids;
}

const char* IndexName(core::IndexType type) {
  return type == core::IndexType::HNSW ? "HNSW" : "FLAT";
}

struct RowResult {
  double build_s;
  double median_ms;
  double p95_ms;
};

// Builds one sealed segment with the given index and measures searches
// through the query executor. The executor has no cache, so every run does
// the full search.
RowResult BenchRow(core::IndexType type, core::Dimension dim,
                   const std::vector<core::Vector>& vectors,
                   const std::vector<core::VectorId>& ids,
                   const std::vector<core::Vector>& queries) {
  const auto collection_id = core::MakeCollectionId(1);
  index::IndexFactory factory;
  storage::SegmentManager segments((g_scratch_dir / "segments").string(),
                                   &factory);

  const auto label = std::string(IndexName(type)) + " dim=" +
                     std::to_string(dim) + " n=" +
                     std::to_string(vectors.size());

  auto build_start = std::chrono::steady_clock::now();
  auto segment_id = segments.CreateSegment(collection_id, dim,
                                           core::MetricType::L2);
  if (!segment_id.ok()) Fail("CreateSegment (" + label + ")", segment_id.status());

  auto status = segments.WriteVectors(*segment_id, vectors, ids);
  if (!status.ok()) Fail("WriteVectors (" + label + ")", status);

  core::IndexConfig config;
  config.index_type = type;
  config.dimension = dim;
  config.metric_type = core::MetricType::L2;
  status = segments.SealSegment(*segment_id, config);
  if (!status.ok()) Fail("SealSegment (" + label + ")", status);
  auto build_end = std::chrono::steady_clock::now();

  compute::QueryExecutor executor(&segments);

  auto run_once = [&](const core::Vector& query) {
    auto result = executor.Search(collection_id, query, kTopK);
    if (!result.ok()) Fail("Search (" + label + ")", result.status());
    if (result->entries.size() != static_cast<size_t>(kTopK)) {
      Fail("Search (" + label + ") returned " +
           std::to_string(result->entries.size()) + " results, expected " +
           std::to_string(kTopK));
    }
  };

  for (int i = 0; i < kWarmup; ++i) run_once(queries[i % queries.size()]);

  std::vector<double> times;
  times.reserve(kRuns);
  for (int i = 0; i < kRuns; ++i) {
    const auto& query = queries[i % queries.size()];
    auto start = std::chrono::steady_clock::now();
    run_once(query);
    auto end = std::chrono::steady_clock::now();
    times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
  }
  std::sort(times.begin(), times.end());

  RowResult row;
  row.build_s = std::chrono::duration<double>(build_end - build_start).count();
  row.median_ms = times[times.size() / 2];
  row.p95_ms = times[(times.size() * 95) / 100];
  return row;
}

void PrintHeader() {
  std::printf("  %5s  |  %5s  |  %8s  |  %9s  |  %11s  |  %11s  |  %8s\n",
              "Index", "Dim", "Vectors", "Build", "Median", "p95", "QPS");
  std::printf("  %5s--+--%5s--+--%8s--+--%9s--+--%11s--+--%11s--+--%8s\n",
              "-----", "-----", "--------", "---------", "-----------",
              "-----------", "--------");
}

void PrintRow(core::IndexType type, core::Dimension dim, size_t n,
              const RowResult& row) {
  double qps = row.median_ms > 0 ? 1000.0 / row.median_ms : 0;
  std::printf("  %5s  |  %5d  |  %8zu  |  %7.2f s  |  %8.3f ms  |  %8.3f ms  |  %8.0f\n",
              IndexName(type), dim, n, row.build_s, row.median_ms, row.p95_ms,
              qps);
  std::fflush(stdout);
}

}  // namespace

int main() {
  g_scratch_dir = MakeScratchDir();

  std::printf("GVDB CPU Search Benchmark (QueryExecutor over FAISS segments)\n");
  std::printf("==============================================================\n\n");
  std::printf("L2, top-k=%d, median and p95 of %d searches (%d distinct queries),\n",
              kTopK, kRuns, kDistinctQueries);
  const core::IndexConfig defaults;
  std::printf("one sealed segment per row, HNSW M=%d ef_construction=%d ef_search=%d\n\n",
              defaults.hnsw_params.M, defaults.hnsw_params.ef_construction,
              defaults.hnsw_params.ef_search);
  PrintHeader();

  const core::Dimension dims[] = {128, 768};
  const size_t counts[] = {10000, 100000};
  const core::IndexType types[] = {core::IndexType::FLAT, core::IndexType::HNSW};

  auto total_start = std::chrono::steady_clock::now();
  try {
    for (core::Dimension dim : dims) {
      auto queries = RandomVectors(kDistinctQueries, dim, 99);
      for (size_t n : counts) {
        auto vectors = RandomVectors(n, dim, 42);
        auto ids = SequentialIds(n);
        for (core::IndexType type : types) {
          PrintRow(type, dim, n, BenchRow(type, dim, vectors, ids, queries));
        }
      }
    }
  } catch (const std::exception& ex) {
    // For example a filesystem error from the segment manager, or a FAISS
    // exception that escaped the index wrapper.
    Fail(std::string("unexpected exception: ") + ex.what());
  }
  auto total_s = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - total_start).count();

  std::printf("\nDone in %.1f s.\n", total_s);
  Cleanup();
  return 0;
}
