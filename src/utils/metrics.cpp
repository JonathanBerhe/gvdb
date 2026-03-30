// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/metrics.h"

#include <chrono>

#include "utils/logger.h"

namespace gvdb {
namespace utils {

// ============================================================================
// MetricsRegistry Implementation
// ============================================================================

MetricsRegistry::MetricsRegistry()
    : registry_(std::make_shared<prometheus::Registry>()),
      exposer_(nullptr) {

  // Create metric families with labels

  // Insert request counter: labels = {collection, status}
  insert_requests_total_ = &prometheus::BuildCounter()
                                .Name("gvdb_insert_requests_total")
                                .Help("Total number of insert requests")
                                .Register(*registry_);

  // Insert vectors counter: labels = {collection}
  insert_vectors_total_ = &prometheus::BuildCounter()
                               .Name("gvdb_insert_vectors_total")
                               .Help("Total number of vectors inserted")
                               .Register(*registry_);

  // Search request counter: labels = {collection, status}
  search_requests_total_ = &prometheus::BuildCounter()
                                .Name("gvdb_search_requests_total")
                                .Help("Total number of search requests")
                                .Register(*registry_);

  // gRPC error counter: labels = {error_code}
  grpc_errors_total_ = &prometheus::BuildCounter()
                            .Name("gvdb_grpc_errors_total")
                            .Help("Total number of gRPC errors")
                            .Register(*registry_);

  // Insert latency histogram: labels = {collection}
  // Buckets: 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
  insert_duration_seconds_ =
      &prometheus::BuildHistogram()
           .Name("gvdb_insert_duration_seconds")
           .Help("Insert request duration in seconds")
           .Register(*registry_);

  // Search latency histogram: labels = {collection}
  // Buckets: 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s
  search_duration_seconds_ =
      &prometheus::BuildHistogram()
           .Name("gvdb_search_duration_seconds")
           .Help("Search request duration in seconds")
           .Register(*registry_);

  // Batch size histogram
  // Buckets: 1, 10, 100, 1000, 5000, 10000, 25000, 50000
  insert_batch_size_ = &prometheus::BuildHistogram()
                            .Name("gvdb_insert_batch_size")
                            .Help("Number of vectors per insert batch")
                            .Register(*registry_);

  // Vector count gauge: labels = {collection}
  vector_count_ = &prometheus::BuildGauge()
                       .Name("gvdb_vector_count")
                       .Help("Current number of vectors per collection")
                       .Register(*registry_);

  // Collection count gauge
  collection_count_ = &prometheus::BuildGauge()
                           .Name("gvdb_collection_count")
                           .Help("Current number of collections")
                           .Register(*registry_);

  // Memory usage gauge
  memory_usage_bytes_ = &prometheus::BuildGauge()
                             .Name("gvdb_memory_usage_bytes")
                             .Help("Current memory usage in bytes")
                             .Register(*registry_);
}

MetricsRegistry::~MetricsRegistry() {
  StopMetricsServer();
}

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

bool MetricsRegistry::StartMetricsServer(int port) {
  try {
    // Create exposer on specified port
    std::string bind_address = "0.0.0.0:" + std::to_string(port);
    exposer_ = std::make_unique<prometheus::Exposer>(bind_address);

    // Register our registry with the exposer
    exposer_->RegisterCollectable(registry_);

    Logger::Instance().Info("Metrics server started on {}/metrics", bind_address);
    return true;

  } catch (const std::exception& e) {
    Logger::Instance().Error("Failed to start metrics server: {}", e.what());
    return false;
  }
}

void MetricsRegistry::StopMetricsServer() {
  if (exposer_) {
    Logger::Instance().Info("Stopping metrics server");
    exposer_.reset();
  }
}

// ============================================================================
// Insert Metrics
// ============================================================================

void MetricsRegistry::RecordInsert(const std::string& collection_name,
                                    bool success, size_t vector_count) {
  std::string status = success ? "success" : "error";

  insert_requests_total_
      ->Add({{"collection", collection_name}, {"status", status}})
      .Increment();

  if (success && vector_count > 0) {
    insert_vectors_total_->Add({{"collection", collection_name}})
        .Increment(vector_count);
  }
}

void MetricsRegistry::RecordInsertLatency(const std::string& collection_name,
                                           double duration_seconds) {
  // Use standard Prometheus buckets
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0};

  insert_duration_seconds_->Add({{"collection", collection_name}}, buckets)
      .Observe(duration_seconds);
}

void MetricsRegistry::RecordBatchSize(size_t batch_size) {
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      1, 10, 100, 1000, 5000, 10000, 25000, 50000};

  insert_batch_size_->Add({}, buckets).Observe(static_cast<double>(batch_size));
}

// ============================================================================
// Search Metrics
// ============================================================================

void MetricsRegistry::RecordSearch(const std::string& collection_name,
                                    bool success) {
  std::string status = success ? "success" : "error";

  search_requests_total_
      ->Add({{"collection", collection_name}, {"status", status}})
      .Increment();
}

void MetricsRegistry::RecordSearchLatency(const std::string& collection_name,
                                           double duration_seconds) {
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0};

  search_duration_seconds_->Add({{"collection", collection_name}}, buckets)
      .Observe(duration_seconds);
}

// ============================================================================
// System Metrics
// ============================================================================

void MetricsRegistry::SetVectorCount(const std::string& collection_name,
                                      uint64_t count) {
  vector_count_->Add({{"collection", collection_name}})
      .Set(static_cast<double>(count));
}

void MetricsRegistry::SetCollectionCount(uint64_t count) {
  collection_count_->Add({}).Set(static_cast<double>(count));
}

void MetricsRegistry::SetMemoryUsage(uint64_t bytes) {
  memory_usage_bytes_->Add({}).Set(static_cast<double>(bytes));
}

// ============================================================================
// MetricsTimer Implementation
// ============================================================================

MetricsTimer::MetricsTimer(MetricsRegistry& registry, OperationType type,
                             const std::string& collection_name)
    : registry_(registry),
      type_(type),
      collection_name_(collection_name),
      start_(std::chrono::steady_clock::now()) {}

MetricsTimer::~MetricsTimer() {
  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start_);
  double duration_seconds = duration.count() / 1000000.0;

  switch (type_) {
    case OperationType::INSERT:
      registry_.RecordInsertLatency(collection_name_, duration_seconds);
      break;
    case OperationType::SEARCH:
      registry_.RecordSearchLatency(collection_name_, duration_seconds);
      break;
  }
}

}  // namespace utils
}  // namespace gvdb