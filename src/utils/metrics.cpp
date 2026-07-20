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

  // Get request counter: labels = {collection, status}
  get_requests_total_ = &prometheus::BuildCounter()
                              .Name("gvdb_get_requests_total")
                              .Help("Total number of get requests")
                              .Register(*registry_);

  // Delete request counter: labels = {collection, status}
  delete_requests_total_ = &prometheus::BuildCounter()
                                .Name("gvdb_delete_requests_total")
                                .Help("Total number of delete requests")
                                .Register(*registry_);

  // UpdateMetadata request counter: labels = {collection, status}
  update_metadata_requests_total_ = &prometheus::BuildCounter()
                                         .Name("gvdb_update_metadata_requests_total")
                                         .Help("Total number of update metadata requests")
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

  // Get latency histogram
  get_duration_seconds_ =
      &prometheus::BuildHistogram()
           .Name("gvdb_get_duration_seconds")
           .Help("Get request duration in seconds")
           .Register(*registry_);

  // Delete latency histogram
  delete_duration_seconds_ =
      &prometheus::BuildHistogram()
           .Name("gvdb_delete_duration_seconds")
           .Help("Delete request duration in seconds")
           .Register(*registry_);

  // UpdateMetadata latency histogram
  update_metadata_duration_seconds_ =
      &prometheus::BuildHistogram()
           .Name("gvdb_update_metadata_duration_seconds")
           .Help("Update metadata request duration in seconds")
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

  // Pending failed tiered-upload gauge. Non-zero means sealed segments are
  // stranded on local disk and have not reached object storage.
  tiered_upload_failed_pending_ =
      &prometheus::BuildGauge()
           .Name("gvdb_tiered_upload_failed_pending")
           .Help("Tiered-storage uploads that failed and are awaiting retry")
           .Register(*registry_);

  // Auto-rebalance counters (roadmap 0b.2). No labels — there is a single
  // coordinator process and a single rebalance subsystem.
  auto_rebalance_triggered_ = &prometheus::BuildCounter()
                                   .Name("gvdb_auto_rebalance_triggered_total")
                                   .Help("Total ExecuteRebalancePlan workers "
                                         "spawned by DetectNewDataNodes")
                                   .Register(*registry_);
  auto_rebalance_debounced_ = &prometheus::BuildCounter()
                                   .Name("gvdb_auto_rebalance_debounced_total")
                                   .Help("Auto-rebalance attempts suppressed "
                                         "by debounce window or overlap guard")
                                   .Register(*registry_);
  auto_rebalance_moves_completed_ =
      &prometheus::BuildCounter()
           .Name("gvdb_auto_rebalance_moves_completed_total")
           .Help("Shard moves completed by auto-rebalance workers")
           .Register(*registry_);
  auto_rebalance_failures_ = &prometheus::BuildCounter()
                                  .Name("gvdb_auto_rebalance_failures_total")
                                  .Help("Auto-rebalance workers that returned "
                                        "a non-OK status")
                                  .Register(*registry_);

  // Read replica-fallback observability. Counters labelled by the logical
  // operation ("get", "hybrid_search", "search", "range_search") so each
  // path can be tracked independently; reasons distinguish whether the
  // primary or a replica was the failed first option.
  read_replica_fallback_total_ =
      &prometheus::BuildCounter()
           .Name("gvdb_read_replica_fallback_total")
           .Help("Read RPCs whose first candidate node failed and that "
                 "succeeded after falling back to a replica")
           .Register(*registry_);
  read_exhausted_replicas_total_ =
      &prometheus::BuildCounter()
           .Name("gvdb_read_exhausted_replicas_total")
           .Help("Read RPCs that exhausted every routable candidate and "
                 "surfaced UNAVAILABLE to the caller")
           .Register(*registry_);
  read_attempts_ = &prometheus::BuildHistogram()
                        .Name("gvdb_read_attempts")
                        .Help("Number of candidate nodes a single read "
                              "tried before succeeding or exhausting")
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
// Get Metrics
// ============================================================================

void MetricsRegistry::RecordGet(const std::string& collection_name, bool success) {
  std::string status = success ? "success" : "error";
  get_requests_total_->Add({{"collection", collection_name}, {"status", status}}).Increment();
}

void MetricsRegistry::RecordGetLatency(const std::string& collection_name, double duration_seconds) {
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0};
  get_duration_seconds_->Add({{"collection", collection_name}}, buckets).Observe(duration_seconds);
}

// ============================================================================
// Delete Metrics
// ============================================================================

void MetricsRegistry::RecordDelete(const std::string& collection_name, bool success) {
  std::string status = success ? "success" : "error";
  delete_requests_total_->Add({{"collection", collection_name}, {"status", status}}).Increment();
}

void MetricsRegistry::RecordDeleteLatency(const std::string& collection_name, double duration_seconds) {
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0};
  delete_duration_seconds_->Add({{"collection", collection_name}}, buckets).Observe(duration_seconds);
}

// ============================================================================
// UpdateMetadata Metrics
// ============================================================================

void MetricsRegistry::RecordUpdateMetadata(const std::string& collection_name, bool success) {
  std::string status = success ? "success" : "error";
  update_metadata_requests_total_->Add({{"collection", collection_name}, {"status", status}}).Increment();
}

void MetricsRegistry::RecordUpdateMetadataLatency(const std::string& collection_name, double duration_seconds) {
  static const auto buckets = prometheus::Histogram::BucketBoundaries{
      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0};
  update_metadata_duration_seconds_->Add({{"collection", collection_name}}, buckets).Observe(duration_seconds);
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

void MetricsRegistry::SetPendingFailedUploads(uint64_t count) {
  tiered_upload_failed_pending_->Add({}).Set(static_cast<double>(count));
}

// ============================================================================
// Auto-Rebalance Metrics (roadmap 0b.2)
// ============================================================================

void MetricsRegistry::IncAutoRebalanceTriggered() {
  auto_rebalance_triggered_->Add({}).Increment();
}

void MetricsRegistry::IncAutoRebalanceDebounced() {
  auto_rebalance_debounced_->Add({}).Increment();
}

void MetricsRegistry::AddAutoRebalanceMovesCompleted(uint64_t moves) {
  auto_rebalance_moves_completed_->Add({}).Increment(
      static_cast<double>(moves));
}

void MetricsRegistry::IncAutoRebalanceFailures() {
  auto_rebalance_failures_->Add({}).Increment();
}

// ============================================================================
// Read Replica Fallback Metrics
// ============================================================================

void MetricsRegistry::IncReadReplicaFallback(const std::string& operation,
                                              const std::string& reason) {
  read_replica_fallback_total_
      ->Add({{"operation", operation}, {"reason", reason}})
      .Increment();
}

void MetricsRegistry::IncReadExhaustedReplicas(const std::string& operation) {
  read_exhausted_replicas_total_->Add({{"operation", operation}}).Increment();
}

void MetricsRegistry::RecordReadAttempts(const std::string& operation,
                                          uint32_t attempts) {
  // Bucket layout sized for typical RF=2-5 clusters: most reads succeed
  // on attempt 1; tail of 2-3 covers fallback to a single replica;
  // ≥4 indicates a fanout cluster or infrastructure trouble.
  static const auto buckets =
      prometheus::Histogram::BucketBoundaries{1, 2, 3, 4, 5, 10};
  read_attempts_->Add({{"operation", operation}}, buckets)
      .Observe(static_cast<double>(attempts));
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
    case OperationType::GET:
      registry_.RecordGetLatency(collection_name_, duration_seconds);
      break;
    case OperationType::DELETE:
      registry_.RecordDeleteLatency(collection_name_, duration_seconds);
      break;
    case OperationType::UPDATE_METADATA:
      registry_.RecordUpdateMetadataLatency(collection_name_, duration_seconds);
      break;
  }
}

}  // namespace utils
}  // namespace gvdb