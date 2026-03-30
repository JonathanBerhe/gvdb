// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>

#include "prometheus/counter.h"
#include "prometheus/exposer.h"
#include "prometheus/gauge.h"
#include "prometheus/histogram.h"
#include "prometheus/registry.h"

namespace gvdb {
namespace utils {

/**
 * @brief Singleton metrics registry for Prometheus integration
 *
 * Provides centralized metrics collection for:
 * - Request counters (inserts, searches, errors)
 * - Latency histograms
 * - System gauges (memory, vector count)
 *
 * Exposes metrics on HTTP endpoint (default :9090/metrics)
 */
class MetricsRegistry {
 public:
  static MetricsRegistry& Instance();

  // Delete copy/move constructors
  MetricsRegistry(const MetricsRegistry&) = delete;
  MetricsRegistry& operator=(const MetricsRegistry&) = delete;
  MetricsRegistry(MetricsRegistry&&) = delete;
  MetricsRegistry& operator=(MetricsRegistry&&) = delete;

  /**
   * @brief Start HTTP server exposing metrics endpoint
   * @param port Port to listen on (default 9090)
   * @return True if server started successfully
   */
  bool StartMetricsServer(int port = 9090);

  /**
   * @brief Stop metrics server
   */
  void StopMetricsServer();

  // ============================================================================
  // Insert Metrics
  // ============================================================================

  /**
   * @brief Record an insert request
   * @param collection_name Collection being inserted into
   * @param success Whether the insert succeeded
   * @param vector_count Number of vectors inserted
   */
  void RecordInsert(const std::string& collection_name, bool success, size_t vector_count);

  /**
   * @brief Record insert latency
   * @param collection_name Collection being inserted into
   * @param duration_seconds Insert duration in seconds
   */
  void RecordInsertLatency(const std::string& collection_name, double duration_seconds);

  /**
   * @brief Record batch size for insert operation
   * @param batch_size Number of vectors in batch
   */
  void RecordBatchSize(size_t batch_size);

  // ============================================================================
  // Search Metrics
  // ============================================================================

  /**
   * @brief Record a search request
   * @param collection_name Collection being searched
   * @param success Whether the search succeeded
   */
  void RecordSearch(const std::string& collection_name, bool success);

  /**
   * @brief Record search latency
   * @param collection_name Collection being searched
   * @param duration_seconds Search duration in seconds
   */
  void RecordSearchLatency(const std::string& collection_name, double duration_seconds);

  // ============================================================================
  // System Metrics
  // ============================================================================

  /**
   * @brief Update total vector count gauge
   * @param collection_name Collection name
   * @param count Current vector count
   */
  void SetVectorCount(const std::string& collection_name, uint64_t count);

  /**
   * @brief Update collection count gauge
   * @param count Current collection count
   */
  void SetCollectionCount(uint64_t count);

  /**
   * @brief Update memory usage gauge
   * @param bytes Current memory usage in bytes
   */
  void SetMemoryUsage(uint64_t bytes);

 private:
  MetricsRegistry();
  ~MetricsRegistry();

  std::shared_ptr<prometheus::Registry> registry_;
  std::unique_ptr<prometheus::Exposer> exposer_;

  // Request counters
  prometheus::Family<prometheus::Counter>* insert_requests_total_;
  prometheus::Family<prometheus::Counter>* insert_vectors_total_;
  prometheus::Family<prometheus::Counter>* search_requests_total_;
  prometheus::Family<prometheus::Counter>* grpc_errors_total_;

  // Latency histograms (in seconds)
  prometheus::Family<prometheus::Histogram>* insert_duration_seconds_;
  prometheus::Family<prometheus::Histogram>* search_duration_seconds_;

  // Batch size histogram
  prometheus::Family<prometheus::Histogram>* insert_batch_size_;

  // System gauges
  prometheus::Family<prometheus::Gauge>* vector_count_;
  prometheus::Family<prometheus::Gauge>* collection_count_;
  prometheus::Family<prometheus::Gauge>* memory_usage_bytes_;
};

/**
 * @brief RAII timer for automatic latency recording
 *
 * Usage:
 *   {
 *     MetricsTimer timer(MetricsRegistry::Instance(), "insert", "my_collection");
 *     // ... perform operation ...
 *   } // Timer records latency on destruction
 */
class MetricsTimer {
 public:
  enum class OperationType { INSERT, SEARCH };

  MetricsTimer(MetricsRegistry& registry, OperationType type,
               const std::string& collection_name);
  ~MetricsTimer();

  // Delete copy/move
  MetricsTimer(const MetricsTimer&) = delete;
  MetricsTimer& operator=(const MetricsTimer&) = delete;

 private:
  MetricsRegistry& registry_;
  OperationType type_;
  std::string collection_name_;
  std::chrono::steady_clock::time_point start_;
};

}  // namespace utils
}  // namespace gvdb