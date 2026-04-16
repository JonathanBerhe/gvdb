// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_CORE_CONFIG_H_
#define GVDB_CORE_CONFIG_H_

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <string>

#include "core/types.h"

namespace gvdb {
namespace core {

// ============================================================================
// IndexConfig - Configuration for vector index creation
// ============================================================================
struct IndexConfig {
  // Index type (FLAT, HNSW, IVF_FLAT, etc.)
  IndexType index_type = IndexType::FLAT;

  // Vector dimension
  Dimension dimension = 0;

  // Distance metric
  MetricType metric_type = MetricType::L2;

  // HNSW-specific parameters
  struct HNSWParams {
    int M = 16;                // Number of connections per layer
    int ef_construction = 200; // Size of dynamic candidate list during construction
    int ef_search = 100;       // Size of dynamic candidate list during search
  } hnsw_params;

  // IVF-specific parameters
  struct IVFParams {
    int nlist = 100;           // Number of clusters/cells
    int nprobe = 10;           // Number of clusters to search
    bool use_quantization = false; // Use PQ or SQ
    int pq_m = 8;              // Number of subquantizers for PQ
    int pq_nbits = 8;          // Number of bits per subquantizer
  } ivf_params;

  // TurboQuant-specific parameters
  struct TurboQuantParams {
    int bit_width = 4;           // Bits per dimension (1, 2, 4, or 8)
    bool use_qjl = true;        // Enable QJL bias correction (Stage 2)
    int qjl_projection_dim = 0; // QJL projection dimensions (0 = auto: padded_dim/4)
  } turboquant_params;

  // IVF + TurboQuant parameters
  struct IVFTurboQuantParams {
    int nlist = 100;             // Number of IVF clusters
    int nprobe = 10;             // Clusters to search at query time
    int bit_width = 4;           // TurboQuant bits per dimension (1, 2, 4, 8)
    bool use_qjl = true;        // Enable QJL bias correction
    int qjl_dim = 0;            // QJL projection dim (0 = auto)
  } ivf_turboquant_params;

  // General parameters
  bool use_gpu = false;        // Use GPU acceleration if available
  int num_threads = 0;         // Number of threads (0 = auto-detect)

  // Validation
  [[nodiscard]] bool IsValid() const {
    return dimension > 0;
  }
};

// ============================================================================
// Adaptive index parameter constants
// ============================================================================

// Sub-tier boundary within HNSW range
constexpr size_t kAutoTierSmallHnswMax = 100'000;  // 10K–100K → small HNSW

// HNSW parameters — small graphs (10K–100K vectors)
constexpr int kSmallHnswM = 12;
constexpr int kSmallHnswEfConstruction = 128;
constexpr int kSmallHnswEfSearch = 64;

// HNSW parameters — large graphs (100K–500K vectors)
constexpr int kLargeHnswM = 16;
constexpr int kLargeHnswEfConstruction = 200;
constexpr int kLargeHnswEfSearch = 100;

// High-dimensional vectors need more graph edges to maintain recall
constexpr Dimension kHighDimThreshold = 512;
constexpr int kHighDimHnswM = 20;

// ============================================================================
// ResolveAutoIndexConfig - Adaptive index parameters based on segment profile
// ============================================================================
// Returns a complete IndexConfig with parameters tuned for the given vector
// count and dimension. For non-AUTO types, returns default parameters.
// Tier boundaries from core/types.h: kAutoTierFlatMax, kAutoTierHnswMax, etc.

// Compute IVF nlist (≈sqrt(n)) and nprobe (≈sqrt(nlist)) for a given vector count.
inline void ComputeIvfParams(size_t vector_count, int& nlist, int& nprobe) {
  nlist = static_cast<int>(std::sqrt(static_cast<double>(vector_count)));
  nprobe = std::max(1, static_cast<int>(std::sqrt(static_cast<double>(nlist))));
}

inline IndexConfig ResolveAutoIndexConfig(IndexType type, size_t vector_count,
                                          Dimension dimension, MetricType metric) {
  IndexConfig config;
  config.index_type = ResolveAutoIndexType(type, vector_count);
  config.dimension = dimension;
  config.metric_type = metric;

  if (type != IndexType::AUTO) return config;

  if (vector_count < kAutoTierFlatMax) {
    // FLAT — brute force, no parameters to tune

  } else if (vector_count < kAutoTierSmallHnswMax) {
    // Small HNSW: lower connectivity sufficient for small graphs
    config.hnsw_params.M = kSmallHnswM;
    config.hnsw_params.ef_construction = kSmallHnswEfConstruction;
    config.hnsw_params.ef_search = kSmallHnswEfSearch;

  } else if (vector_count < kAutoTierHnswMax) {
    // Large HNSW: full connectivity for recall in denser graphs
    config.hnsw_params.M = kLargeHnswM;
    config.hnsw_params.ef_construction = kLargeHnswEfConstruction;
    config.hnsw_params.ef_search = kLargeHnswEfSearch;
    if (dimension > kHighDimThreshold) {
      config.hnsw_params.M = kHighDimHnswM;
    }

  } else if (vector_count < kAutoTierSqMax) {
    // IVF_SQ: scalar quantization (float32→int8), 4x memory savings
    ComputeIvfParams(vector_count, config.ivf_params.nlist, config.ivf_params.nprobe);
    config.ivf_params.use_quantization = true;

  } else if (vector_count < kAutoTierTQ4Max) {
    // IVF_TURBOQUANT 4-bit
    ComputeIvfParams(vector_count, config.ivf_turboquant_params.nlist,
                     config.ivf_turboquant_params.nprobe);
    config.ivf_turboquant_params.bit_width = 4;

  } else {
    // IVF_TURBOQUANT 2-bit (≥10M vectors)
    ComputeIvfParams(vector_count, config.ivf_turboquant_params.nlist,
                     config.ivf_turboquant_params.nprobe);
    config.ivf_turboquant_params.bit_width = 2;
  }

  return config;
}

// ============================================================================
// StorageConfig - Configuration for storage backend
// ============================================================================
struct StorageConfig {
  // Storage type
  enum class Type {
    LOCAL_DISK,    // Local file system
    S3,            // Amazon S3
    MINIO,         // MinIO object storage
    MEMORY         // In-memory only (for testing)
  } type = Type::LOCAL_DISK;

  // Base path for storage
  std::string base_path = "./data";

  // S3/MinIO configuration
  struct ObjectStoreParams {
    std::string endpoint;
    std::string access_key;
    std::string secret_key;
    std::string bucket;
    bool use_ssl = true;
    std::string region;           // e.g., "us-east-1"
    std::string prefix;           // S3 key prefix, e.g., "gvdb"
    std::string local_cache_dir;  // defaults to {base_path}/cache if empty
    size_t local_cache_size = 256 * 1024 * 1024;  // 256 MB
    int upload_threads = 2;
  } object_store_params;

  // Segment configuration
  size_t max_segment_size = 64 * 1024 * 1024;  // 64 MB
  size_t segment_buffer_size = 4 * 1024 * 1024; // 4 MB

  // WAL configuration
  bool enable_wal = true;
  size_t wal_buffer_size = 16 * 1024 * 1024;    // 16 MB
  size_t wal_sync_interval_ms = 1000;            // 1 second

  // Cache configuration
  size_t cache_size = 256 * 1024 * 1024;        // 256 MB

  // Compaction configuration
  bool auto_compaction = true;
  size_t compaction_threshold = 100 * 1024 * 1024; // 100 MB

  // Validation
  [[nodiscard]] bool IsValid() const {
    return !base_path.empty() && max_segment_size > 0;
  }
};

// ============================================================================
// ClusterConfig - Configuration for cluster/node
// ============================================================================
struct ClusterConfig {
  // Node identification
  NodeId node_id = kInvalidNodeId;
  std::string node_address;
  int node_port = 19530;

  // Role configuration
  enum class NodeRole {
    COORDINATOR,   // Metadata management
    QUERY_NODE,    // Query execution
    DATA_NODE,     // Data storage
    PROXY,         // Client gateway
    ALL_IN_ONE     // Single node with all roles
  } role = NodeRole::ALL_IN_ONE;

  // Coordinator configuration
  std::vector<std::string> coordinator_addresses;

  // Raft consensus configuration
  struct RaftParams {
    int election_timeout_ms = 1000;
    int heartbeat_interval_ms = 100;
    int snapshot_interval = 1000;  // Number of log entries
    std::string raft_data_dir = "./raft";
  } raft_params;

  // Resource limits
  size_t max_memory_bytes = 4ULL * 1024 * 1024 * 1024;  // 4 GB
  int max_cpu_cores = 0;  // 0 = use all available

  // Validation
  [[nodiscard]] bool IsValid() const {
    return node_id != kInvalidNodeId && !node_address.empty() && node_port > 0;
  }
};

// ============================================================================
// QueryConfig - Configuration for query execution
// ============================================================================
struct QueryConfig {
  // Search parameters
  int default_topk = 10;
  int max_topk = 1000;
  float default_radius = 0.0f;

  // Timeout configuration
  int query_timeout_ms = 30000;  // 30 seconds

  // Result limits
  size_t max_result_size = 10000;

  // Validation
  [[nodiscard]] bool IsValid() const {
    return default_topk > 0 && max_topk >= default_topk;
  }
};

// ============================================================================
// SystemConfig - Overall system configuration
// ============================================================================
struct SystemConfig {
  ClusterConfig cluster_config;
  StorageConfig storage_config;
  QueryConfig query_config;

  // Logging configuration
  struct LogConfig {
    enum class Level {
      TRACE,
      DEBUG,
      INFO,
      WARN,
      ERROR,
      CRITICAL
    } level = Level::INFO;

    std::string log_file = "./logs/gvdb.log";
    size_t max_file_size = 100 * 1024 * 1024;  // 100 MB
    int max_files = 10;
  } log_config;

  // Metrics configuration
  struct MetricsConfig {
    bool enable_metrics = true;
    std::string metrics_address = "0.0.0.0";
    int metrics_port = 9091;
    int scrape_interval_ms = 15000;  // 15 seconds
  } metrics_config;

  // Validation
  [[nodiscard]] bool IsValid() const {
    return cluster_config.IsValid() &&
           storage_config.IsValid() &&
           query_config.IsValid();
  }
};

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_CONFIG_H_