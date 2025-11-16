#ifndef GVDB_CORE_CONFIG_H_
#define GVDB_CORE_CONFIG_H_

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

  // General parameters
  bool use_gpu = false;        // Use GPU acceleration if available
  int num_threads = 0;         // Number of threads (0 = auto-detect)

  // Validation
  [[nodiscard]] bool IsValid() const {
    return dimension > 0;
  }
};

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
