// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace YAML {
class Node;
}

namespace gvdb {
namespace utils {

/**
 * @brief Server configuration options
 */
struct TlsConfig {
  bool enabled = false;
  std::string cert_path;       // Server certificate (PEM)
  std::string key_path;        // Server private key (PEM)
  std::string ca_cert_path;    // CA certificate for client verification (PEM, optional)
  bool mutual_tls = false;     // Require client certificates
};

struct ApiKeyConfig {
  std::string key;
  std::string role;                        // "admin", "readwrite", "readonly", "collection_admin"
  std::vector<std::string> collections;    // empty or ["*"] = all collections
};

struct AuthConfig {
  bool enabled = false;
  std::vector<std::string> api_keys;       // Legacy: flat keys treated as admin
  std::vector<ApiKeyConfig> roles;         // Role-based keys
};

struct ServerConfig {
  std::string bind_address = "0.0.0.0";
  int grpc_port = 50051;
  int metrics_port = 9090;
  int max_message_size_mb = 256;
  int max_concurrent_streams = 1000;
  TlsConfig tls;
  AuthConfig auth;
};

/**
 * @brief Storage configuration options
 */
struct StorageConfig {
  std::string data_dir = "/tmp/gvdb";
  size_t segment_max_size_mb = 512;
  size_t wal_buffer_size_mb = 64;
  bool enable_compression = true;
  int compaction_threads = 4;
};

/**
 * @brief Index configuration options
 */
struct IndexConfig {
  std::string default_index_type = "HNSW";
  size_t hnsw_m = 16;              // HNSW parameter M
  size_t hnsw_ef_construction = 200; // HNSW ef_construction
  size_t hnsw_ef_search = 100;      // HNSW ef_search (default)
  size_t ivf_nlist = 100;           // IVF number of clusters
  bool use_gpu = false;
};

/**
 * @brief Logging configuration options
 */
struct LoggingConfig {
  std::string level = "info";
  bool console_enabled = true;
  bool file_enabled = true;
  std::string file_path = "/tmp/gvdb/logs/gvdb.log";
  size_t max_file_size_mb = 100;
  size_t max_files = 10;
};

/**
 * @brief Consensus configuration options
 */
struct ConsensusConfig {
  int node_id = 1;
  bool single_node_mode = true;
  std::vector<std::string> peers;  // List of peer addresses
  int election_timeout_ms = 5000;
  int heartbeat_interval_ms = 1000;
};

/**
 * @brief Complete GVDB configuration
 */
struct GVDBConfig {
  ServerConfig server;
  StorageConfig storage;
  IndexConfig index;
  LoggingConfig logging;
  ConsensusConfig consensus;
};

/**
 * @brief Configuration manager for YAML-based config files
 *
 * Supports loading configuration from YAML files with:
 * - Type-safe access to config values
 * - Default values for missing keys
 * - Validation of configuration values
 *
 * Example YAML:
 * ```yaml
 * server:
 *   bind_address: "0.0.0.0"
 *   grpc_port: 50051
 *   metrics_port: 9090
 *
 * storage:
 *   data_dir: "/var/lib/gvdb"
 *   segment_max_size_mb: 512
 *
 * index:
 *   default_index_type: "HNSW"
 *   hnsw_m: 16
 * ```
 */
class Config {
 public:
  /**
   * @brief Load configuration from YAML file
   * @param config_path Path to YAML configuration file
   * @return GVDBConfig structure or error status
   */
  static absl::StatusOr<GVDBConfig> load_from_file(const std::string& config_path);

  /**
   * @brief Get default configuration
   * @return Default GVDBConfig with sensible defaults
   */
  static GVDBConfig get_default();

  /**
   * @brief Validate configuration values
   * @param config Configuration to validate
   * @return OK status if valid, error otherwise
   */
  static absl::Status validate(const GVDBConfig& config);

  /**
   * @brief Save configuration to YAML file
   * @param config Configuration to save
   * @param config_path Path to write YAML file
   * @return OK status on success
   */
  static absl::Status save_to_file(const GVDBConfig& config, const std::string& config_path);

 private:
  static ServerConfig parse_server_config(const YAML::Node& node);
  static StorageConfig parse_storage_config(const YAML::Node& node);
  static IndexConfig parse_index_config(const YAML::Node& node);
  static LoggingConfig parse_logging_config(const YAML::Node& node);
  static ConsensusConfig parse_consensus_config(const YAML::Node& node);
};

}  // namespace utils
}  // namespace gvdb