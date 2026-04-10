// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/config.h"

#include <fstream>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "yaml-cpp/yaml.h"

namespace gvdb {
namespace utils {

// ============================================================================
// Helper Functions
// ============================================================================

template<typename T>
T get_or_default(const YAML::Node& node, const std::string& key, const T& default_value) {
  if (node[key]) {
    return node[key].as<T>();
  }
  return default_value;
}

// ============================================================================
// Config Parsing
// ============================================================================

ServerConfig Config::parse_server_config(const YAML::Node& node) {
  ServerConfig config;

  if (!node) {
    return config;  // Return defaults
  }

  config.bind_address = get_or_default(node, "bind_address", config.bind_address);
  config.grpc_port = get_or_default(node, "grpc_port", config.grpc_port);
  config.metrics_port = get_or_default(node, "metrics_port", config.metrics_port);
  config.max_message_size_mb = get_or_default(node, "max_message_size_mb", config.max_message_size_mb);
  config.max_concurrent_streams = get_or_default(node, "max_concurrent_streams", config.max_concurrent_streams);

  // TLS config
  if (node["tls"]) {
    auto tls_node = node["tls"];
    config.tls.enabled = get_or_default(tls_node, "enabled", config.tls.enabled);
    config.tls.cert_path = get_or_default(tls_node, "cert_path", config.tls.cert_path);
    config.tls.key_path = get_or_default(tls_node, "key_path", config.tls.key_path);
    config.tls.ca_cert_path = get_or_default(tls_node, "ca_cert_path", config.tls.ca_cert_path);
    config.tls.mutual_tls = get_or_default(tls_node, "mutual_tls", config.tls.mutual_tls);
  }

  // Auth config
  if (node["auth"]) {
    auto auth_node = node["auth"];
    config.auth.enabled = get_or_default(auth_node, "enabled", config.auth.enabled);
    if (auth_node["api_keys"] && auth_node["api_keys"].IsSequence()) {
      for (const auto& key : auth_node["api_keys"]) {
        config.auth.api_keys.push_back(key.as<std::string>());
      }
    }
    if (auth_node["roles"] && auth_node["roles"].IsSequence()) {
      for (const auto& role_node : auth_node["roles"]) {
        ApiKeyConfig akc;
        akc.key = get_or_default(role_node, "key", std::string{});
        akc.role = get_or_default(role_node, "role", std::string{"admin"});
        if (role_node["collections"] && role_node["collections"].IsSequence()) {
          for (const auto& c : role_node["collections"]) {
            akc.collections.push_back(c.as<std::string>());
          }
        }
        if (!akc.key.empty()) {
          config.auth.roles.push_back(std::move(akc));
        }
      }
    }
  }

  return config;
}

StorageConfig Config::parse_storage_config(const YAML::Node& node) {
  StorageConfig config;

  if (!node) {
    return config;
  }

  config.data_dir = get_or_default(node, "data_dir", config.data_dir);
  config.segment_max_size_mb = get_or_default(node, "segment_max_size_mb", config.segment_max_size_mb);
  config.wal_buffer_size_mb = get_or_default(node, "wal_buffer_size_mb", config.wal_buffer_size_mb);
  config.enable_compression = get_or_default(node, "enable_compression", config.enable_compression);
  config.compaction_threads = get_or_default(node, "compaction_threads", config.compaction_threads);

  // Object store (S3/MinIO) configuration
  if (node["object_store"]) {
    auto os = node["object_store"];
    config.object_store_type = get_or_default(os, "type", std::string(""));
    config.object_store_endpoint = get_or_default(os, "endpoint", std::string(""));
    config.object_store_access_key = get_or_default(os, "access_key", std::string(""));
    config.object_store_secret_key = get_or_default(os, "secret_key", std::string(""));
    config.object_store_bucket = get_or_default(os, "bucket", std::string(""));
    config.object_store_region = get_or_default(os, "region", std::string("us-east-1"));
    config.object_store_prefix = get_or_default(os, "prefix", std::string("gvdb"));
    config.object_store_use_ssl = get_or_default(os, "use_ssl", true);
    config.object_store_cache_size_mb = get_or_default(os, "local_cache_size_mb", 256);
    config.object_store_upload_threads = get_or_default(os, "upload_threads", 2);
  }

  return config;
}

IndexConfig Config::parse_index_config(const YAML::Node& node) {
  IndexConfig config;

  if (!node) {
    return config;
  }

  config.default_index_type = get_or_default(node, "default_index_type", config.default_index_type);
  config.hnsw_m = get_or_default(node, "hnsw_m", config.hnsw_m);
  config.hnsw_ef_construction = get_or_default(node, "hnsw_ef_construction", config.hnsw_ef_construction);
  config.hnsw_ef_search = get_or_default(node, "hnsw_ef_search", config.hnsw_ef_search);
  config.ivf_nlist = get_or_default(node, "ivf_nlist", config.ivf_nlist);
  config.use_gpu = get_or_default(node, "use_gpu", config.use_gpu);

  return config;
}

LoggingConfig Config::parse_logging_config(const YAML::Node& node) {
  LoggingConfig config;

  if (!node) {
    return config;
  }

  config.level = get_or_default(node, "level", config.level);
  config.console_enabled = get_or_default(node, "console_enabled", config.console_enabled);
  config.file_enabled = get_or_default(node, "file_enabled", config.file_enabled);
  config.file_path = get_or_default(node, "file_path", config.file_path);
  config.max_file_size_mb = get_or_default(node, "max_file_size_mb", config.max_file_size_mb);
  config.max_files = get_or_default(node, "max_files", config.max_files);

  return config;
}

ConsensusConfig Config::parse_consensus_config(const YAML::Node& node) {
  ConsensusConfig config;

  if (!node) {
    return config;
  }

  config.node_id = get_or_default(node, "node_id", config.node_id);
  config.single_node_mode = get_or_default(node, "single_node_mode", config.single_node_mode);
  config.election_timeout_ms = get_or_default(node, "election_timeout_ms", config.election_timeout_ms);
  config.heartbeat_interval_ms = get_or_default(node, "heartbeat_interval_ms", config.heartbeat_interval_ms);

  // Parse peers array
  if (node["peers"] && node["peers"].IsSequence()) {
    for (const auto& peer : node["peers"]) {
      config.peers.push_back(peer.as<std::string>());
    }
  }

  return config;
}

// ============================================================================
// Public API
// ============================================================================

absl::StatusOr<GVDBConfig> Config::load_from_file(const std::string& config_path) {
  try {
    // Load YAML file
    YAML::Node root = YAML::LoadFile(config_path);

    GVDBConfig config;
    config.server = parse_server_config(root["server"]);
    config.storage = parse_storage_config(root["storage"]);
    config.index = parse_index_config(root["index"]);
    config.logging = parse_logging_config(root["logging"]);
    config.consensus = parse_consensus_config(root["consensus"]);

    // Validate configuration
    auto status = validate(config);
    if (!status.ok()) {
      return status;
    }

    return config;

  } catch (const YAML::Exception& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to parse YAML config: ", e.what()));
  } catch (const std::exception& e) {
    return absl::InternalError(
        absl::StrCat("Failed to load config file: ", e.what()));
  }
}

GVDBConfig Config::get_default() {
  GVDBConfig config;
  // All struct members already have default values
  return config;
}

absl::Status Config::validate(const GVDBConfig& config) {
  // Validate server config
  if (config.server.grpc_port <= 0 || config.server.grpc_port > 65535) {
    return absl::InvalidArgumentError("Invalid gRPC port");
  }
  if (config.server.metrics_port <= 0 || config.server.metrics_port > 65535) {
    return absl::InvalidArgumentError("Invalid metrics port");
  }
  if (config.server.max_message_size_mb <= 0) {
    return absl::InvalidArgumentError("Invalid max message size");
  }

  // Validate storage config
  if (config.storage.data_dir.empty()) {
    return absl::InvalidArgumentError("Data directory cannot be empty");
  }
  if (config.storage.segment_max_size_mb == 0) {
    return absl::InvalidArgumentError("Segment max size must be > 0");
  }

  // Validate index config
  if (config.index.hnsw_m == 0) {
    return absl::InvalidArgumentError("HNSW M parameter must be > 0");
  }
  if (config.index.hnsw_ef_construction == 0) {
    return absl::InvalidArgumentError("HNSW ef_construction must be > 0");
  }

  // Validate logging config
  if (config.logging.level != "trace" &&
      config.logging.level != "debug" &&
      config.logging.level != "info" &&
      config.logging.level != "warn" &&
      config.logging.level != "error") {
    return absl::InvalidArgumentError(
        "Invalid log level (must be: trace, debug, info, warn, error)");
  }

  // Validate consensus config
  if (config.consensus.node_id <= 0) {
    return absl::InvalidArgumentError("Node ID must be > 0");
  }
  if (config.consensus.election_timeout_ms <= 0) {
    return absl::InvalidArgumentError("Election timeout must be > 0");
  }
  if (config.consensus.heartbeat_interval_ms <= 0) {
    return absl::InvalidArgumentError("Heartbeat interval must be > 0");
  }

  return absl::OkStatus();
}

absl::Status Config::save_to_file(const GVDBConfig& config, const std::string& config_path) {
  try {
    YAML::Emitter out;
    out << YAML::BeginMap;

    // Server config
    out << YAML::Key << "server" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "bind_address" << YAML::Value << config.server.bind_address;
    out << YAML::Key << "grpc_port" << YAML::Value << config.server.grpc_port;
    out << YAML::Key << "metrics_port" << YAML::Value << config.server.metrics_port;
    out << YAML::Key << "max_message_size_mb" << YAML::Value << config.server.max_message_size_mb;
    out << YAML::Key << "max_concurrent_streams" << YAML::Value << config.server.max_concurrent_streams;
    out << YAML::EndMap;

    // Storage config
    out << YAML::Key << "storage" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "data_dir" << YAML::Value << config.storage.data_dir;
    out << YAML::Key << "segment_max_size_mb" << YAML::Value << config.storage.segment_max_size_mb;
    out << YAML::Key << "wal_buffer_size_mb" << YAML::Value << config.storage.wal_buffer_size_mb;
    out << YAML::Key << "enable_compression" << YAML::Value << config.storage.enable_compression;
    out << YAML::Key << "compaction_threads" << YAML::Value << config.storage.compaction_threads;
    out << YAML::EndMap;

    // Index config
    out << YAML::Key << "index" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "default_index_type" << YAML::Value << config.index.default_index_type;
    out << YAML::Key << "hnsw_m" << YAML::Value << config.index.hnsw_m;
    out << YAML::Key << "hnsw_ef_construction" << YAML::Value << config.index.hnsw_ef_construction;
    out << YAML::Key << "hnsw_ef_search" << YAML::Value << config.index.hnsw_ef_search;
    out << YAML::Key << "ivf_nlist" << YAML::Value << config.index.ivf_nlist;
    out << YAML::Key << "use_gpu" << YAML::Value << config.index.use_gpu;
    out << YAML::EndMap;

    // Logging config
    out << YAML::Key << "logging" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "level" << YAML::Value << config.logging.level;
    out << YAML::Key << "console_enabled" << YAML::Value << config.logging.console_enabled;
    out << YAML::Key << "file_enabled" << YAML::Value << config.logging.file_enabled;
    out << YAML::Key << "file_path" << YAML::Value << config.logging.file_path;
    out << YAML::Key << "max_file_size_mb" << YAML::Value << config.logging.max_file_size_mb;
    out << YAML::Key << "max_files" << YAML::Value << config.logging.max_files;
    out << YAML::EndMap;

    // Consensus config
    out << YAML::Key << "consensus" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "node_id" << YAML::Value << config.consensus.node_id;
    out << YAML::Key << "single_node_mode" << YAML::Value << config.consensus.single_node_mode;
    out << YAML::Key << "election_timeout_ms" << YAML::Value << config.consensus.election_timeout_ms;
    out << YAML::Key << "heartbeat_interval_ms" << YAML::Value << config.consensus.heartbeat_interval_ms;

    if (!config.consensus.peers.empty()) {
      out << YAML::Key << "peers" << YAML::Value << YAML::BeginSeq;
      for (const auto& peer : config.consensus.peers) {
        out << peer;
      }
      out << YAML::EndSeq;
    }
    out << YAML::EndMap;

    out << YAML::EndMap;

    // Write to file
    std::ofstream fout(config_path);
    if (!fout) {
      return absl::InternalError("Failed to open file for writing");
    }
    fout << out.c_str();
    fout.close();

    return absl::OkStatus();

  } catch (const std::exception& e) {
    return absl::InternalError(
        absl::StrCat("Failed to save config file: ", e.what()));
  }
}

}  // namespace utils
}  // namespace gvdb