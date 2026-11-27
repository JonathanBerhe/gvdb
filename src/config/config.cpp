// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "config/config.h"
#include "core/status.h"

#include "absl/strings/str_cat.h"
#include "yaml-cpp/yaml.h"

namespace gvdb::config {

// ============================================================================
// Helper Functions
// ============================================================================

template <typename T>
T GetOrDefault(const YAML::Node& node, const std::string& key, const T& default_value) {
  if (node[key]) {
    return node[key].as<T>();
  }
  return default_value;
}

// ============================================================================
// Config Parsing
// ============================================================================

ServerConfig Config::ParseServerConfig(const YAML::Node& node) {
  ServerConfig config;

  if (!node) {
    return config;  // Return defaults
  }

  config.bind_address = GetOrDefault(node, "bind_address", config.bind_address);
  config.grpc_port = GetOrDefault(node, "grpc_port", config.grpc_port);
  config.metrics_port = GetOrDefault(node, "metrics_port", config.metrics_port);
  config.max_message_size_mb =
      GetOrDefault(node, "max_message_size_mb", config.max_message_size_mb);
  config.max_concurrent_streams =
      GetOrDefault(node, "max_concurrent_streams", config.max_concurrent_streams);

  // TLS config
  if (node["tls"]) {
    auto tls_node = node["tls"];
    config.tls.enabled = GetOrDefault(tls_node, "enabled", config.tls.enabled);
    config.tls.cert_path = GetOrDefault(tls_node, "cert_path", config.tls.cert_path);
    config.tls.key_path = GetOrDefault(tls_node, "key_path", config.tls.key_path);
    config.tls.ca_cert_path = GetOrDefault(tls_node, "ca_cert_path", config.tls.ca_cert_path);
    config.tls.mutual_tls = GetOrDefault(tls_node, "mutual_tls", config.tls.mutual_tls);
  }

  // Auth config
  if (node["auth"]) {
    auto auth_node = node["auth"];
    config.auth.enabled = GetOrDefault(auth_node, "enabled", config.auth.enabled);
    if (auth_node["api_keys"] && auth_node["api_keys"].IsSequence()) {
      for (const auto& key : auth_node["api_keys"]) {
        config.auth.api_keys.push_back(key.as<std::string>());
      }
    }
    if (auth_node["roles"] && auth_node["roles"].IsSequence()) {
      for (const auto& role_node : auth_node["roles"]) {
        auth::ApiKeyConfig akc;
        akc.key = GetOrDefault(role_node, "key", std::string{});
        akc.role = GetOrDefault(role_node, "role", std::string{"admin"});
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

StorageConfig Config::ParseStorageConfig(const YAML::Node& node) {
  StorageConfig config;

  if (!node) {
    return config;
  }

  config.data_dir = GetOrDefault(node, "data_dir", config.data_dir);
  config.segment_max_size_mb =
      GetOrDefault(node, "segment_max_size_mb", config.segment_max_size_mb);
  config.wal_buffer_size_mb = GetOrDefault(node, "wal_buffer_size_mb", config.wal_buffer_size_mb);
  config.enable_compression = GetOrDefault(node, "enable_compression", config.enable_compression);
  config.compaction_threads = GetOrDefault(node, "compaction_threads", config.compaction_threads);
  config.local_backup_dir = GetOrDefault(node, "local_backup_dir", config.local_backup_dir);

  // Object store (S3/MinIO) configuration
  if (node["object_store"]) {
    auto os = node["object_store"];
    config.object_store.type = GetOrDefault(os, "type", std::string(""));
    config.object_store.endpoint = GetOrDefault(os, "endpoint", std::string(""));
    config.object_store.access_key = GetOrDefault(os, "access_key", std::string(""));
    config.object_store.secret_key = GetOrDefault(os, "secret_key", std::string(""));
    config.object_store.bucket = GetOrDefault(os, "bucket", std::string(""));
    config.object_store.region = GetOrDefault(os, "region", std::string("us-east-1"));
    config.object_store.prefix = GetOrDefault(os, "prefix", std::string("gvdb"));
    // GCS-specific (ignored by the S3/MinIO backend).
    config.object_store.project = GetOrDefault(os, "project", std::string(""));
    config.object_store.credentials_path = GetOrDefault(os, "credentials_path", std::string(""));
    config.object_store.use_ssl = GetOrDefault(os, "use_ssl", true);
    config.object_store_cache_size_mb = GetOrDefault(os, "local_cache_size_mb", 256);
    config.object_store_upload_threads = GetOrDefault(os, "upload_threads", 2);
  }

  return config;
}

IndexConfig Config::ParseIndexConfig(const YAML::Node& node) {
  IndexConfig config;

  if (!node) {
    return config;
  }

  config.default_index_type = GetOrDefault(node, "default_index_type", config.default_index_type);
  config.hnsw_m = GetOrDefault(node, "hnsw_m", config.hnsw_m);
  config.hnsw_ef_construction =
      GetOrDefault(node, "hnsw_ef_construction", config.hnsw_ef_construction);
  config.hnsw_ef_search = GetOrDefault(node, "hnsw_ef_search", config.hnsw_ef_search);
  config.ivf_nlist = GetOrDefault(node, "ivf_nlist", config.ivf_nlist);
  config.use_gpu = GetOrDefault(node, "use_gpu", config.use_gpu);

  return config;
}

LoggingConfig Config::ParseLoggingConfig(const YAML::Node& node) {
  LoggingConfig config;

  if (!node) {
    return config;
  }

  config.level = GetOrDefault(node, "level", config.level);
  config.console_enabled = GetOrDefault(node, "console_enabled", config.console_enabled);
  config.file_enabled = GetOrDefault(node, "file_enabled", config.file_enabled);
  config.file_path = GetOrDefault(node, "file_path", config.file_path);
  config.max_file_size_mb = GetOrDefault(node, "max_file_size_mb", config.max_file_size_mb);
  config.max_files = GetOrDefault(node, "max_files", config.max_files);

  if (node["audit"]) {
    auto audit_node = node["audit"];
    config.audit.enabled = GetOrDefault(audit_node, "enabled", config.audit.enabled);
    config.audit.file_path = GetOrDefault(audit_node, "file_path", config.audit.file_path);
    config.audit.max_file_size_mb =
        GetOrDefault(audit_node, "max_file_size_mb", config.audit.max_file_size_mb);
    config.audit.max_files = GetOrDefault(audit_node, "max_files", config.audit.max_files);
  }

  return config;
}

ConsensusConfig Config::ParseConsensusConfig(const YAML::Node& node) {
  ConsensusConfig config;

  if (!node) {
    return config;
  }

  config.node_id = GetOrDefault(node, "node_id", config.node_id);
  config.single_node_mode = GetOrDefault(node, "single_node_mode", config.single_node_mode);
  config.election_timeout_ms =
      GetOrDefault(node, "election_timeout_ms", config.election_timeout_ms);
  config.heartbeat_interval_ms =
      GetOrDefault(node, "heartbeat_interval_ms", config.heartbeat_interval_ms);

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

absl::StatusOr<GVDBConfig> Config::LoadFromFile(const std::string& config_path) {
  try {
    // Load YAML file
    YAML::Node root = YAML::LoadFile(config_path);

    GVDBConfig config;
    config.server = ParseServerConfig(root["server"]);
    config.storage = ParseStorageConfig(root["storage"]);
    config.index = ParseIndexConfig(root["index"]);
    config.logging = ParseLoggingConfig(root["logging"]);
    config.consensus = ParseConsensusConfig(root["consensus"]);

    // Validate configuration
    RETURN_IF_ERROR(Validate(config));

    return config;

  } catch (const YAML::Exception& e) {
    return absl::InvalidArgumentError(absl::StrCat("Failed to parse YAML config: ", e.what()));
  } catch (const std::exception& e) {
    return absl::InternalError(absl::StrCat("Failed to load config file: ", e.what()));
  }
}

GVDBConfig Config::GetDefault() {
  GVDBConfig config;
  // All struct members already have default values
  return config;
}

absl::Status Config::Validate(const GVDBConfig& config) {
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
  if (config.logging.level != "trace" && config.logging.level != "debug" &&
      config.logging.level != "info" && config.logging.level != "warn" &&
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

}  // namespace gvdb::config
