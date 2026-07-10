// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "consensus/raft_node.h"
#include "consensus/raft_config.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "storage/segment_manager.h"
#include "storage/tiered_segment_manager.h"
#include "storage/segment_cache.h"
#include "storage/backup_manager.h"
#include "storage/bulk_importer.h"
#include "cluster/shard_write_gate.h"
#include "storage/object_store_factory.h"
#include "compute/query_executor.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "network/auth_processor.h"
#include "network/audit_interceptor.h"
#include "utils/audit_logger.h"
#include "auth/rbac.h"
#include "index/index_factory.h"
#include "utils/server_bootstrap.h"
#include "utils/config.h"

// Command-line arguments
std::string g_config_file = "";
int g_port = -1;
std::string g_data_dir = "";
int g_node_id = -1;

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --config FILE        YAML config file (optional)\n"
            << "  --port PORT          gRPC server port (overrides config, default: 50051)\n"
            << "  --data-dir PATH      Data directory (overrides config, default: /tmp/gvdb)\n"
            << "  --node-id ID         Node ID (overrides config, default: 1)\n"
            << "  --help               Show this help message\n";
}

bool ParseArgs(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
    } else if (arg == "--config" && i + 1 < argc) {
      g_config_file = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      g_port = std::stoi(argv[++i]);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      g_data_dir = argv[++i];
    } else if (arg == "--node-id" && i + 1 < argc) {
      g_node_id = std::stoi(argv[++i]);
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }
  return true;
}

gvdb::utils::LogLevel ParseLogLevel(const std::string& level_str) {
  std::string lower = level_str;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  if (lower == "debug") return gvdb::utils::LogLevel::DEBUG;
  if (lower == "warn" || lower == "warning") return gvdb::utils::LogLevel::WARN;
  if (lower == "error") return gvdb::utils::LogLevel::ERROR;
  return gvdb::utils::LogLevel::INFO;
}

int main(int argc, char** argv) {
  if (!ParseArgs(argc, argv)) return 1;

  using namespace gvdb;
  utils::ServerBootstrap::InstallSignalHandlers();

  // Load configuration: CLI flags > YAML > defaults
  utils::GVDBConfig config = utils::Config::get_default();
  if (!g_config_file.empty()) {
    auto yaml_result = utils::Config::load_from_file(g_config_file);
    if (!yaml_result.ok()) {
      std::cerr << "Failed to load config: " << yaml_result.status().message() << std::endl;
      return 1;
    }
    config = std::move(yaml_result.value());
  }
  if (g_port != -1) config.server.grpc_port = g_port;
  if (!g_data_dir.empty()) config.storage.data_dir = g_data_dir;
  if (g_node_id != -1) config.consensus.node_id = g_node_id;

  auto validate_status = utils::Config::validate(config);
  if (!validate_status.ok()) {
    std::cerr << "Invalid configuration: " << validate_status.message() << std::endl;
    return 1;
  }

  int port = config.server.grpc_port;
  std::string data_dir = config.storage.data_dir;
  int node_id = config.consensus.node_id;

  // Initialize shared infrastructure
  auto log_status = utils::ServerBootstrap::InitializeLogger(
      data_dir, "gvdb.log", ParseLogLevel(config.logging.level));
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB All-in-One Server");
  utils::ServerBootstrap::StartMetricsServer(9090);

  try {
    // 1. Consensus (single-node mode)
    consensus::RaftConfig raft_config;
    raft_config.node_id = node_id;
    raft_config.single_node_mode = true;
    raft_config.data_dir = data_dir + "/raft";

    auto raft_node = std::make_unique<consensus::RaftNode>(raft_config);
    auto status = raft_node->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start Raft: " << status.message() << std::endl;
      return 1;
    }

    // 2. Storage + compute
    auto index_factory = std::make_unique<index::IndexFactory>();
    auto local_manager = std::make_unique<storage::SegmentManager>(
        data_dir + "/segments", index_factory.get());

    // Optionally wrap in tiered storage. The backend (S3 / MinIO / GCS) is
    // selected from config by the shared factory; an empty result means
    // object storage is disabled and we run local-disk only.
    std::shared_ptr<storage::ISegmentStore> segment_store;
    storage::IObjectStore* object_store_ptr = nullptr;  // for BulkImporter
    auto object_store_or = storage::CreateObjectStore(config.storage);
    if (!object_store_or.ok()) {
      std::cerr << "Failed to create object store: "
                << object_store_or.status().message() << std::endl;
      return 1;
    }
    if (*object_store_or) {
      auto cache_dir = data_dir + "/cache";
      auto cache_size = static_cast<size_t>(config.storage.object_store_cache_size_mb) * 1024 * 1024;
      auto cache = std::make_unique<storage::SegmentCache>(cache_dir, cache_size);
      auto prefix = config.storage.object_store_prefix.empty()
          ? "gvdb" : config.storage.object_store_prefix;

      object_store_ptr = object_store_or->get();
      segment_store = std::make_shared<storage::TieredSegmentManager>(
          std::move(local_manager), std::move(*object_store_or),
          std::move(cache), prefix, config.storage.object_store_upload_threads);
    } else {
      segment_store = std::shared_ptr<storage::SegmentManager>(
          std::move(local_manager));
    }

    segment_store->LoadAllSegments();
    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_store.get());
    query_executor->SetCache(std::make_shared<utils::QueryCache>(10000));

    // Wire auto-seal: when a segment fills up, seal it inline
    auto* index_factory_ptr = index_factory.get();
    segment_store->SetSealCallback(
        [segment_store, index_factory_ptr](core::SegmentId sid, core::IndexType idx_type) {
          auto* seg = segment_store->GetSegment(sid);
          if (!seg) return;
          auto config = core::ResolveAutoIndexConfig(
              idx_type, seg->GetVectorCount(), seg->GetDimension(), seg->GetMetric());
          utils::Logger::Instance().Info("Auto-sealing segment {} ({} vectors, index={})",
              core::ToUInt32(sid), seg->GetVectorCount(),
              static_cast<int>(config.index_type));
          auto status = segment_store->SealSegment(sid, config);
          if (!status.ok()) {
            utils::Logger::Instance().Error("Auto-seal failed: {}", status.message());
          }
        });

    // Background TTL sweep thread
    std::thread ttl_sweep_thread([segment_store]() {
      segment_store->RunTTLSweepLoop(utils::ServerBootstrap::ShutdownFlag());
    });

    // 3. Cluster coordinator
    auto shard_manager = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    auto node_registry = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));
    auto coordinator = std::make_unique<cluster::Coordinator>(
        shard_manager, node_registry);

    // 4. RBAC (if auth enabled in config)
    std::shared_ptr<auth::RbacStore> rbac_store;
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
    if (config.server.auth.enabled) {
      auto rbac_result = auth::RbacStore::Create(config.server.auth);
      if (!rbac_result.ok()) {
        std::cerr << "Invalid auth config: " << rbac_result.status().message() << std::endl;
        return 1;
      }
      rbac_store = std::move(*rbac_result);
      interceptors.push_back(
          std::make_unique<network::ApiKeyAuthInterceptorFactory>(rbac_store));
    }

    // 5. Audit logging (if enabled in config)
    if (config.logging.audit.enabled) {
      utils::AuditLogger::Initialize(config.logging.audit);
      interceptors.push_back(
          std::make_unique<network::AuditInterceptorFactory>());
    }

    // 6. Bulk importer (optional — requires object store)
    std::shared_ptr<storage::BulkImporter> bulk_importer;
    if (object_store_ptr) {
      bulk_importer = std::make_shared<storage::BulkImporter>(
          segment_store, object_store_ptr, data_dir + "/tmp", 2);
    }

    // 7. Backup / restore engines. Single-node has no cluster fan-out;
    // BackupManager runs the single-shard path against the local
    // segment store. Requires either an S3 store (for S3 targets) or a
    // local backup directory (for LocalBackupTarget); we configure both
    // when available so operators can choose at request time.
    std::shared_ptr<storage::BackupManager> backup_manager;
    std::shared_ptr<storage::RestoreManager> restore_manager;
    {
      storage::BackupManagerOptions bopts;
      bopts.default_object_store = object_store_ptr;
      bopts.s3_bucket = config.storage.object_store_bucket;
      bopts.local_root_allowlist = config.storage.local_backup_dir;
      bopts.flushed_segments_root = data_dir + "/segments";
      bopts.tmp_dir = data_dir + "/tmp/backup";
      bopts.gvdb_version = "single-node";
      bopts.node_id = node_id;
      backup_manager = std::make_shared<storage::BackupManager>(std::move(bopts));

      storage::RestoreManagerOptions ropts;
      ropts.default_object_store = object_store_ptr;
      ropts.s3_bucket = config.storage.object_store_bucket;
      ropts.local_root_allowlist = config.storage.local_backup_dir;
      ropts.staging_dir = data_dir + "/tmp/restore";
      restore_manager = std::make_shared<storage::RestoreManager>(
          std::move(ropts));
    }

    // Per-shard backup write fence. Single-node uses a single shard but
    // the gate plumbing is the same — FreezeWrites against shard 0
    // works identically.
    auto shard_write_gate = std::make_unique<cluster::ShardWriteGate>();

    // 8. gRPC service
    auto resolver = network::MakeLocalResolver(segment_store);
    auto service = std::make_unique<network::VectorDBService>(
        segment_store, query_executor, std::move(resolver), rbac_store,
        bulk_importer, backup_manager, restore_manager);
    service->SetShardWriteGate(shard_write_gate.get());

    // 6. Start server
    std::string server_address = absl::StrCat("0.0.0.0:", port);
    auto credentials = utils::ServerBootstrap::MakeServerCredentials(config.server.tls);
    auto server = utils::ServerBootstrap::StartGrpcServer(
        server_address, {service.get()}, credentials, std::move(interceptors));
    if (!server) {
      std::cerr << "Failed to start gRPC server on " << server_address << std::endl;
      return 1;
    }

    utils::ServerBootstrap::PrintBanner("GVDB All-in-One Server", {
        "gRPC Service: " + server_address,
        "Metrics: http://0.0.0.0:9090/metrics",
        "Node ID: " + std::to_string(node_id),
        "Data Directory: " + data_dir,
    });

    utils::ServerBootstrap::WaitForShutdown();

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    if (ttl_sweep_thread.joinable()) ttl_sweep_thread.join();
    server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    (void)raft_node->Shutdown();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}