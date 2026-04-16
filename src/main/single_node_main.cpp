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
#include "storage/bulk_importer.h"
#ifdef GVDB_HAS_S3
#include "storage/s3_object_store.h"
#endif
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

    // Optionally wrap in tiered storage (S3/MinIO)
    std::shared_ptr<storage::ISegmentStore> segment_store;
    storage::IObjectStore* object_store_ptr = nullptr;  // for BulkImporter
#ifdef GVDB_HAS_S3
    if (!config.storage.object_store_endpoint.empty()) {
      storage::S3Config s3_config;
      s3_config.endpoint = config.storage.object_store_endpoint;
      s3_config.access_key = config.storage.object_store_access_key;
      s3_config.secret_key = config.storage.object_store_secret_key;
      s3_config.bucket = config.storage.object_store_bucket;
      s3_config.region = config.storage.object_store_region;
      s3_config.use_ssl = config.storage.object_store_use_ssl;
      s3_config.path_style = (config.storage.object_store_type == "minio");

      auto s3_result = storage::S3ObjectStore::Create(s3_config);
      if (!s3_result.ok()) {
        std::cerr << "Failed to create S3 client: " << s3_result.status().message() << std::endl;
        return 1;
      }

      auto cache_dir = data_dir + "/cache";
      auto cache_size = static_cast<size_t>(config.storage.object_store_cache_size_mb) * 1024 * 1024;
      auto cache = std::make_unique<storage::SegmentCache>(cache_dir, cache_size);
      auto prefix = config.storage.object_store_prefix.empty()
          ? "gvdb" : config.storage.object_store_prefix;

      object_store_ptr = s3_result->get();
      segment_store = std::make_shared<storage::TieredSegmentManager>(
          std::move(local_manager), std::move(*s3_result),
          std::move(cache), prefix, config.storage.object_store_upload_threads);
    } else {
      segment_store = std::shared_ptr<storage::SegmentManager>(
          std::move(local_manager));
    }
#else
    segment_store = std::shared_ptr<storage::SegmentManager>(
        std::move(local_manager));
#endif

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

    // 7. gRPC service
    auto resolver = network::MakeLocalResolver(segment_store);
    auto service = std::make_unique<network::VectorDBService>(
        segment_store, query_executor, std::move(resolver), rbac_store,
        bulk_importer);

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