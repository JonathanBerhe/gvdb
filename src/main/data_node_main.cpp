// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cluster/data_node.h"
#include "cluster/shard_manager.h"
#include "cluster/heartbeat_sender.h"
#include "storage/segment_manager.h"
#include "storage/tiered_segment_manager.h"
#include "storage/segment_cache.h"
#include "storage/bulk_importer.h"
#ifdef GVDB_HAS_S3
#include "storage/s3_object_store.h"
#endif
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/vectordb_service.h"
#include "network/internal_service.h"
#include "network/collection_resolver.h"
#include "utils/server_bootstrap.h"
#include "utils/env_flags.h"
#include "utils/config.h"
#include "utils/audit_logger.h"
#include "network/audit_interceptor.h"

struct DataNodeArgs {
  int node_id = 101;
  std::string bind_address = "0.0.0.0:50060";
  std::string advertise_address;
  std::string data_dir = "/tmp/gvdb/data_node";
  std::string config_file;
  std::vector<std::string> coordinator_addresses;
  std::vector<int> assigned_shards;
  size_t memory_limit_gb = 8;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID                Data node ID (default: 101)\n"
            << "  --bind-address ADDR         gRPC bind address (default: 0.0.0.0:50060)\n"
            << "  --advertise-address ADDR    Address advertised to coordinator (default: bind-address)\n"
            << "  --data-dir PATH             Data directory (default: /tmp/gvdb/data_node)\n"
            << "  --coordinator ADDR          Coordinator addresses (comma-separated)\n"
            << "  --shards SHARD_IDS          Comma-separated shard IDs\n"
            << "  --config FILE               YAML config file (for S3/MinIO settings)\n"
            << "  --memory-limit-gb SIZE      Memory limit in GB (default: 8)\n"
            << "  --help                      Show this help message\n";
}

std::vector<int> ParseShardIds(const std::string& shard_str) {
  std::vector<int> shards;
  size_t start = 0;
  size_t end = shard_str.find(',');
  while (end != std::string::npos) {
    shards.push_back(std::stoi(shard_str.substr(start, end - start)));
    start = end + 1;
    end = shard_str.find(',', start);
  }
  shards.push_back(std::stoi(shard_str.substr(start)));
  return shards;
}

bool ParseArgs(int argc, char** argv, DataNodeArgs& args) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
    } else if (arg == "--node-id" && i + 1 < argc) {
      args.node_id = std::stoi(argv[++i]);
    } else if (arg == "--bind-address" && i + 1 < argc) {
      args.bind_address = argv[++i];
    } else if (arg == "--advertise-address" && i + 1 < argc) {
      args.advertise_address = argv[++i];
    } else if (arg == "--data-dir" && i + 1 < argc) {
      args.data_dir = argv[++i];
    } else if (arg == "--coordinator" && i + 1 < argc) {
      std::string coords_str = argv[++i];
      size_t start = 0;
      size_t end = coords_str.find(',');
      while (end != std::string::npos) {
        args.coordinator_addresses.push_back(coords_str.substr(start, end - start));
        start = end + 1;
        end = coords_str.find(',', start);
      }
      args.coordinator_addresses.push_back(coords_str.substr(start));
    } else if (arg == "--shards" && i + 1 < argc) {
      args.assigned_shards = ParseShardIds(argv[++i]);
    } else if (arg == "--config" && i + 1 < argc) {
      args.config_file = argv[++i];
    } else if (arg == "--memory-limit-gb" && i + 1 < argc) {
      args.memory_limit_gb = std::stoull(argv[++i]);
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }
  return true;
}

int main(int argc, char** argv) {
  DataNodeArgs args;
  if (!ParseArgs(argc, argv, args)) return 1;

  using namespace gvdb;

  // Env vars override CLI flags
  args.bind_address = utils::ResolveFlag("GVDB_BIND_ADDRESS", args.bind_address);
  args.advertise_address = utils::ResolveFlag("GVDB_ADVERTISE_ADDRESS", args.advertise_address);
  args.data_dir = utils::ResolveFlag("GVDB_DATA_DIR", args.data_dir);
  utils::ServerBootstrap::InstallSignalHandlers();

  auto log_status = utils::ServerBootstrap::InitializeLogger(
      args.data_dir, "data_node.log");
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB Data Node (ID: {})", args.node_id);

  int metrics_port = 9100 + (args.node_id - 100);
  utils::ServerBootstrap::StartMetricsServer(metrics_port);

  try {
    // 1. Storage + compute
    // Load YAML config for S3 settings if provided
    utils::GVDBConfig config = utils::Config::get_default();
    if (!args.config_file.empty()) {
      auto yaml_result = utils::Config::load_from_file(args.config_file);
      if (yaml_result.ok()) {
        config = std::move(*yaml_result);
      }
    }

    auto index_factory = std::make_unique<index::IndexFactory>();
    auto local_manager = std::make_unique<storage::SegmentManager>(
        args.data_dir + "/segments", index_factory.get());

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
      if (s3_result.ok()) {
        auto cache_dir = args.data_dir + "/cache";
        auto cache_size = static_cast<size_t>(
            config.storage.object_store_cache_size_mb) * 1024 * 1024;
        auto cache = std::make_unique<storage::SegmentCache>(
            cache_dir, cache_size);
        auto prefix = config.storage.object_store_prefix.empty()
            ? "gvdb" : config.storage.object_store_prefix;
        object_store_ptr = s3_result->get();
        segment_store = std::make_shared<storage::TieredSegmentManager>(
            std::move(local_manager), std::move(*s3_result),
            std::move(cache), prefix,
            config.storage.object_store_upload_threads);
      } else {
        segment_store = std::shared_ptr<storage::SegmentManager>(
            std::move(local_manager));
      }
    } else {
      segment_store = std::shared_ptr<storage::SegmentManager>(
          std::move(local_manager));
    }
#else
    segment_store = std::shared_ptr<storage::SegmentManager>(
        std::move(local_manager));
#endif

    segment_store->LoadAllSegments();
    auto data_node = std::make_shared<cluster::DataNode>(std::move(index_factory), segment_store);

    // Wire auto-seal: when a segment fills up, queue it for background index building
    segment_store->SetSealCallback(
        [data_node](core::SegmentId sid, core::IndexType idx_type) {
          (void)data_node->ScheduleBuildTask({sid, idx_type, 100});
        });

    // Background thread to process build queue (seals segments + builds indexes)
    std::thread build_thread([data_node]() {
      data_node->RunBuildLoop(utils::ServerBootstrap::ShutdownFlag());
    });

    // Background thread to sweep expired TTL vectors
    std::thread ttl_sweep_thread([data_node]() {
      data_node->RunTTLSweepLoop(utils::ServerBootstrap::ShutdownFlag());
    });

    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_store.get());
    query_executor->SetCache(std::make_shared<utils::QueryCache>(10000));

    // 2. ShardManager + InternalService
    auto shard_manager = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    auto internal_service = std::make_unique<network::InternalService>(
        shard_manager, segment_store, query_executor);

    // 3. Bulk importer (optional — requires object store)
    std::shared_ptr<storage::BulkImporter> bulk_importer;
    if (object_store_ptr) {
      bulk_importer = std::make_shared<storage::BulkImporter>(
          segment_store, object_store_ptr, args.data_dir + "/tmp", 2);
    }

    // 4. VectorDBService
    std::unique_ptr<network::ICollectionResolver> resolver;
    if (!args.coordinator_addresses.empty()) {
      resolver = network::MakeCachedCoordinatorResolver(args.coordinator_addresses[0]);
    } else {
      resolver = network::MakeLocalResolver(segment_store);
    }
    auto service = std::make_unique<network::VectorDBService>(
        segment_store, query_executor, std::move(resolver), nullptr,
        bulk_importer);

    // 4. Audit logging + Start server
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
    if (config.logging.audit.enabled) {
      utils::AuditLogger::Initialize(config.logging.audit);
      interceptors.push_back(
          std::make_unique<network::AuditInterceptorFactory>());
    }

    auto server = utils::ServerBootstrap::StartGrpcServer(
        args.bind_address,
        {internal_service.get(), service.get()},
        grpc::InsecureServerCredentials(),
        std::move(interceptors));
    if (!server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    utils::ServerBootstrap::PrintBanner("GVDB Data Node", {
        "Node ID: " + std::to_string(args.node_id),
        "gRPC Service: " + args.bind_address,
        "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
        "Data Directory: " + args.data_dir,
        "Memory Limit: " + std::to_string(args.memory_limit_gb) + " GB",
    });

    // 5. Heartbeat sender
    std::unique_ptr<cluster::HeartbeatSender> heartbeat;
    if (!args.coordinator_addresses.empty()) {
      std::string heartbeat_addr = args.advertise_address.empty()
          ? args.bind_address : args.advertise_address;
      heartbeat = std::make_unique<cluster::HeartbeatSender>(
          args.coordinator_addresses[0], args.node_id, heartbeat_addr,
          proto::internal::NodeType::NODE_TYPE_DATA_NODE,
          utils::ServerBootstrap::ShutdownFlag());
      heartbeat->Start();
    }

    utils::ServerBootstrap::WaitForShutdown();

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;

    // Notify coordinator that we're draining so it stops routing new work
    // immediately instead of waiting for heartbeat timeout. DrainAndStop()
    // joins the heartbeat loop first so no racing READY heartbeat can
    // overwrite the DRAINING signal at the coordinator (roadmap 0b.1).
    if (heartbeat) heartbeat->DrainAndStop();

    if (build_thread.joinable()) build_thread.join();
    if (ttl_sweep_thread.joinable()) ttl_sweep_thread.join();
    heartbeat.reset();  // no-op join (already stopped above)
    server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}