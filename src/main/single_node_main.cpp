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
#include "compute/query_executor.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
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
    auto segment_manager = std::make_shared<storage::SegmentManager>(
        data_dir + "/segments", index_factory.get());
    segment_manager->LoadAllSegments();
    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_manager.get());

    // 3. Cluster coordinator
    auto shard_manager = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    auto node_registry = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));
    auto coordinator = std::make_unique<cluster::Coordinator>(
        shard_manager, node_registry);

    // 4. gRPC service
    auto resolver = network::MakeLocalResolver(segment_manager);
    auto service = std::make_unique<network::VectorDBService>(
        segment_manager, query_executor, std::move(resolver));

    // 5. Start server
    std::string server_address = absl::StrCat("0.0.0.0:", port);
    auto server = utils::ServerBootstrap::StartGrpcServer(
        server_address, {service.get()});
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