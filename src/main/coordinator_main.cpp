// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "consensus/raft_node.h"
#include "consensus/raft_config.h"
#include "consensus/timestamp_oracle.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "cluster/node_registry.h"
#include "cluster/internal_client.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/internal_service.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "network/auth_processor.h"
#include "auth/rbac.h"
#include "utils/server_bootstrap.h"
#include "utils/config.h"
#include "utils/env_flags.h"

struct CoordinatorArgs {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50051";
  std::string advertise_address;
  std::string raft_address = "0.0.0.0:8300";
  std::vector<std::string> raft_peers;
  std::string data_dir = "/tmp/gvdb/coordinator";
  std::string config_file;
  bool single_node_mode = true;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID             Node ID (default: 1)\n"
            << "  --bind-address ADDR      gRPC bind address (default: 0.0.0.0:50051)\n"
            << "  --advertise-address ADDR Address advertised to peers (default: bind-address)\n"
            << "  --raft-address ADDR      Raft listen address (default: 0.0.0.0:8300)\n"
            << "  --raft-peers PEERS       Comma-separated Raft peer addresses\n"
            << "  --data-dir PATH          Data directory (default: /tmp/gvdb/coordinator)\n"
            << "  --config FILE            YAML configuration file\n"
            << "  --single-node            Run in single-node mode\n"
            << "  --help                   Show this help message\n";
}

bool ParseArgs(int argc, char** argv, CoordinatorArgs& args) {
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
    } else if (arg == "--raft-address" && i + 1 < argc) {
      args.raft_address = argv[++i];
    } else if (arg == "--raft-peers" && i + 1 < argc) {
      std::string peers_str = argv[++i];
      size_t start = 0;
      size_t end = peers_str.find(',');
      while (end != std::string::npos) {
        args.raft_peers.push_back(peers_str.substr(start, end - start));
        start = end + 1;
        end = peers_str.find(',', start);
      }
      args.raft_peers.push_back(peers_str.substr(start));
      args.single_node_mode = false;
    } else if (arg == "--data-dir" && i + 1 < argc) {
      args.data_dir = argv[++i];
    } else if (arg == "--config" && i + 1 < argc) {
      args.config_file = argv[++i];
    } else if (arg == "--single-node") {
      args.single_node_mode = true;
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }
  return true;
}

int main(int argc, char** argv) {
  CoordinatorArgs args;
  if (!ParseArgs(argc, argv, args)) return 1;

  using namespace gvdb;

  // Env vars override CLI flags
  args.bind_address = utils::ResolveFlag("GVDB_BIND_ADDRESS", args.bind_address);
  args.advertise_address = utils::ResolveFlag("GVDB_ADVERTISE_ADDRESS", args.advertise_address);
  args.data_dir = utils::ResolveFlag("GVDB_DATA_DIR", args.data_dir);
  args.raft_address = utils::ResolveFlag("GVDB_RAFT_ADDRESS", args.raft_address);
  utils::ServerBootstrap::InstallSignalHandlers();

  auto log_status = utils::ServerBootstrap::InitializeLogger(
      args.data_dir, "coordinator.log");
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB Coordinator Node");
  utils::Logger::Instance().Info("  Node ID: {}, Mode: {}",
      args.node_id, args.single_node_mode ? "Single-Node" : "Multi-Node");

  int metrics_port = 9090 + args.node_id;
  utils::ServerBootstrap::StartMetricsServer(metrics_port);

  try {
    // 1. Raft consensus
    consensus::RaftConfig raft_config;
    raft_config.node_id = args.node_id;
    raft_config.single_node_mode = args.single_node_mode;
    raft_config.listen_address = args.raft_address;
    raft_config.peers = args.raft_peers;
    raft_config.data_dir = args.data_dir + "/raft";

    auto raft_node = std::make_unique<consensus::RaftNode>(raft_config);
    auto status = raft_node->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start Raft: " << status.message() << std::endl;
      return 1;
    }

    if (!args.single_node_mode) {
      utils::Logger::Instance().Info("Waiting for leader election...");
      auto start_time = std::chrono::steady_clock::now();
      while (!raft_node->IsLeader() && raft_node->GetLeaderId() < 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(10)) {
          std::cerr << "Leader election timeout." << std::endl;
          return 1;
        }
      }
    }

    // 2. Cluster components
    auto shard_manager = std::make_shared<cluster::ShardManager>(
        16, cluster::ShardingStrategy::HASH);
    auto node_registry = std::make_shared<cluster::NodeRegistry>(
        std::chrono::seconds(30));
    auto client_factory = std::make_shared<cluster::GrpcInternalServiceClientFactory>();
    auto coordinator = std::make_shared<cluster::Coordinator>(
        shard_manager, node_registry, client_factory);

    // 3. TimestampOracle
    auto* tso_ptr = raft_node->GetTimestampOracle();
    std::shared_ptr<consensus::TimestampOracle> timestamp_oracle;
    if (tso_ptr) {
      timestamp_oracle = std::shared_ptr<consensus::TimestampOracle>(
          tso_ptr, [](consensus::TimestampOracle*){});
    }

    // 4. Storage/compute stubs (coordinator doesn't store data)
    auto index_factory = std::make_unique<index::IndexFactory>();
    auto segment_manager = std::make_shared<storage::SegmentManager>(
        args.data_dir + "/segments", index_factory.get());
    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_manager.get());

    // 5. Services
    auto internal_service = std::make_unique<network::InternalService>(
        shard_manager, segment_manager, query_executor,
        node_registry, timestamp_oracle, coordinator);
    // Load config for auth (optional)
    utils::GVDBConfig config = utils::Config::get_default();
    if (!args.config_file.empty()) {
      auto cfg_result = utils::Config::load_from_file(args.config_file);
      if (cfg_result.ok()) config = std::move(*cfg_result);
    }

    // RBAC
    std::shared_ptr<auth::RbacStore> rbac_store;
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
    if (config.server.auth.enabled) {
      rbac_store = std::make_shared<auth::RbacStore>(config.server.auth);
      interceptors.push_back(
          std::make_unique<network::ApiKeyAuthInterceptorFactory>(rbac_store));
    }

    auto coord_resolver = network::MakeCoordinatorResolver(coordinator);
    auto vectordb_service = std::make_unique<network::VectorDBService>(
        segment_manager, query_executor, std::move(coord_resolver), rbac_store);

    // 6. Start gRPC server
    auto credentials = utils::ServerBootstrap::MakeServerCredentials(config.server.tls);
    auto grpc_server = utils::ServerBootstrap::StartGrpcServer(
        args.bind_address,
        {internal_service.get(), vectordb_service.get()},
        credentials, std::move(interceptors));
    if (!grpc_server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    std::string role = raft_node->IsLeader() ? "LEADER" : "FOLLOWER";
    utils::ServerBootstrap::PrintBanner("GVDB Coordinator Node", {
        "Node ID: " + std::to_string(args.node_id),
        "Mode: " + std::string(args.single_node_mode ? "Single-Node" : "Multi-Node"),
        "gRPC Services: " + args.bind_address,
        "Raft Address: " + args.raft_address,
        "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
        "Role: " + role,
    });

    // Wait loop with leader status monitoring
    while (!utils::ServerBootstrap::ShutdownFlag().load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (!args.single_node_mode) {
        static bool was_leader = raft_node->IsLeader();
        bool is_leader = raft_node->IsLeader();
        if (was_leader != is_leader) {
          std::cout << ">>> " << (is_leader ? "Became LEADER" : "Lost leadership") << " <<<" << std::endl;
          was_leader = is_leader;
        }
      }
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    grpc_server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    (void)raft_node->Shutdown();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}