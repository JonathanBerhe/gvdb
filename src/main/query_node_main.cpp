// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cluster/query_node.h"
#include "cluster/heartbeat_sender.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "index/index_factory.h"
#include "network/vectordb_service.h"
#include "network/collection_resolver.h"
#include "utils/server_bootstrap.h"

struct QueryNodeArgs {
  int node_id = 201;
  std::string bind_address = "0.0.0.0:50070";
  std::string data_dir = "/tmp/gvdb/query_node";
  std::vector<std::string> coordinator_addresses;
  size_t memory_limit_gb = 16;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID                Query node ID (default: 201)\n"
            << "  --bind-address ADDR         gRPC bind address (default: 0.0.0.0:50070)\n"
            << "  --data-dir PATH             Data directory for cache (default: /tmp/gvdb/query_node)\n"
            << "  --coordinator ADDR          Coordinator addresses (comma-separated)\n"
            << "  --memory-limit-gb SIZE      Memory limit for caching in GB (default: 16)\n"
            << "  --help                      Show this help message\n";
}

bool ParseArgs(int argc, char** argv, QueryNodeArgs& args) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
    } else if (arg == "--node-id" && i + 1 < argc) {
      args.node_id = std::stoi(argv[++i]);
    } else if (arg == "--bind-address" && i + 1 < argc) {
      args.bind_address = argv[++i];
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
  QueryNodeArgs args;
  if (!ParseArgs(argc, argv, args)) return 1;

  using namespace gvdb;
  utils::ServerBootstrap::InstallSignalHandlers();

  auto log_status = utils::ServerBootstrap::InitializeLogger(
      args.data_dir, "query_node.log");
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB Query Node (ID: {})", args.node_id);

  int metrics_port = 9200 + (args.node_id - 200);
  utils::ServerBootstrap::StartMetricsServer(metrics_port);

  try {
    // 1. Storage (for caching) + compute
    auto index_factory = std::make_unique<index::IndexFactory>();
    auto segment_manager = std::make_shared<storage::SegmentManager>(
        args.data_dir + "/cache", index_factory.get());
    auto query_executor = std::make_shared<compute::QueryExecutor>(
        segment_manager.get());

    // 2. QueryNode (segment loading/eviction)
    size_t memory_limit_bytes = args.memory_limit_gb * 1024ULL * 1024ULL * 1024ULL;
    auto query_node = std::make_unique<cluster::QueryNode>(
        segment_manager, query_executor, memory_limit_bytes);

    // 3. VectorDBService
    std::unique_ptr<network::ICollectionResolver> resolver;
    if (!args.coordinator_addresses.empty()) {
      resolver = network::MakeCachedCoordinatorResolver(args.coordinator_addresses[0]);
    } else {
      resolver = network::MakeLocalResolver(segment_manager);
    }
    auto service = std::make_unique<network::VectorDBService>(
        segment_manager, query_executor, std::move(resolver));

    // 4. Start server
    auto server = utils::ServerBootstrap::StartGrpcServer(
        args.bind_address, {service.get()});
    if (!server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    utils::ServerBootstrap::PrintBanner("GVDB Query Node", {
        "Node ID: " + std::to_string(args.node_id),
        "gRPC Service: " + args.bind_address,
        "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
        "Cache Directory: " + args.data_dir,
        "Cache Memory Limit: " + std::to_string(args.memory_limit_gb) + " GB",
    });

    // 5. Heartbeat sender
    std::unique_ptr<cluster::HeartbeatSender> heartbeat;
    if (!args.coordinator_addresses.empty()) {
      heartbeat = std::make_unique<cluster::HeartbeatSender>(
          args.coordinator_addresses[0], args.node_id, args.bind_address,
          proto::internal::NodeType::NODE_TYPE_QUERY_NODE,
          utils::ServerBootstrap::ShutdownFlag());
      heartbeat->Start();
    }

    utils::ServerBootstrap::WaitForShutdown();

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    heartbeat.reset();
    server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}