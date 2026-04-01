// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "network/proxy_service.h"
#include "utils/server_bootstrap.h"
#include "utils/env_flags.h"

struct ProxyArgs {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50050";
  std::string data_dir = "/tmp/gvdb/proxy";
  std::vector<std::string> coordinator_addresses;
  std::vector<std::string> query_node_addresses;
  std::vector<std::string> data_node_addresses;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID                Proxy node ID (default: 1)\n"
            << "  --bind-address ADDR         gRPC bind address (default: 0.0.0.0:50050)\n"
            << "  --data-dir PATH             Data directory (default: /tmp/gvdb/proxy)\n"
            << "  --coordinators ADDRS        Coordinator addresses (comma-separated)\n"
            << "  --query-nodes ADDRS         Query node addresses (comma-separated)\n"
            << "  --data-nodes ADDRS          Data node addresses (comma-separated)\n"
            << "  --help                      Show this help message\n";
}

std::vector<std::string> ParseAddresses(const std::string& addrs_str) {
  std::vector<std::string> addrs;
  size_t start = 0;
  size_t end = addrs_str.find(',');
  while (end != std::string::npos) {
    addrs.push_back(addrs_str.substr(start, end - start));
    start = end + 1;
    end = addrs_str.find(',', start);
  }
  addrs.push_back(addrs_str.substr(start));
  return addrs;
}

bool ParseArgs(int argc, char** argv, ProxyArgs& args) {
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
    } else if (arg == "--coordinators" && i + 1 < argc) {
      args.coordinator_addresses = ParseAddresses(argv[++i]);
    } else if (arg == "--query-nodes" && i + 1 < argc) {
      args.query_node_addresses = ParseAddresses(argv[++i]);
    } else if (arg == "--data-nodes" && i + 1 < argc) {
      args.data_node_addresses = ParseAddresses(argv[++i]);
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(argv[0]);
      return false;
    }
  }
  if (args.coordinator_addresses.empty()) {
    std::cerr << "Error: At least one coordinator address must be specified" << std::endl;
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  ProxyArgs args;
  if (!ParseArgs(argc, argv, args)) return 1;

  using namespace gvdb;

  // Env vars override CLI flags
  args.bind_address = utils::ResolveFlag("GVDB_BIND_ADDRESS", args.bind_address);
  args.data_dir = utils::ResolveFlag("GVDB_DATA_DIR", args.data_dir);
  utils::ServerBootstrap::InstallSignalHandlers();

  auto log_status = utils::ServerBootstrap::InitializeLogger(
      args.data_dir, "proxy.log");
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  utils::Logger::Instance().Info("Starting GVDB Proxy (ID: {})", args.node_id);

  int metrics_port = 9050;
  utils::ServerBootstrap::StartMetricsServer(metrics_port);

  try {
    auto proxy_service = std::make_unique<network::ProxyService>(
        args.coordinator_addresses,
        args.query_node_addresses,
        args.data_node_addresses);

    auto grpc_server = utils::ServerBootstrap::StartGrpcServer(
        args.bind_address, {proxy_service.get()});
    if (!grpc_server) {
      std::cerr << "Failed to start gRPC server on " << args.bind_address << std::endl;
      return 1;
    }

    std::vector<std::string> banner_lines = {
        "Node ID: " + std::to_string(args.node_id),
        "gRPC Service: " + args.bind_address,
        "Metrics: http://0.0.0.0:" + std::to_string(metrics_port) + "/metrics",
        "Coordinators: " + std::to_string(args.coordinator_addresses.size()) + " node(s)",
    };
    if (!args.query_node_addresses.empty()) {
      banner_lines.push_back("Query Nodes: " + std::to_string(args.query_node_addresses.size()) + " node(s)");
    }
    if (!args.data_node_addresses.empty()) {
      banner_lines.push_back("Data Nodes: " + std::to_string(args.data_node_addresses.size()) + " node(s)");
    }
    utils::ServerBootstrap::PrintBanner("GVDB Proxy", banner_lines);

    utils::ServerBootstrap::WaitForShutdown();

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    grpc_server->Shutdown();
    utils::ServerBootstrap::StopMetricsServer();
    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
}