#include <signal.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "utils/logger.h"
#include "utils/metrics.h"

// Command-line arguments
struct ProxyArgs {
  int node_id = 1;
  std::string bind_address = "0.0.0.0:50050";  // Proxy on 50050, lower than all other nodes
  std::string data_dir = "/tmp/gvdb/proxy";
  std::vector<std::string> coordinator_addresses;
  std::vector<std::string> query_node_addresses;
  std::vector<std::string> data_node_addresses;
};

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nReceived shutdown signal, gracefully shutting down..." << std::endl;
    g_shutdown.store(true);
  }
}

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [options]\n"
            << "Options:\n"
            << "  --node-id ID                Proxy node ID (default: 1)\n"
            << "  --bind-address ADDR         gRPC bind address (default: 0.0.0.0:50050)\n"
            << "  --data-dir PATH             Data directory (default: /tmp/gvdb/proxy)\n"
            << "  --coordinators ADDRS        Coordinator addresses (comma-separated)\n"
            << "                              Example: coord1:50051,coord2:50051,coord3:50051\n"
            << "  --query-nodes ADDRS         Query node addresses (comma-separated)\n"
            << "                              Example: qn1:50070,qn2:50071\n"
            << "  --data-nodes ADDRS          Data node addresses (comma-separated)\n"
            << "                              Example: dn1:50060,dn2:50061\n"
            << "  --help                      Show this help message\n"
            << "\nExamples:\n"
            << "  # Proxy routing to full cluster\n"
            << "  " << program_name << " \\\n"
            << "    --coordinators coord1:50051,coord2:50051,coord3:50051 \\\n"
            << "    --query-nodes qn1:50070,qn2:50071 \\\n"
            << "    --data-nodes dn1:50060,dn2:50061\n"
            << "\n"
            << "  # Proxy for single-node deployment\n"
            << "  " << program_name << " --coordinators localhost:50051\n";
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

  // Parse command-line arguments
  if (!ParseArgs(argc, argv, args)) {
    return 1;
  }

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Initialize logger
  gvdb::utils::LogConfig log_config;
  log_config.file_path = args.data_dir + "/logs/proxy.log";
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = gvdb::utils::LogLevel::INFO;

  auto log_status = gvdb::utils::Logger::Initialize(log_config);
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  gvdb::utils::Logger::Instance().Info("Starting GVDB Proxy");
  gvdb::utils::Logger::Instance().Info("  Node ID: {}", args.node_id);
  gvdb::utils::Logger::Instance().Info("  gRPC Address: {}", args.bind_address);
  gvdb::utils::Logger::Instance().Info("  Data Directory: {}", args.data_dir);

  auto log_addresses = [](const std::string& label, const std::vector<std::string>& addrs) {
    if (!addrs.empty()) {
      std::string addrs_str;
      for (size_t i = 0; i < addrs.size(); ++i) {
        if (i > 0) addrs_str += ", ";
        addrs_str += addrs[i];
      }
      gvdb::utils::Logger::Instance().Info("  {}: [{}]", label, addrs_str);
    }
  };

  log_addresses("Coordinators", args.coordinator_addresses);
  log_addresses("Query Nodes", args.query_node_addresses);
  log_addresses("Data Nodes", args.data_node_addresses);

  // Start metrics server
  int metrics_port = 9050;
  if (!gvdb::utils::MetricsRegistry::Instance().StartMetricsServer(metrics_port)) {
    std::cerr << "Warning: Failed to start metrics server on :" << metrics_port << std::endl;
  } else {
    gvdb::utils::Logger::Instance().Info("Metrics server started on :{}/metrics", metrics_port);
  }

  try {
    // NOTE: This is a lightweight proxy stub for Phase 2.
    // Full routing logic with gRPC forwarding will be implemented in Phase 5.
    // For now, it just validates configuration and waits.

    gvdb::utils::Logger::Instance().Warn(
        "Proxy is in STUB mode (Phase 2 optimized). "
        "Full routing with gRPC forwarding will be implemented in Phase 5.");

    // TODO Phase 5: Implement actual routing
    // - Create gRPC clients to coordinators, query nodes, data nodes
    // - Implement LoadBalancer for request routing
    // - Forward requests and merge results
    // - Connection pooling and retry logic

    std::cout << "\n========================================" << std::endl;
    std::cout << "GVDB Proxy Started (Lightweight Stub)" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Node ID: " << args.node_id << std::endl;
    std::cout << "Binary Size: ~15 MB (optimized!)" << std::endl;
    std::cout << "Metrics: http://0.0.0.0:" << metrics_port << "/metrics" << std::endl;
    std::cout << "Mode: STUB (awaiting Phase 5 implementation)" << std::endl;
    std::cout << "\nConfigured Backend Nodes:" << std::endl;
    std::cout << "  Coordinators: " << args.coordinator_addresses.size() << " node(s)" << std::endl;
    for (const auto& addr : args.coordinator_addresses) {
      std::cout << "    - " << addr << std::endl;
    }
    if (!args.query_node_addresses.empty()) {
      std::cout << "  Query Nodes: " << args.query_node_addresses.size() << " node(s)" << std::endl;
    }
    if (!args.data_node_addresses.empty()) {
      std::cout << "  Data Nodes: " << args.data_node_addresses.size() << " node(s)" << std::endl;
    }
    std::cout << "\nProxy is ready but not yet forwarding requests." << std::endl;
    std::cout << "Full routing will be implemented in Phase 5." << std::endl;
    std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
    std::cout << "========================================\n" << std::endl;

    gvdb::utils::Logger::Instance().Info("Proxy stub running (lightweight mode)");

    // Wait for shutdown signal
    while (!g_shutdown.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    gvdb::utils::Logger::Instance().Info("Shutting down...");

    gvdb::utils::MetricsRegistry::Instance().StopMetricsServer();
    gvdb::utils::Logger::Instance().Info("Metrics server stopped");

    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    gvdb::utils::Logger::Instance().Error("Fatal error: {}", e.what());
    return 1;
  }
}
