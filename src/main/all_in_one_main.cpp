#include <signal.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "consensus/raft_node.h"
#include "consensus/raft_config.h"
#include "cluster/coordinator.h"
#include "cluster/shard_manager.h"
#include "storage/segment_manager.h"
#include "compute/query_executor.h"
#include "network/grpc_server.h"
#include "network/vectordb_service.h"
#include "index/index_factory.h"
#include "utils/logger.h"
#include "utils/metrics.h"

#include <grpcpp/grpcpp.h>

// Command-line arguments (simple approach without gflags for now)
int g_port = 50051;
std::string g_data_dir = "/tmp/gvdb";
int g_node_id = 1;

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
            << "  --port PORT          gRPC server port (default: 50051)\n"
            << "  --data-dir PATH      Data directory (default: /tmp/gvdb)\n"
            << "  --node-id ID         Node ID (default: 1)\n"
            << "  --help               Show this help message\n";
}

bool ParseArgs(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return false;
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

int main(int argc, char** argv) {
  // Parse command-line arguments
  if (!ParseArgs(argc, argv)) {
    return 1;
  }

  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Initialize logger
  gvdb::utils::LogConfig log_config;
  log_config.file_path = g_data_dir + "/logs/gvdb.log";
  log_config.console_enabled = true;
  log_config.file_enabled = true;
  log_config.level = gvdb::utils::LogLevel::INFO;

  auto log_status = gvdb::utils::Logger::Initialize(log_config);
  if (!log_status.ok()) {
    std::cerr << "Warning: Failed to initialize logger: " << log_status.message() << std::endl;
  }

  gvdb::utils::Logger::Instance().Info("Starting GVDB All-in-One Server");
  gvdb::utils::Logger::Instance().Info("  Node ID: {}", g_node_id);
  gvdb::utils::Logger::Instance().Info("  Port: {}", g_port);
  gvdb::utils::Logger::Instance().Info("  Data Directory: {}", g_data_dir);

  // Start metrics server
  if (!gvdb::utils::MetricsRegistry::Instance().StartMetricsServer(9090)) {
    std::cerr << "Warning: Failed to start metrics server on :9090" << std::endl;
  } else {
    gvdb::utils::Logger::Instance().Info("Metrics server started on :9090/metrics");
  }

  try {
    // 1. Create consensus node (single-node mode)
    gvdb::consensus::RaftConfig raft_config;
    raft_config.node_id = g_node_id;
    raft_config.single_node_mode = true;
    raft_config.data_dir = g_data_dir + "/raft";

    auto raft_node = std::make_unique<gvdb::consensus::RaftNode>(raft_config);
    auto status = raft_node->Start();
    if (!status.ok()) {
      std::cerr << "Failed to start Raft: " << status.message() << std::endl;
      return 1;
    }
    gvdb::utils::Logger::Instance().Info("Consensus node started (single-node mode)");

    // 2. Create storage layer
    auto index_factory = std::make_unique<gvdb::index::IndexFactory>();
    auto segment_manager = std::make_shared<gvdb::storage::SegmentManager>(
        g_data_dir + "/segments", index_factory.get());
    gvdb::utils::Logger::Instance().Info("Storage layer initialized");

    // 3. Create compute layer
    auto query_executor = std::make_shared<gvdb::compute::QueryExecutor>(
        segment_manager.get());
    gvdb::utils::Logger::Instance().Info("Compute layer initialized");

    // 4. Create cluster coordinator
    auto shard_manager = std::make_shared<gvdb::cluster::ShardManager>(
        16, gvdb::cluster::ShardingStrategy::HASH);
    auto coordinator = std::make_unique<gvdb::cluster::Coordinator>(shard_manager);
    gvdb::utils::Logger::Instance().Info("Cluster coordinator initialized");

    // 5. Create gRPC service
    auto service = std::make_unique<gvdb::network::VectorDBService>(
        segment_manager, query_executor);
    gvdb::utils::Logger::Instance().Info("gRPC service created");

    // 6. Start gRPC server
    std::string server_address = absl::StrCat("0.0.0.0:", g_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address,
                              grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());

    // Increase message size limits for large vector batches
    // 256 MB allows realistic batch sizes for high-dimensional vectors:
    //   - 10K vectors × 1536D (OpenAI ada-002): ~61 MB ✓
    //   - 10K vectors × 3072D (OpenAI large): ~123 MB ✓
    //   - 50K vectors × 768D (BERT): ~153 MB ✓
    // Industry standard: Milvus=64MB, Elasticsearch=100MB, Qdrant=32MB
    // We choose 256 MB for future-proofing high-dimensional embeddings
    builder.SetMaxReceiveMessageSize(256 * 1024 * 1024);  // 256 MB
    builder.SetMaxSendMessageSize(256 * 1024 * 1024);     // 256 MB

    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "Failed to start gRPC server on " << server_address << std::endl;
      return 1;
    }

    std::cout << "\n========================================" << std::endl;
    std::cout << "GVDB All-in-One Server Started" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "gRPC Service: " << server_address << std::endl;
    std::cout << "Metrics: http://0.0.0.0:9090/metrics" << std::endl;
    std::cout << "Node ID: " << g_node_id << std::endl;
    std::cout << "Data Directory: " << g_data_dir << std::endl;
    std::cout << "\nPress Ctrl+C to shutdown..." << std::endl;
    std::cout << "========================================\n" << std::endl;

    gvdb::utils::Logger::Instance().Info("Server listening on {}", server_address);

    // Wait for shutdown signal
    while (!g_shutdown.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\nShutting down gracefully..." << std::endl;
    gvdb::utils::Logger::Instance().Info("Shutting down...");

    server->Shutdown();
    gvdb::utils::Logger::Instance().Info("gRPC server stopped");

    gvdb::utils::MetricsRegistry::Instance().StopMetricsServer();
    gvdb::utils::Logger::Instance().Info("Metrics server stopped");

    auto shutdown_status = raft_node->Shutdown();
    if (!shutdown_status.ok()) {
      gvdb::utils::Logger::Instance().Warn("Raft shutdown returned: {}", shutdown_status.message());
    }
    gvdb::utils::Logger::Instance().Info("Consensus node stopped");

    std::cout << "Shutdown complete. Goodbye!" << std::endl;
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    gvdb::utils::Logger::Instance().Error("Fatal error: {}", e.what());
    return 1;
  }
}
