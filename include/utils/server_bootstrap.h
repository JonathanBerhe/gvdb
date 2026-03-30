// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/config.h"
#include "core/status.h"
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace gvdb {
namespace utils {

// Shared infrastructure for all GVDB server binaries.
// Encapsulates signal handling, logger init, metrics, gRPC server setup,
// and the shutdown wait loop.
class ServerBootstrap {
 public:
  // Global shutdown flag (shared across all binaries)
  static std::atomic<bool>& ShutdownFlag();

  // Install SIGINT/SIGTERM handlers that set ShutdownFlag
  static void InstallSignalHandlers();

  // Initialize logger with standard settings
  static absl::Status InitializeLogger(const std::string& data_dir,
                                        const std::string& log_file_name,
                                        LogLevel level = LogLevel::INFO);

  // Start Prometheus metrics server
  static bool StartMetricsServer(int port);

  // Build and start a gRPC server with 256MB message limits
  static std::unique_ptr<grpc::Server> StartGrpcServer(
      const std::string& bind_address,
      const std::vector<grpc::Service*>& services);

  // Build and start a gRPC server with TLS
  static std::unique_ptr<grpc::Server> StartGrpcServer(
      const std::string& bind_address,
      const std::vector<grpc::Service*>& services,
      const TlsConfig& tls_config);

  // Build and start a gRPC server with pre-built credentials
  static std::unique_ptr<grpc::Server> StartGrpcServer(
      const std::string& bind_address,
      const std::vector<grpc::Service*>& services,
      std::shared_ptr<grpc::ServerCredentials> credentials);

  // Create server credentials from TLS config (insecure if TLS disabled)
  static std::shared_ptr<grpc::ServerCredentials> MakeServerCredentials(
      const TlsConfig& tls_config);

  // Create gRPC channel credentials (TLS or insecure)
  static std::shared_ptr<grpc::ChannelCredentials> MakeChannelCredentials(
      const TlsConfig& tls_config);

  // Block until ShutdownFlag is set
  static void WaitForShutdown();

  // Print startup banner to stdout
  static void PrintBanner(const std::string& node_name,
                           const std::vector<std::string>& info_lines);

  // Shutdown metrics server
  static void StopMetricsServer();
};

}  // namespace utils
}  // namespace gvdb