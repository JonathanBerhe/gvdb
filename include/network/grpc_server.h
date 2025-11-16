#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include "absl/status/status.h"

namespace gvdb {
namespace network {

// Configuration for the gRPC server
struct GrpcServerConfig {
  std::string address = "0.0.0.0";
  int port = 50051;
  int max_message_size = 100 * 1024 * 1024; // 100MB default

  std::string bindAddress() const {
    return address + ":" + std::to_string(port);
  }
};

// RAII wrapper for gRPC server lifecycle
class GrpcServer {
 public:
  explicit GrpcServer(const GrpcServerConfig& config = GrpcServerConfig());
  ~GrpcServer();

  // Non-copyable, non-movable
  GrpcServer(const GrpcServer&) = delete;
  GrpcServer& operator=(const GrpcServer&) = delete;

  // Register a service with the server (must be called before start)
  void registerService(grpc::Service* service);

  // Start the server (blocking call)
  absl::Status start();

  // Shutdown the server gracefully
  void shutdown();

  // Get the listening address
  std::string address() const { return config_.bindAddress(); }

 private:
  GrpcServerConfig config_;
  std::unique_ptr<grpc::Server> server_;
  grpc::ServerBuilder builder_;
  bool is_running_ = false;
};

} // namespace network
} // namespace gvdb
