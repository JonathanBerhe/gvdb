#include "network/grpc_server.h"
#include "utils/logger.h"

namespace gvdb {
namespace network {

GrpcServer::GrpcServer(const GrpcServerConfig& config) : config_(config) {
  // Configure server builder
  builder_.AddListeningPort(config_.bindAddress(),
                           grpc::InsecureServerCredentials());
  builder_.SetMaxReceiveMessageSize(config_.max_message_size);
  builder_.SetMaxSendMessageSize(config_.max_message_size);
}

GrpcServer::~GrpcServer() {
  shutdown();
}

void GrpcServer::registerService(grpc::Service* service) {
  if (is_running_) {
    utils::Logger::Instance().Error("Cannot register service after server has started");
    return;
  }
  builder_.RegisterService(service);
}

absl::Status GrpcServer::start() {
  if (is_running_) {
    return absl::FailedPreconditionError("Server is already running");
  }

  server_ = builder_.BuildAndStart();
  if (!server_) {
    return absl::InternalError("Failed to start gRPC server");
  }

  is_running_ = true;
  utils::Logger::Instance().Info("gRPC server listening on {}", config_.bindAddress());

  // This is a blocking call
  server_->Wait();

  return absl::OkStatus();
}

void GrpcServer::shutdown() {
  if (is_running_ && server_) {
    utils::Logger::Instance().Info("Shutting down gRPC server");
    server_->Shutdown();
    is_running_ = false;
  }
}

} // namespace network
} // namespace gvdb
