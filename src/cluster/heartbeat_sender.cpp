// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/heartbeat_sender.h"
#include "utils/logger.h"
#include <grpcpp/grpcpp.h>
#include <chrono>

namespace gvdb {
namespace cluster {

HeartbeatSender::HeartbeatSender(const std::string& coordinator_address,
                                  int node_id,
                                  const std::string& grpc_address,
                                  proto::internal::NodeType node_type,
                                  std::atomic<bool>& shutdown_flag)
    : coordinator_address_(coordinator_address),
      node_id_(node_id),
      grpc_address_(grpc_address),
      node_type_(node_type),
      shutdown_flag_(shutdown_flag) {}

HeartbeatSender::~HeartbeatSender() {
  Stop();
}

void HeartbeatSender::Start() {
  thread_ = std::make_unique<std::thread>([this] { SendLoop(); });
}

void HeartbeatSender::Stop() {
  if (thread_ && thread_->joinable()) {
    thread_->join();
    utils::Logger::Instance().Info("Heartbeat thread stopped");
  }
}

bool HeartbeatSender::SendDrainHeartbeat() {
  auto channel = grpc::CreateChannel(coordinator_address_,
                                      grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);

  proto::internal::HeartbeatRequest request;
  auto* node_info = request.mutable_node_info();
  node_info->set_node_id(node_id_);
  node_info->set_node_type(node_type_);
  node_info->set_status(proto::internal::NodeStatus::NODE_STATUS_DRAINING);
  node_info->set_grpc_address(grpc_address_);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(2));

  proto::internal::HeartbeatResponse response;
  auto status = stub->Heartbeat(&context, request, &response);

  if (status.ok() && response.acknowledged()) {
    utils::Logger::Instance().Info(
        "Drain heartbeat acknowledged (node_id={})", node_id_);
    return true;
  }
  utils::Logger::Instance().Warn(
      "Drain heartbeat failed (node_id={}): {}", node_id_,
      status.error_message());
  return false;
}

void HeartbeatSender::SendLoop() {
  auto channel = grpc::CreateChannel(coordinator_address_,
                                      grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);

  utils::Logger::Instance().Info("Heartbeat sender started (coordinator={})",
                                  coordinator_address_);

  while (!shutdown_flag_.load()) {
    try {
      proto::internal::HeartbeatRequest request;
      auto* node_info = request.mutable_node_info();
      node_info->set_node_id(node_id_);
      node_info->set_node_type(node_type_);
      node_info->set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
      node_info->set_grpc_address(grpc_address_);

      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(5));

      proto::internal::HeartbeatResponse response;
      auto status = stub->Heartbeat(&context, request, &response);

      if (status.ok() && response.acknowledged()) {
        utils::Logger::Instance().Debug("Heartbeat acknowledged by coordinator");
      } else {
        utils::Logger::Instance().Warn("Heartbeat failed: {}",
                                        status.error_message());
      }
    } catch (const std::exception& e) {
      utils::Logger::Instance().Error("Heartbeat error: {}", e.what());
    }

    // Wait 10 seconds (100ms intervals for fast shutdown)
    for (int i = 0; i < 100 && !shutdown_flag_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  utils::Logger::Instance().Info("Heartbeat sender stopped");
}

}  // namespace cluster
}  // namespace gvdb