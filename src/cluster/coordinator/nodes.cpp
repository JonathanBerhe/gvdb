// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Node registration, the healthy-node lists and the clients to the data
// nodes.

#include "cluster/coordinator/coordinator.h"
#include "core/status.h"
#include "cluster/coordinator/internal_client.h"
#include "rpc/proto_conversions.h"
#include "common/logger.h"
#include "absl/strings/str_cat.h"
#include <mutex>

namespace gvdb {
namespace cluster {

namespace {

// Convert RegisteredNode to meta::NodeInfo
meta::NodeInfo ConvertRegisteredNode(const RegisteredNode& reg_node) {
  return rpc::fromProto(reg_node.info);
}

}  // namespace

absl::Status Coordinator::RegisterNode(const meta::NodeInfo& node_info) {
  // Check if node already exists in NodeRegistry
  RegisteredNode existing;
  if (node_registry_->GetNode(core::ToUInt32(node_info.node_id), &existing)) {
    return absl::AlreadyExistsError(
        absl::StrCat("Node already registered: ", core::ToUInt32(node_info.node_id)));
  }

  // Register with shard manager first (can fail)
  RETURN_IF_ERROR(shard_manager_->RegisterNode(node_info.node_id));

  // Register with NodeRegistry (updates heartbeat timestamp automatically)
  proto::internal::NodeInfo proto_info;
  rpc::toProto(node_info, &proto_info);
  node_registry_->UpdateNode(proto_info);

  common::Logger::Instance().Info("Registered node {} (type: {}, address: {})",
                                 core::ToUInt32(node_info.node_id),
                                 static_cast<int>(node_info.type),
                                 node_info.address);

  return absl::OkStatus();
}

std::vector<meta::NodeInfo> Coordinator::GetHealthyNodes(meta::NodeType type) const {
  // Convert Coordinator::NodeType to proto::internal::NodeType
  proto::internal::NodeType proto_type;
  switch (type) {
    case meta::NodeType::COORDINATOR:
      proto_type = proto::internal::NodeType::NODE_TYPE_COORDINATOR;
      break;
    case meta::NodeType::DATA_NODE:
      proto_type = proto::internal::NodeType::NODE_TYPE_DATA_NODE;
      break;
    case meta::NodeType::QUERY_NODE:
      proto_type = proto::internal::NodeType::NODE_TYPE_QUERY_NODE;
      break;
    case meta::NodeType::PROXY:
      proto_type = proto::internal::NodeType::NODE_TYPE_PROXY;
      break;
    default:
      proto_type = proto::internal::NodeType::NODE_TYPE_QUERY_NODE;
      break;
  }

  // Get nodes fit to receive new work: healthy AND not draining. Draining
  // nodes have sent a graceful-shutdown heartbeat and should not be targets
  // of new placement or replication.
  auto registered_nodes = node_registry_->GetRoutableNodesByType(proto_type);

  // Convert to Coordinator::NodeInfo
  std::vector<meta::NodeInfo> result;
  result.reserve(registered_nodes.size());

  for (const auto& reg_node : registered_nodes) {
    result.push_back(ConvertRegisteredNode(reg_node));
  }

  common::Logger::Instance().Debug("GetHealthyNodes(type={}) returned {} nodes",
                                   static_cast<int>(type), result.size());

  return result;
}

IInternalServiceClient* Coordinator::GetOrCreateDataNodeClient(core::NodeId node_id) {
  // Check if factory is available
  if (!client_factory_) {
    return nullptr;  // Distributed mode not enabled
  }

  // Try read lock first (fast path for existing clients)
  {
    std::shared_lock lock(client_mutex_);
    auto it = data_node_clients_.find(node_id);
    if (it != data_node_clients_.end()) {
      return it->second.get();
    }
  }

  // Need to create new client - upgrade to write lock
  std::unique_lock lock(client_mutex_);

  // Double-check in case another thread created it while we waited
  auto it = data_node_clients_.find(node_id);
  if (it != data_node_clients_.end()) {
    return it->second.get();
  }

  // Get node info from NodeRegistry
  RegisteredNode node;
  if (!node_registry_->GetNode(core::ToUInt32(node_id), &node)) {
    common::Logger::Instance().Error("Cannot create client for unknown node {}", core::ToUInt32(node_id));
    return nullptr;
  }

  const std::string& address = node.info.grpc_address();

  // Use factory to create client
  auto client = client_factory_->CreateClient(node_id, address);
  if (!client) {
    common::Logger::Instance().Error("Factory failed to create client for node {}", core::ToUInt32(node_id));
    return nullptr;
  }

  // Store in map first (transfers ownership)
  data_node_clients_[node_id] = std::move(client);

  // Then return pointer from map
  return data_node_clients_[node_id].get();
}

}  // namespace cluster
}  // namespace gvdb
