// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// InternalService: the cluster-internal gRPC service. The coordinator and
// the data nodes serve the same proto service, and each RPC uses the parts
// that its program provides. coordinator_handlers.cpp and
// data_node_handlers.cpp hold the two groups of RPCs.

#include "service/internal/internal_service.h"
#include "cluster/node_registry.h"
#include "cluster/coordinator.h"
#include "consensus/raft_node.h"
#include "common/logger.h"

namespace gvdb {
namespace service {

InternalService::InternalService(
    std::shared_ptr<cluster::ShardManager> shard_manager,
    std::shared_ptr<storage::ISegmentStore> segment_store,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::shared_ptr<cluster::NodeRegistry> node_registry,
    std::shared_ptr<cluster::Coordinator> coordinator,
    std::shared_ptr<consensus::RaftNode> raft_node)
    : shard_manager_(shard_manager),
      segment_store_(segment_store),
      query_executor_(query_executor),
      node_registry_(node_registry),
      coordinator_(coordinator),
      raft_node_(raft_node) {
  common::Logger::Instance().Info(
      "InternalService initialized (node_registry={}, coordinator={}, raft_node={})",
      node_registry_ != nullptr ? "yes" : "no",
      coordinator_ != nullptr ? "yes" : "no",
      raft_node_ != nullptr ? "yes" : "no");
}

InternalService::~InternalService() {
  common::Logger::Instance().Info("InternalService shutting down");
}

}  // namespace service
}  // namespace gvdb
