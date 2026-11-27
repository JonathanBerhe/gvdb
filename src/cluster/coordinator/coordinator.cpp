// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Coordinator: construction, start, shutdown and id allocation. The other
// files in this folder hold the rest of the Coordinator methods, one file
// per responsibility.

#include "cluster/coordinator/coordinator.h"
#include <chrono>
#include <mutex>
#include <set>
#include <thread>
#include "cluster/coordinator/internal_client.h"
#include "common/logger.h"

namespace gvdb::cluster {

Coordinator::Coordinator(std::shared_ptr<ShardManager> shard_manager,
                         std::shared_ptr<NodeRegistry> node_registry, CoordinatorOptions options)
    : shard_manager_(std::move(shard_manager)),
      node_registry_(std::move(node_registry)),
      registry_path_(options.registry_dir.empty() ? std::string()
                                                  : options.registry_dir + "/registry.json"),
      auto_rebalance_config_(options.auto_rebalance),
      client_factory_(std::move(options.client_factory)),
      backup_manager_(std::move(options.backup_manager)),
      restore_manager_(std::move(options.restore_manager)) {
  common::Logger::Instance().Info(
      "Coordinator initialized (using NodeRegistry, distributed_mode={}, "
      "auto_rebalance_debounce={}s)",
      client_factory_ != nullptr, auto_rebalance_config_.debounce.count());
}

Coordinator::~Coordinator() { Shutdown().IgnoreError(); }

absl::Status Coordinator::Start(const std::string& bind_address) {
  common::Logger::Instance().Info("Starting coordinator on {}", bind_address);

  // Seed the "known data nodes" set with whoever is already heartbeating.
  // Without this, every existing data node
  // looks "new" on the first health-check cycle after a coordinator
  // restart → we would fire an unnecessary rebalance even if the cluster
  // was already balanced. Seeding on Start() means only nodes that join
  // AFTER we're running count as scale-up events.
  {
    auto data_nodes =
        node_registry_->GetHealthyNodesByType(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
    std::scoped_lock lock(known_nodes_mutex_);
    known_data_nodes_.clear();
    for (const auto& n : data_nodes) {
      if (n.IsDraining()) continue;
      known_data_nodes_.insert(core::MakeNodeId(n.info.node_id()));
    }
    common::Logger::Instance().Info(
        "Auto-rebalance: seeded known_data_nodes_ with {} existing node(s)",
        known_data_nodes_.size());
  }

  running_.store(true, std::memory_order_release);
  StartHealthCheckLoop();

  return absl::OkStatus();
}

absl::Status Coordinator::Shutdown() {
  const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
  if (was_running) {
    common::Logger::Instance().Info("Shutting down coordinator");
    StopHealthCheckLoop();
  }

  // Always wait for any in-flight auto-rebalance worker: even if Start()
  // was never called: so that a joinable std::thread isn't destroyed
  // and doesn't std::terminate the process.
  // Tests construct a Coordinator and call DetectNewDataNodes directly
  // without Start(); the worker thread is still live.
  {
    std::scoped_lock lock(rebalance_worker_mutex_);
    if (rebalance_worker_ && rebalance_worker_->joinable()) {
      if (was_running) {
        common::Logger::Instance().Info(
            "Coordinator shutdown: waiting for auto-rebalance worker to finish");
      }
      rebalance_worker_->join();
    }
    rebalance_worker_.reset();
  }

  return absl::OkStatus();
}

core::CollectionId Coordinator::AllocateCollectionId() {
  return core::MakeCollectionId(next_collection_id_.fetch_add(1, std::memory_order_relaxed));
}

core::Timestamp Coordinator::GetCurrentTimestamp() const {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

}  // namespace gvdb::cluster
