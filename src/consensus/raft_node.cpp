#include "consensus/raft_node.h"
#include "consensus/metadata_state_machine.h"
#include "consensus/gvdb_state_manager.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"

#include <libnuraft/nuraft.hxx>
#include <filesystem>

namespace gvdb {
namespace consensus {

// NuRaft logger adapter to use our logging system
class NuRaftLoggerAdapter : public nuraft::logger {
 public:
  NuRaftLoggerAdapter() = default;

  void put_details(int level,
                   const char* source_file,
                   const char* func_name,
                   size_t line_number,
                   const std::string& msg) override {
    // Map NuRaft log levels to our logger
    if (level <= 2) {  // ERROR, FATAL
      utils::Logger::Instance().Error("[NuRaft] {}", msg);
    } else if (level == 3) {  // WARN
      utils::Logger::Instance().Warn("[NuRaft] {}", msg);
    } else if (level == 4) {  // INFO
      utils::Logger::Instance().Info("[NuRaft] {}", msg);
    } else {  // DEBUG, TRACE
      utils::Logger::Instance().Debug("[NuRaft] {}", msg);
    }
  }

  void set_level(int level) override {
    level_ = level;
  }

  int get_level() override {
    return level_;
  }

 private:
  int level_ = 4;  // INFO by default
};


RaftNode::RaftNode(const RaftConfig& config)
    : config_(config) {

  if (!config_.IsValid()) {
    throw std::invalid_argument("Invalid Raft configuration");
  }

  utils::Logger::Instance().Info("RaftNode created (node_id={}, single_node_mode={})",
                                 config_.node_id,
                                 config_.single_node_mode);
}

RaftNode::~RaftNode() {
  if (running_.load(std::memory_order_acquire)) {
    Shutdown();
  }
}

core::Status RaftNode::Start() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (running_.load(std::memory_order_acquire)) {
    return core::FailedPreconditionError("RaftNode already running");
  }

  utils::Logger::Instance().Info("Starting RaftNode (node_id={})", config_.node_id);

  // In single-node mode, immediately become leader
  if (config_.single_node_mode) {
    is_leader_.store(true, std::memory_order_release);
    utils::Logger::Instance().Info("RaftNode is leader (single-node mode)");
  } else {
    // Multi-node mode: Initialize NuRaft
    auto status = InitializeNuRaft();
    if (!status.ok()) {
      return status;
    }

    // Leader election happens asynchronously in NuRaft
    // The raft_server will trigger callbacks when leadership changes
    // For now, we don't set is_leader_ - it will be determined by querying raft_server
    utils::Logger::Instance().Info(
        "RaftNode started in multi-node mode (leader election in progress)");
  }

  running_.store(true, std::memory_order_release);

  utils::Logger::Instance().Info("RaftNode started successfully");
  return core::OkStatus();
}

core::Status RaftNode::Shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!running_.load(std::memory_order_acquire)) {
    return core::OkStatus();  // Already stopped
  }

  utils::Logger::Instance().Info("Shutting down RaftNode");

  // Shutdown NuRaft if running in multi-node mode
  if (launcher_) {
    launcher_->shutdown();
    launcher_.reset();
  }

  raft_server_.reset();
  state_machine_.reset();
  nuraft_logger_.reset();

  is_leader_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);

  utils::Logger::Instance().Info("RaftNode shut down successfully");
  return core::OkStatus();
}

bool RaftNode::IsRunning() const {
  return running_.load(std::memory_order_acquire);
}

bool RaftNode::IsLeader() const {
  // Single-node mode: use cached value
  if (config_.single_node_mode) {
    return is_leader_.load(std::memory_order_acquire);
  }

  // Multi-node mode: query NuRaft server
  if (!raft_server_) {
    return false;  // Not initialized yet
  }

  return raft_server_->is_leader();
}

int RaftNode::GetLeaderId() const {
  // Single-node mode: return self if leader
  if (config_.single_node_mode) {
    if (is_leader_.load(std::memory_order_acquire)) {
      return config_.node_id;
    }
    return -1;  // No leader
  }

  // Multi-node mode: query NuRaft server
  if (!raft_server_) {
    return -1;  // Not initialized yet
  }

  int leader_id = raft_server_->get_leader();
  return leader_id;  // Returns -1 if no leader
}

core::StatusOr<core::CollectionId> RaftNode::CreateCollection(
    const std::string& name,
    core::Dimension dimension,
    core::MetricType metric_type,
    core::IndexType index_type,
    size_t replication_factor) {

  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  // Get timestamp from TSO
  core::Timestamp ts = tso_.GetTimestamp();

  // Apply directly to metadata store (in single-node mode, no consensus needed)
  auto result = metadata_store_.CreateCollection(
      name, dimension, metric_type, index_type, replication_factor, ts);

  if (result.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return result;
}

core::Status RaftNode::DropCollection(core::CollectionId collection_id) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.DropCollection(collection_id, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::StatusOr<cluster::CollectionMetadata> RaftNode::GetCollectionMetadata(
    core::CollectionId id) const {
  return metadata_store_.GetCollectionMetadata(id);
}

core::StatusOr<cluster::CollectionMetadata> RaftNode::GetCollectionMetadata(
    const std::string& name) const {
  return metadata_store_.GetCollectionMetadata(name);
}

std::vector<cluster::CollectionMetadata> RaftNode::ListCollections() const {
  return metadata_store_.ListCollections();
}

core::Status RaftNode::RegisterNode(const cluster::NodeInfo& node_info) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.RegisterNode(node_info, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::Status RaftNode::UnregisterNode(core::NodeId node_id) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.UnregisterNode(node_id, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::StatusOr<cluster::NodeInfo> RaftNode::GetNodeInfo(core::NodeId id) const {
  return metadata_store_.GetNodeInfo(id);
}

std::vector<cluster::NodeInfo> RaftNode::ListNodes() const {
  return metadata_store_.ListNodes();
}

size_t RaftNode::GetCommittedOpCount() const {
  return committed_ops_.load(std::memory_order_relaxed);
}

core::Status RaftNode::ProposeOperation(const MetadataOp& op) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader");
  }

  // Single-node mode: apply directly to metadata store
  if (config_.single_node_mode) {
    auto status = metadata_store_.Apply(op);
    if (status.ok()) {
      committed_ops_.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
  }

  // Multi-node mode: propose through NuRaft
  if (!raft_server_) {
    return core::InternalError("NuRaft server not initialized");
  }

  // Serialize the operation using the state machine's serialization
  auto buffer = MetadataStateMachine::SerializeMetadataOp(op);

  // Propose to NuRaft (this will replicate and wait for commit)
  auto result = raft_server_->append_entries({buffer});

  if (!result->get_accepted()) {
    return core::InternalError(
        absl::StrCat("Raft proposal rejected: ",
                     result->get_result_code()));
  }

  // Wait for commit (blocking mode)
  // The state machine's commit() will be called when this is committed
  // and it will apply to metadata_store_

  // Check the result code
  if (result->get_result_code() != nuraft::cmd_result_code::OK) {
    return core::InternalError(
        absl::StrCat("Raft commit failed with code: ",
                     static_cast<int>(result->get_result_code())));
  }

  committed_ops_.fetch_add(1, std::memory_order_relaxed);
  return core::OkStatus();
}

core::Status RaftNode::InitializeNuRaft() {
  utils::Logger::Instance().Info("Initializing NuRaft for multi-node mode");

  // Create NuRaft logger adapter
  nuraft_logger_ = nuraft::cs_new<NuRaftLoggerAdapter>();
  nuraft_logger_->set_level(4);  // INFO level

  // Create metadata state machine with shared metadata store
  // This ensures both single-node and multi-node modes use the same store
  state_machine_ = std::make_shared<MetadataStateMachine>(&metadata_store_);

  // Create state manager with node ID and endpoint
  // Note: listen_address should be in format "host:port"
  state_mgr_ = std::make_shared<GvdbStateManager>(
      config_.node_id,
      config_.listen_address);

  // Parse port from listen_address for raft_launcher
  // Format: "host:port" -> extract port number
  size_t colon_pos = config_.listen_address.find(':');
  if (colon_pos == std::string::npos) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid listen_address format (expected 'host:port'): ",
                     config_.listen_address));
  }

  std::string port_str = config_.listen_address.substr(colon_pos + 1);
  int port = 0;
  try {
    port = std::stoi(port_str);
  } catch (const std::exception& e) {
    return core::InvalidArgumentError(
        absl::StrCat("Invalid port number in listen_address: ", port_str));
  }

  // Create Raft parameters
  nuraft::raft_params params;
  params.heart_beat_interval_ = 100;           // 100ms heartbeat
  params.election_timeout_lower_bound_ = 200;  // 200ms min election timeout
  params.election_timeout_upper_bound_ = 400;  // 400ms max election timeout
  params.reserved_log_items_ = 10000;          // Keep 10k log entries before snapshot
  params.snapshot_distance_ = 5000;            // Create snapshot every 5k operations
  params.client_req_timeout_ = 3000;           // 3s timeout for client requests
  params.return_method_ = nuraft::raft_params::blocking;  // Blocking mode for simplicity

  // Initialize Raft launcher
  nuraft::asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 4;  // 4 threads for async I/O

  launcher_ = nuraft::cs_new<nuraft::raft_launcher>();

  nuraft::raft_server::init_options init_opts;
  init_opts.skip_initial_election_timeout_ = false;  // Participate in election immediately
  init_opts.start_server_in_constructor_ = false;    // Start manually after init

  raft_server_ = launcher_->init(
      state_machine_,
      state_mgr_,
      nuraft_logger_,
      port,
      asio_opts,
      params,
      init_opts);

  if (!raft_server_) {
    return core::InternalError("Failed to initialize NuRaft launcher (returned null server)");
  }

  utils::Logger::Instance().Info(
      "NuRaft initialized successfully (node_id={}, endpoint={})",
      config_.node_id,
      config_.listen_address);

  // Note: Leader election will happen asynchronously
  // Use IsLeader() to check leadership status
  return core::OkStatus();
}

} // namespace consensus
} // namespace gvdb
