// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/raft_node.h"
#include "consensus/metadata_state_machine.h"
#include "consensus/gvdb_state_manager.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"

#include <grpcpp/grpcpp.h>
#include "internal.grpc.pb.h"

#include <libnuraft/nuraft.hxx>
#include <chrono>
#include <filesystem>
#include <optional>
#include <thread>

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


core::StatusOr<RaftPeerSpec> ParseRaftPeerSpec(const std::string& spec) {
  // Be tolerant of whitespace introduced by hand-edited YAML / env vars —
  // strip leading/trailing whitespace on the whole spec, the id, and the
  // endpoint. Reject any embedded whitespace in the id or inside the
  // endpoint's host/port to keep errors unambiguous.
  auto strip = [](std::string s) {
    auto begin = s.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return std::string();
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(begin, end - begin + 1);
  };

  std::string trimmed = strip(spec);
  auto first_colon = trimmed.find(':');
  if (first_colon == std::string::npos || first_colon == 0) {
    return core::InvalidArgumentError(
        absl::StrCat("Raft peer must be 'id:host:port', got: ", spec));
  }
  RaftPeerSpec out;
  std::string id_str = strip(trimmed.substr(0, first_colon));
  if (id_str.find_first_of(" \t\r\n") != std::string::npos) {
    return core::InvalidArgumentError(
        absl::StrCat("Raft peer id contains whitespace in: ", spec));
  }
  try {
    out.id = std::stoi(id_str);
  } catch (const std::exception&) {
    return core::InvalidArgumentError(
        absl::StrCat("Raft peer id must be an integer in: ", spec));
  }
  if (out.id <= 0) {
    return core::InvalidArgumentError(
        absl::StrCat("Raft peer id must be > 0 in: ", spec));
  }
  out.endpoint = strip(trimmed.substr(first_colon + 1));
  if (out.endpoint.empty() ||
      out.endpoint.find(':') == std::string::npos ||
      out.endpoint.find(':') == out.endpoint.size() - 1 ||
      out.endpoint.find_first_of(" \t\r\n") != std::string::npos) {
    return core::InvalidArgumentError(
        absl::StrCat("Raft peer endpoint must be 'host:port' in: ", spec));
  }
  return out;
}

core::StatusOr<PeerListPlan> PrepareRaftPeerList(
    int self_id,
    const std::vector<std::string>& declared_peers,
    size_t persisted_cluster_size) {
  PeerListPlan plan;
  std::set<int> seen_ids;
  bool self_found = false;

  for (const auto& raw : declared_peers) {
    auto parsed = ParseRaftPeerSpec(raw);
    if (!parsed.ok()) return parsed.status();

    if (!seen_ids.insert(parsed->id).second) {
      return core::InvalidArgumentError(
          absl::StrCat("Duplicate Raft peer id ", parsed->id,
                       " in --raft-peers"));
    }
    if (parsed->id == self_id) self_found = true;
    plan.peers.push_back(*parsed);
  }

  if (!plan.peers.empty() && !self_found) {
    return core::InvalidArgumentError(
        absl::StrCat("--node-id ", self_id,
                     " does not appear in --raft-peers — the declared "
                     "peer list must include this node"));
  }

  // Seed when the persisted config is fresh or single-self; trust otherwise.
  // size==0 covers partial-write / corruption (GvdbStateManager normally
  // leaves at least self, but we still want to recover cleanly).
  plan.needs_seed = persisted_cluster_size <= 1;
  return plan;
}

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

    // If the peer probe during InitializeNuRaft found a live leader, we
    // skipped self-seeding and must now announce our join. Failure here
    // is fatal for startup: we have no cluster membership and no quorum.
    // K8s will restart us, and the next probe will try again (roadmap 1.7b).
    if (!joining_peer_.empty()) {
      auto join_status = AnnounceJoinToCluster();
      if (!join_status.ok()) {
        utils::Logger::Instance().Error(
            "AnnounceJoinToCluster failed: {}", join_status.message());
        return join_status;
      }
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

uint64_t RaftNode::GetCurrentTerm() const {
  // NuRaft's raft_server exposes get_term() returning ulong; single-node
  // has no term concept — return 0 so the operator treats the field as
  // "don't know" rather than a spurious stable value.
  if (config_.single_node_mode || !raft_server_) {
    return 0;
  }
  return static_cast<uint64_t>(raft_server_->get_term());
}

namespace {

// Result of the startup peer-probe. Populated only when the probe found a
// live leader on one of the declared coordinator gRPC peers.
struct LeaderProbeResult {
  int32_t leader_id = 0;
  // Endpoint that RESPONDED (may itself be the leader, or a follower that
  // knows the leader's id). Used by AnnounceJoin to call JoinCluster.
  std::string responding_peer;
};

// Probe each peer endpoint for a live Raft leader via GetLeaderInfo. Used
// on fresh-PVC startup (persisted_cluster_size <= 1) to distinguish
// "initial bootstrap" from "runtime scale-up": if any peer reports
// leader_id > 0 we're joining an existing cluster and must NOT seed our
// own cluster_config (roadmap 1.7b).
//
// Each RPC is bounded at 1.5s (matches the operator's existing pattern
// in operator/internal/gvdbclient/client.go:230-243). Overall budget is
// the sum across peers; typically 3 peers × 1.5s = 4.5s worst case.
std::optional<LeaderProbeResult> ProbePeersForLeader(
    const std::vector<std::string>& grpc_peers) {
  for (const auto& target : grpc_peers) {
    if (target.empty()) continue;
    auto channel = grpc::CreateChannel(target,
                                      grpc::InsecureChannelCredentials());
    auto stub = proto::internal::InternalService::NewStub(channel);

    proto::internal::GetLeaderInfoRequest req;
    proto::internal::GetLeaderInfoResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(1500));

    auto status = stub->GetLeaderInfo(&ctx, req, &resp);
    if (!status.ok()) {
      utils::Logger::Instance().Debug(
          "Peer probe: {} unreachable ({})", target, status.error_message());
      continue;
    }
    if (resp.leader_id() > 0) {
      utils::Logger::Instance().Info(
          "Peer probe: {} reports leader_id={} (term={})",
          target, resp.leader_id(), resp.current_term());
      return LeaderProbeResult{resp.leader_id(), target};
    }
    utils::Logger::Instance().Debug(
        "Peer probe: {} has no leader yet (term={})",
        target, resp.current_term());
  }
  return std::nullopt;
}

// Map a NuRaft cmd_result_code to a GVDB core::Status. Only the codes that
// can arise from add_srv / remove_srv are enumerated; anything else falls
// through to InternalError so the caller sees an opaque failure rather than
// silently swallowing a new error path added in a future NuRaft version.
core::Status MapNuRaftCode(nuraft::cmd_result_code code, const char* op) {
  using namespace nuraft;
  switch (code) {
    case cmd_result_code::OK:
      return core::OkStatus();
    case cmd_result_code::NOT_LEADER:
      return core::FailedPreconditionError(
          absl::StrCat(op, " rejected: not leader"));
    case cmd_result_code::SERVER_ALREADY_EXISTS:
      // Idempotent outcome — the peer was already a member. Surface as OK
      // so re-entrancy on a retry doesn't break the caller.
      return core::OkStatus();
    case cmd_result_code::SERVER_NOT_FOUND:
      // Same logic: removing a peer that's not there is a no-op, treat OK.
      return core::OkStatus();
    case cmd_result_code::CONFIG_CHANGING:
      return core::UnavailableError(
          absl::StrCat(op, " busy: another membership change in progress"));
    case cmd_result_code::SERVER_IS_JOINING:
      return core::UnavailableError(
          absl::StrCat(op, " busy: peer still catching up"));
    case cmd_result_code::SERVER_IS_LEAVING:
      return core::UnavailableError(
          absl::StrCat(op, " busy: peer is mid-leave"));
    case cmd_result_code::CANNOT_REMOVE_LEADER:
      return core::FailedPreconditionError(
          absl::StrCat(op, " rejected: cannot remove current leader; "
                           "transfer leadership first"));
    case cmd_result_code::TIMEOUT:
      return core::DeadlineExceededError(absl::StrCat(op, " timed out"));
    case cmd_result_code::CANCELLED:
      return core::CancelledError(absl::StrCat(op, " cancelled"));
    case cmd_result_code::BAD_REQUEST:
      return core::InvalidArgumentError(absl::StrCat(op, " bad request"));
    case cmd_result_code::TERM_MISMATCH:
      return core::FailedPreconditionError(
          absl::StrCat(op, " term mismatch; retry"));
    default:
      return core::InternalError(
          absl::StrCat(op, " failed with NuRaft code ",
                       static_cast<int>(code)));
  }
}

}  // namespace

core::Status RaftNode::AddPeer(int32_t node_id, const std::string& raft_endpoint) {
  if (config_.single_node_mode) {
    return core::FailedPreconditionError(
        "AddPeer not supported in single-node mode");
  }
  if (!raft_server_) {
    return core::FailedPreconditionError("RaftNode not initialized");
  }
  // Leader-only guard; NuRaft would reject with NOT_LEADER anyway, but
  // checking here lets us surface a clearer error to the RPC caller
  // (JoinCluster handler populates current_leader_id from this path).
  if (!raft_server_->is_leader()) {
    return core::FailedPreconditionError(
        absl::StrCat("AddPeer rejected: not leader (current leader=",
                     raft_server_->get_leader(), ")"));
  }

  utils::Logger::Instance().Info(
      "RaftNode::AddPeer node_id={} endpoint={}", node_id, raft_endpoint);

  nuraft::srv_config new_srv(node_id, raft_endpoint);
  auto result = raft_server_->add_srv(new_srv);
  if (!result) {
    return core::InternalError("add_srv returned null cmd_result");
  }
  // Block until the membership change is committed or NuRaft reports an
  // error. The async handler pattern is available via when_ready() but
  // this RPC is already bounded by the caller's gRPC deadline so a
  // synchronous wait keeps the handler simple.
  (void)result->get();  // block until NuRaft commits / errors; we read the code below
  return MapNuRaftCode(result->get_result_code(), "AddPeer");
}

core::Status RaftNode::RemovePeer(int32_t node_id) {
  if (config_.single_node_mode) {
    return core::FailedPreconditionError(
        "RemovePeer not supported in single-node mode");
  }
  if (!raft_server_) {
    return core::FailedPreconditionError("RaftNode not initialized");
  }
  if (!raft_server_->is_leader()) {
    return core::FailedPreconditionError(
        absl::StrCat("RemovePeer rejected: not leader (current leader=",
                     raft_server_->get_leader(), ")"));
  }

  utils::Logger::Instance().Info("RaftNode::RemovePeer node_id={}", node_id);

  auto result = raft_server_->remove_srv(node_id);
  if (!result) {
    return core::InternalError("remove_srv returned null cmd_result");
  }
  (void)result->get();  // block until NuRaft commits / errors; we read the code below
  return MapNuRaftCode(result->get_result_code(), "RemovePeer");
}

core::Status RaftNode::AnnounceJoinToCluster() {
  // Called from Start() after InitializeNuRaft returns, only when
  // joining_peer_ was set by the peer probe. Calls JoinCluster on the
  // responding peer; if that peer is a follower, it returns NOT_LEADER
  // with current_leader_id, and we retry against the leader (roadmap 1.7b).
  if (joining_peer_.empty() || config_.coordinator_grpc_peers.empty()) {
    return core::FailedPreconditionError(
        "AnnounceJoinToCluster called without joining_peer_ set");
  }

  const std::string self_endpoint = config_.advertise_address.empty()
      ? config_.listen_address
      : config_.advertise_address;

  // Build a per-node-id → gRPC endpoint map from the configured peer list.
  // Relies on the stable ordering + size of coordinator_grpc_peers matching
  // the declared --raft-peers list (both rendered from the same
  // CoordinatorRaftPeers helper in the operator).
  auto resolve_leader_target = [&](int32_t leader_id) -> std::string {
    // coordinator_grpc_peers is 0-indexed; node_ids start at 1 (ORDINAL+1).
    size_t idx = static_cast<size_t>(leader_id - 1);
    if (idx < config_.coordinator_grpc_peers.size()) {
      return config_.coordinator_grpc_peers[idx];
    }
    return "";
  };

  std::string target = joining_peer_;
  constexpr int kMaxAttempts = 5;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto channel = grpc::CreateChannel(target,
                                      grpc::InsecureChannelCredentials());
    auto stub = proto::internal::InternalService::NewStub(channel);

    proto::internal::JoinClusterRequest req;
    req.set_node_id(static_cast<uint32_t>(config_.node_id));
    req.set_raft_advertise_address(self_endpoint);

    proto::internal::JoinClusterResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));

    auto status = stub->JoinCluster(&ctx, req, &resp);
    if (!status.ok()) {
      utils::Logger::Instance().Warn(
          "JoinCluster attempt {} to {} failed: {}",
          attempt, target, status.error_message());
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    if (resp.success()) {
      utils::Logger::Instance().Info(
          "JoinCluster succeeded on attempt {} via {}: {}",
          attempt, target, resp.message());
      joining_peer_.clear();
      return core::OkStatus();
    }

    // Non-leader redirect: follow the returned leader id.
    if (resp.current_leader_id() > 0) {
      std::string new_target = resolve_leader_target(resp.current_leader_id());
      if (!new_target.empty() && new_target != target) {
        utils::Logger::Instance().Info(
            "JoinCluster redirected to leader_id={} ({})",
            resp.current_leader_id(), new_target);
        target = new_target;
        continue;  // retry immediately against leader
      }
    }

    // No leader id or unresolvable — back off and retry against a
    // different probe peer.
    utils::Logger::Instance().Warn(
        "JoinCluster denied on attempt {}: {} (leader_id={})",
        attempt, resp.message(), resp.current_leader_id());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  return core::UnavailableError(absl::StrCat(
      "AnnounceJoinToCluster: no coordinator accepted the join after ",
      kMaxAttempts, " attempts"));
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

  // The NuRaft srv_config endpoint is what peers use to connect to us —
  // prefer the explicit advertise_address (K8s FQDN) and fall back to
  // listen_address for single-node / backward-compat paths (roadmap 0b.4).
  const std::string& advertise = config_.advertise_address.empty()
      ? config_.listen_address
      : config_.advertise_address;

  // Persistent log+state so Raft survives coordinator restart (required for HA).
  const std::filesystem::path raft_dir(config_.data_dir);
  const std::string log_path = (raft_dir / "log").string();
  const std::string state_path = (raft_dir / "state").string();
  state_mgr_ = std::make_shared<GvdbStateManager>(
      config_.node_id,
      advertise,
      log_path,
      state_path);

  // Seed the cluster configuration with the declared peers (roadmap 0b.4).
  // Accepted format for each entry: "id:host:port" — self is identified by
  // matching `id` against `config_.node_id` and skipped. Without this step
  // NuRaft starts with a single-server cluster config and no quorum ever
  // forms. This runs BEFORE launcher_->init() so the initial cluster_config
  // contains all intended members at bootstrap.
  //
  // On subsequent restarts the state manager has a persisted cluster_config
  // with more than one server — we trust that and skip reseeding to avoid
  // clobbering runtime membership changes.
  if (!config_.peers.empty()) {
    auto existing = state_mgr_->load_config();
    auto plan = PrepareRaftPeerList(config_.node_id, config_.peers,
                                    existing->get_servers().size());
    if (!plan.ok()) return plan.status();

    if (plan->needs_seed) {
      // Bootstrap-vs-join detection (roadmap 1.7b). Probe declared
      // coordinator gRPC peers for a live Raft leader. If one is found, we
      // are runtime-joining an existing cluster: leave existing config as
      // just-self, remember the responding peer for an AnnounceJoin call
      // after NuRaft is up. If no peer reports a leader, fall through to
      // the legacy seed path (initial cold-start of a fresh 3-node deploy).
      auto probe = ProbePeersForLeader(config_.coordinator_grpc_peers);
      if (probe.has_value()) {
        joining_peer_ = probe->responding_peer;
        state_mgr_->save_config(*existing);
        utils::Logger::Instance().Info(
            "Peer probe found leader_id={} on {}; will AnnounceJoin after "
            "NuRaft init (cluster_config stays self-only pre-join)",
            probe->leader_id, probe->responding_peer);
      } else {
        for (const auto& peer : plan->peers) {
          if (peer.id == config_.node_id) continue;  // self already in config
          existing->get_servers().push_back(
              nuraft::cs_new<nuraft::srv_config>(peer.id, peer.endpoint));
        }
        state_mgr_->save_config(*existing);
        utils::Logger::Instance().Info(
            "Seeded cluster_config with {} server(s) including self "
            "(no existing leader found via peer probe — initial bootstrap)",
            existing->get_servers().size());
      }
    } else {
      utils::Logger::Instance().Info(
          "Using persisted cluster_config with {} server(s)",
          existing->get_servers().size());
    }
  }

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
  init_opts.start_server_in_constructor_ = true;

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