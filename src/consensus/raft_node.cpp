// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/raft_node.h"
#include "core/status.h"

#include <libnuraft/nuraft.hxx>

#include <chrono>
#include <filesystem>
#include <optional>
#include <thread>
#include <utility>

#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "consensus/gvdb_state_manager.h"
#include "consensus/metadata_state_machine.h"
#include "consensus/nuraft_logger.h"

namespace gvdb::consensus {

absl::StatusOr<std::unique_ptr<RaftNode>> RaftNode::Create(
    const RaftConfig& config, std::shared_ptr<IRaftPeerClient> peer_client) {
  if (!config.IsValid()) {
    return absl::InvalidArgumentError("Invalid Raft configuration");
  }
  return std::unique_ptr<RaftNode>(new RaftNode(config, std::move(peer_client)));
}

RaftNode::RaftNode(RaftConfig config, std::shared_ptr<IRaftPeerClient> peer_client)
    : config_(std::move(config)), peer_client_(std::move(peer_client)) {
  common::Logger::Instance().Info("RaftNode created (node_id={}, single_node_mode={})",
                                  config_.node_id, config_.single_node_mode);
}

RaftNode::~RaftNode() {
  if (running_.load(std::memory_order_acquire)) {
    Shutdown().IgnoreError();
  }
}

absl::Status RaftNode::Start() {
  std::scoped_lock lock(mutex_);

  if (running_.load(std::memory_order_acquire)) {
    return absl::FailedPreconditionError("RaftNode already running");
  }

  common::Logger::Instance().Info("Starting RaftNode (node_id={})", config_.node_id);

  // In single-node mode, immediately become leader
  if (config_.single_node_mode) {
    is_leader_.store(true, std::memory_order_release);
    common::Logger::Instance().Info("RaftNode is leader (single-node mode)");
  } else {
    // Multi-node mode: Initialize NuRaft.
    //
    // Invariant check: coordinator_grpc_peers must match --raft-peers in
    // size and ordinal order (render layer guarantees this; the binary's
    // leader-redirect uses node_id-1 as the index into grpc_peers). When
    // coordinator_grpc_peers is empty, auto-join is disabled and we fall
    // back to initial-bootstrap semantics only: don't enforce.
    if (!config_.coordinator_grpc_peers.empty() &&
        config_.coordinator_grpc_peers.size() != config_.peers.size()) {
      return absl::InvalidArgumentError(
          absl::StrCat("coordinator_grpc_peers size (", config_.coordinator_grpc_peers.size(),
                       ") must match --raft-peers size (", config_.peers.size(),
                       ") for correct leader-redirect indexing"));
    }
    if (!config_.coordinator_grpc_peers.empty() && !peer_client_) {
      return absl::InvalidArgumentError(
          "coordinator_grpc_peers is set, but the RaftNode has no peer client "
          "for the startup probe and the join request");
    }

    RETURN_IF_ERROR(InitializeNuRaft());

    // If the peer probe during InitializeNuRaft found a live leader, we
    // skipped self-seeding and must now announce our join. Failure here
    // is fatal for startup: we have no cluster membership and no quorum.
    // K8s will restart us, and the next probe will try again.
    if (!joining_peer_.empty()) {
      auto join_status = AnnounceJoinToCluster();
      if (!join_status.ok()) {
        common::Logger::Instance().Error("AnnounceJoinToCluster failed: {}", join_status.message());
        return join_status;
      }
    }

    // Leader election happens asynchronously in NuRaft
    // The raft_server will trigger callbacks when leadership changes
    // For now, we don't set is_leader_ - it will be determined by querying raft_server
    common::Logger::Instance().Info(
        "RaftNode started in multi-node mode (leader election in progress)");
  }

  running_.store(true, std::memory_order_release);

  common::Logger::Instance().Info("RaftNode started successfully");
  return absl::OkStatus();
}

absl::Status RaftNode::Shutdown() {
  std::scoped_lock lock(mutex_);

  if (!running_.load(std::memory_order_acquire)) {
    return absl::OkStatus();  // Already stopped
  }

  common::Logger::Instance().Info("Shutting down RaftNode");

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

  common::Logger::Instance().Info("RaftNode shut down successfully");
  return absl::OkStatus();
}

bool RaftNode::IsRunning() const { return running_.load(std::memory_order_acquire); }

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
  // has no term concept: return 0 so the operator treats the field as
  // "don't know" rather than a spurious stable value.
  if (config_.single_node_mode || !raft_server_) {
    return 0;
  }
  return static_cast<uint64_t>(raft_server_->get_term());
}

namespace {

// Map a NuRaft cmd_result_code to a GVDB absl::Status. Only the codes that
// can arise from add_srv / remove_srv are enumerated; anything else falls
// through to InternalError so the caller sees an opaque failure rather than
// silently swallowing a new error path added in a future NuRaft version.
absl::Status MapNuRaftCode(nuraft::cmd_result_code code, const char* op) {
  using namespace nuraft;
  switch (code) {
    case cmd_result_code::OK:
      return absl::OkStatus();
    case cmd_result_code::NOT_LEADER:
      return absl::FailedPreconditionError(absl::StrCat(op, " rejected: not leader"));
    case cmd_result_code::SERVER_ALREADY_EXISTS:
      // Idempotent outcome: the peer was already a member. Surface as OK
      // so re-entrancy on a retry doesn't break the caller, but log so
      // the swallow is observable (useful diagnostic when operators chase
      // "why did JoinCluster return success but the peer never joined?").
      common::Logger::Instance().Info("{} idempotent: peer already present in cluster_config", op);
      return absl::OkStatus();
    case cmd_result_code::SERVER_NOT_FOUND:
      // Same logic: removing a peer that's not there is a no-op, treat OK.
      common::Logger::Instance().Info("{} idempotent: peer not present in cluster_config", op);
      return absl::OkStatus();
    case cmd_result_code::CONFIG_CHANGING:
      return absl::UnavailableError(
          absl::StrCat(op, " busy: another membership change in progress"));
    case cmd_result_code::SERVER_IS_JOINING:
      return absl::UnavailableError(absl::StrCat(op, " busy: peer still catching up"));
    case cmd_result_code::SERVER_IS_LEAVING:
      return absl::UnavailableError(absl::StrCat(op, " busy: peer is mid-leave"));
    case cmd_result_code::CANNOT_REMOVE_LEADER:
      return absl::FailedPreconditionError(absl::StrCat(op,
                                                        " rejected: cannot remove current leader; "
                                                        "transfer leadership first"));
    case cmd_result_code::TIMEOUT:
      return absl::DeadlineExceededError(absl::StrCat(op, " timed out"));
    case cmd_result_code::CANCELLED:
      return absl::CancelledError(absl::StrCat(op, " cancelled"));
    case cmd_result_code::BAD_REQUEST:
      return absl::InvalidArgumentError(absl::StrCat(op, " bad request"));
    case cmd_result_code::TERM_MISMATCH:
      return absl::FailedPreconditionError(absl::StrCat(op, " term mismatch; retry"));
    default:
      return absl::InternalError(
          absl::StrCat(op, " failed with NuRaft code ", std::to_underlying(code)));
  }
}

}  // namespace

absl::Status RaftNode::AddPeer(int32_t node_id, const std::string& raft_endpoint) {
  if (config_.single_node_mode) {
    return absl::FailedPreconditionError("AddPeer not supported in single-node mode");
  }
  if (!raft_server_) {
    return absl::FailedPreconditionError("RaftNode not initialized");
  }
  // Leader-only guard; NuRaft would reject with NOT_LEADER anyway, but
  // checking here lets us surface a clearer error to the RPC caller
  // (JoinCluster handler populates current_leader_id from this path).
  if (!raft_server_->is_leader()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "AddPeer rejected: not leader (current leader=", raft_server_->get_leader(), ")"));
  }

  common::Logger::Instance().Info("RaftNode::AddPeer node_id={} endpoint={}", node_id,
                                  raft_endpoint);

  nuraft::srv_config new_srv(node_id, raft_endpoint);
  auto result = raft_server_->add_srv(new_srv);
  if (!result) {
    return absl::InternalError("add_srv returned null cmd_result");
  }
  // Block until the membership change is committed or NuRaft reports an
  // error. The async handler pattern is available via when_ready() but
  // this RPC is already bounded by the caller's gRPC deadline so a
  // synchronous wait keeps the handler simple.
  (void)result->get();  // block until NuRaft commits / errors; we read the code below
  return MapNuRaftCode(result->get_result_code(), "AddPeer");
}

absl::Status RaftNode::RemovePeer(int32_t node_id) {
  if (config_.single_node_mode) {
    return absl::FailedPreconditionError("RemovePeer not supported in single-node mode");
  }
  if (!raft_server_) {
    return absl::FailedPreconditionError("RaftNode not initialized");
  }
  if (!raft_server_->is_leader()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "RemovePeer rejected: not leader (current leader=", raft_server_->get_leader(), ")"));
  }

  common::Logger::Instance().Info("RaftNode::RemovePeer node_id={}", node_id);

  auto result = raft_server_->remove_srv(node_id);
  if (!result) {
    return absl::InternalError("remove_srv returned null cmd_result");
  }
  (void)result->get();  // block until NuRaft commits / errors; we read the code below
  return MapNuRaftCode(result->get_result_code(), "RemovePeer");
}

std::vector<RaftNode::RaftMemberInfo> RaftNode::GetClusterMembership() const {
  std::vector<RaftMemberInfo> out;
  if (config_.single_node_mode) {
    out.push_back(
        {config_.node_id,
         config_.advertise_address.empty() ? config_.listen_address : config_.advertise_address,
         /*is_learner=*/false});
    return out;
  }
  if (!raft_server_) return out;

  auto cfg = raft_server_->get_config();
  if (!cfg) return out;

  for (const auto& srv : cfg->get_servers()) {
    if (!srv) continue;
    out.push_back({static_cast<int32_t>(srv->get_id()), srv->get_endpoint(), srv->is_learner()});
  }
  return out;
}

absl::Status RaftNode::YieldLeadership(int32_t target_node_id) {
  if (config_.single_node_mode) {
    return absl::FailedPreconditionError("YieldLeadership not supported in single-node mode");
  }
  if (!raft_server_) {
    return absl::FailedPreconditionError("RaftNode not initialized");
  }
  if (!raft_server_->is_leader()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "YieldLeadership rejected: not leader (current leader=", raft_server_->get_leader(), ")"));
  }
  if (target_node_id == config_.node_id) {
    // No-op: we're already the leader. NuRaft would silently ignore a
    // self-target; return OK so the caller's success-predicate
    // (GetLeaderId() == target) holds immediately.
    return absl::OkStatus();
  }

  common::Logger::Instance().Info("RaftNode::YieldLeadership target_node_id={} (current leader={})",
                                  target_node_id, raft_server_->get_leader());

  // Kick off the transfer. NuRaft's yield_leadership returns void; we
  // synthesize success by polling get_leader() with a bounded deadline.
  raft_server_->yield_leadership(/*immediate_yield=*/false, target_node_id);

  // Poll every 100ms up to 3s. Typical transfer commits in 200-500ms
  // on localhost; 3s covers NuRaft's election timeout window
  // (election_timeout_upper_bound_ = 400ms in raft_node.cpp:664).
  constexpr auto kTransferTimeout = std::chrono::seconds(3);
  constexpr auto kPollInterval = std::chrono::milliseconds(100);
  const auto deadline = std::chrono::steady_clock::now() + kTransferTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (raft_server_->get_leader() == target_node_id) {
      common::Logger::Instance().Info("YieldLeadership: target {} is now leader", target_node_id);
      return absl::OkStatus();
    }
    std::this_thread::sleep_for(kPollInterval);
  }
  return absl::DeadlineExceededError(absl::StrCat(
      "YieldLeadership: target ", target_node_id, " did not become leader within ",
      kTransferTimeout.count(), "s (current leader=", raft_server_->get_leader(), ")"));
}

absl::Status RaftNode::InitializeNuRaft() {
  common::Logger::Instance().Info("Initializing NuRaft for multi-node mode");

  // Create NuRaft logger adapter
  nuraft_logger_ = nuraft::cs_new<NuRaftLoggerAdapter>();
  nuraft_logger_->set_level(4);  // INFO level

  // Create the NuRaft state machine
  state_machine_ = std::make_shared<MetadataStateMachine>();

  // The NuRaft srv_config endpoint is what peers use to connect to us:
  // prefer the explicit advertise_address (K8s FQDN) and fall back to
  // listen_address for single-node / backward-compat paths.
  const std::string& advertise =
      config_.advertise_address.empty() ? config_.listen_address : config_.advertise_address;

  // Persistent log+state so Raft survives coordinator restart (required for HA).
  const std::filesystem::path raft_dir(config_.data_dir);
  const std::string log_path = (raft_dir / "log").string();
  const std::string state_path = (raft_dir / "state").string();
  ASSIGN_OR_RETURN(state_mgr_,
                   GvdbStateManager::Create(config_.node_id, advertise, log_path, state_path));

  // Seed the cluster configuration with the declared peers.
  // Accepted format for each entry: "id:host:port", self is identified by
  // matching `id` against `config_.node_id` and skipped. Without this step
  // NuRaft starts with a single-server cluster config and no quorum ever
  // forms. This runs BEFORE launcher_->init() so the initial cluster_config
  // contains all intended members at bootstrap.
  //
  // On subsequent restarts the state manager has a persisted cluster_config
  // with more than one server: we trust that and skip reseeding to avoid
  // clobbering runtime membership changes.
  if (!config_.peers.empty()) {
    auto existing = state_mgr_->load_config();
    auto plan = PrepareRaftPeerList(config_.node_id, config_.peers, existing->get_servers().size());
    if (!plan.ok()) return plan.status();

    if (plan->needs_seed) {
      // Bootstrap-vs-join detection. Probe declared
      // coordinator gRPC peers for a live Raft leader. If one is found, we
      // are runtime-joining an existing cluster: leave existing config as
      // just-self, remember the responding peer for an AnnounceJoin call
      // after NuRaft is up. If no peer reports a leader, fall through to
      // the legacy seed path (initial cold-start of a fresh 3-node deploy).
      auto probe = ProbePeersForLeader();
      if (probe.has_value()) {
        joining_peer_ = probe->responding_peer;
        // Deliberately NOT calling save_config here: `existing` is
        // unmodified so there is nothing new to persist. The leader's
        // add_srv will replicate the real cluster_config to us via Raft
        // log once AnnounceJoinToCluster succeeds.
        common::Logger::Instance().Info(
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
        common::Logger::Instance().Info(
            "Seeded cluster_config with {} server(s) including self "
            "(no existing leader found via peer probe, initial bootstrap)",
            existing->get_servers().size());
      }
    } else {
      common::Logger::Instance().Info("Using persisted cluster_config with {} server(s)",
                                      existing->get_servers().size());
    }
  }

  // Parse port from listen_address for raft_launcher
  // Format: "host:port" -> extract port number
  size_t colon_pos = config_.listen_address.find(':');
  if (colon_pos == std::string::npos) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Invalid listen_address format (expected 'host:port'): ", config_.listen_address));
  }

  std::string port_str = config_.listen_address.substr(colon_pos + 1);
  int port = 0;
  try {
    port = std::stoi(port_str);
  } catch (const std::exception& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid port number in listen_address: ", port_str));
  }

  // Create Raft parameters
  nuraft::raft_params params;
  params.heart_beat_interval_ = 100;                      // 100ms heartbeat
  params.election_timeout_lower_bound_ = 200;             // 200ms min election timeout
  params.election_timeout_upper_bound_ = 400;             // 400ms max election timeout
  params.reserved_log_items_ = 10000;                     // Keep 10k log entries before snapshot
  params.snapshot_distance_ = 5000;                       // Create snapshot every 5k operations
  params.client_req_timeout_ = 3000;                      // 3s timeout for client requests
  params.return_method_ = nuraft::raft_params::blocking;  // Blocking mode for simplicity

  // Initialize Raft launcher
  nuraft::asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 4;  // 4 threads for async I/O

  launcher_ = nuraft::cs_new<nuraft::raft_launcher>();

  nuraft::raft_server::init_options init_opts;
  init_opts.skip_initial_election_timeout_ = false;  // Participate in election immediately
  init_opts.start_server_in_constructor_ = true;

  raft_server_ = launcher_->init(state_machine_, state_mgr_, nuraft_logger_, port, asio_opts,
                                 params, init_opts);

  if (!raft_server_) {
    return absl::InternalError("Failed to initialize NuRaft launcher (returned null server)");
  }

  common::Logger::Instance().Info("NuRaft initialized successfully (node_id={}, endpoint={})",
                                  config_.node_id, config_.listen_address);

  // Note: Leader election will happen asynchronously
  // Use IsLeader() to check leadership status
  return absl::OkStatus();
}

}  // namespace gvdb::consensus
