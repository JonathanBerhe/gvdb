// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Joining a Raft group: the --raft-peers parsing, the startup probe for a
// live leader, and the join request to that leader.

#include "consensus/raft_node.h"

#include <chrono>
#include <optional>
#include <set>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "common/logger.h"

namespace gvdb::consensus {

absl::StatusOr<RaftPeerSpec> ParseRaftPeerSpec(const std::string& spec) {
  // Be tolerant of whitespace introduced by hand-edited YAML / env vars:
  // strip leading/trailing whitespace on the whole spec, the id, and the
  // endpoint. Reject any embedded whitespace in the id or inside the
  // endpoint's host/port to keep errors unambiguous.
  auto strip = [](const std::string& s) {
    auto begin = s.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return std::string();
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(begin, end - begin + 1);
  };

  std::string trimmed = strip(spec);
  auto first_colon = trimmed.find(':');
  if (first_colon == std::string::npos || first_colon == 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Raft peer must be 'id:host:port', got: ", spec));
  }
  RaftPeerSpec out;
  std::string id_str = strip(trimmed.substr(0, first_colon));
  if (id_str.find_first_of(" \t\r\n") != std::string::npos) {
    return absl::InvalidArgumentError(absl::StrCat("Raft peer id contains whitespace in: ", spec));
  }
  try {
    out.id = std::stoi(id_str);
  } catch (const std::exception&) {
    return absl::InvalidArgumentError(absl::StrCat("Raft peer id must be an integer in: ", spec));
  }
  if (out.id <= 0) {
    return absl::InvalidArgumentError(absl::StrCat("Raft peer id must be > 0 in: ", spec));
  }
  out.endpoint = strip(trimmed.substr(first_colon + 1));
  if (out.endpoint.empty() || out.endpoint.find(':') == std::string::npos ||
      out.endpoint.find(':') == out.endpoint.size() - 1 ||
      out.endpoint.find_first_of(" \t\r\n") != std::string::npos) {
    return absl::InvalidArgumentError(
        absl::StrCat("Raft peer endpoint must be 'host:port' in: ", spec));
  }
  return out;
}

absl::StatusOr<PeerListPlan> PrepareRaftPeerList(int self_id,
                                                 const std::vector<std::string>& declared_peers,
                                                 size_t persisted_cluster_size) {
  PeerListPlan plan;
  std::set<int> seen_ids;
  bool self_found = false;

  for (const auto& raw : declared_peers) {
    auto parsed = ParseRaftPeerSpec(raw);
    if (!parsed.ok()) return parsed.status();

    if (!seen_ids.insert(parsed->id).second) {
      return absl::InvalidArgumentError(
          absl::StrCat("Duplicate Raft peer id ", parsed->id, " in --raft-peers"));
    }
    if (parsed->id == self_id) self_found = true;
    plan.peers.push_back(*parsed);
  }

  if (!plan.peers.empty() && !self_found) {
    return absl::InvalidArgumentError(absl::StrCat("--node-id ", self_id,
                                                   " does not appear in --raft-peers, the declared "
                                                   "peer list must include this node"));
  }

  // Seed when the persisted config is fresh or single-self; trust otherwise.
  // size==0 covers partial-write / corruption (GvdbStateManager normally
  // leaves at least self, but we still want to recover cleanly).
  plan.needs_seed = persisted_cluster_size <= 1;
  return plan;
}

// Probes each peer endpoint for a live Raft leader via GetLeaderInfo. Used
// on fresh-PVC startup (persisted_cluster_size <= 1) to distinguish
// "initial bootstrap" from "runtime scale-up": if any peer reports
// leader_id > 0 we're joining an existing cluster and must NOT seed our
// own cluster_config.
//
// Each RPC is bounded at 1.5s (matches the operator's existing pattern
// in operator/internal/gvdbclient/client.go:230-243). The probe is
// INTENTIONALLY serial: on runtime scale-up the first peer (lowest
// ordinal) is almost always the leader or a live follower that knows the
// leader, so the common-case latency is one round-trip. The pathological
// bootstrap case (N peers × 1.5s, all timing out because no leader has
// been elected yet) is acceptable because it only happens once per pod
// lifetime. Parallel probes would cut that to 1.5s flat but at the cost
// of more connections + complexity for a rarely-hit path.
std::optional<RaftNode::LeaderProbeResult> RaftNode::ProbePeersForLeader() const {
  for (const auto& target : config_.coordinator_grpc_peers) {
    if (target.empty()) continue;
    auto info = peer_client_->GetLeaderInfo(target, std::chrono::milliseconds(1500));
    if (!info.ok()) {
      common::Logger::Instance().Debug("Peer probe: {} unreachable ({})", target,
                                       info.status().message());
      continue;
    }
    if (info->leader_id > 0) {
      common::Logger::Instance().Info("Peer probe: {} reports leader_id={} (term={})", target,
                                      info->leader_id, info->current_term);
      return LeaderProbeResult{info->leader_id, target};
    }
    common::Logger::Instance().Debug("Peer probe: {} has no leader yet (term={})", target,
                                     info->current_term);
  }
  return std::nullopt;
}

absl::Status RaftNode::AnnounceJoinToCluster() {
  // Called from Start() after InitializeNuRaft returns, only when
  // joining_peer_ was set by the peer probe. Calls JoinCluster on the
  // responding peer; if that peer is a follower, it returns NOT_LEADER
  // with current_leader_id, and we retry against the leader.
  //
  // Retry strategy: exponential backoff (250ms → 500ms → 1s → 2s → 4s,
  // capped) bounded by an absolute wall-clock deadline of kTotalBudget.
  // Concurrent scale-up (two pods joining simultaneously) is the dominant
  // hard case: the leader serializes via NuRaft's CONFIG_CHANGING gate,
  // so the second pod sees Unavailable until the first commits. add_srv
  // typically commits in 100-300ms; snapshot-install for log catch-up
  // can extend that to seconds. 20s total budget accommodates both.
  if (joining_peer_.empty() || config_.coordinator_grpc_peers.empty()) {
    return absl::FailedPreconditionError("AnnounceJoinToCluster called without joining_peer_ set");
  }

  const std::string self_endpoint =
      config_.advertise_address.empty() ? config_.listen_address : config_.advertise_address;

  // Relies on the render-layer invariant that coordinator_grpc_peers
  // iterates ordinals 0..N-1 in the same order as --raft-peers. Validated
  // at coordinator startup (ValidatePeerListLengths in raft_node.cpp).
  // Index by (node_id - 1) since node_ids start at 1.
  auto resolve_leader_target = [&](int32_t leader_id) -> std::string {
    auto idx = static_cast<size_t>(leader_id - 1);
    if (idx < config_.coordinator_grpc_peers.size()) {
      return config_.coordinator_grpc_peers[idx];
    }
    return "";
  };

  constexpr auto kTotalBudget = std::chrono::seconds(20);
  constexpr auto kInitialBackoff = std::chrono::milliseconds(250);
  constexpr auto kMaxBackoff = std::chrono::seconds(4);
  const auto deadline = std::chrono::steady_clock::now() + kTotalBudget;

  std::string target = joining_peer_;
  auto backoff = kInitialBackoff;
  int attempt = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    ++attempt;
    auto reply =
        peer_client_->JoinCluster(target, config_.node_id, self_endpoint, std::chrono::seconds(5));
    if (!reply.ok()) {
      common::Logger::Instance().Warn("JoinCluster attempt {} to {} transport failure: {}", attempt,
                                      target, reply.status().message());
    } else if (reply->success) {
      common::Logger::Instance().Info("JoinCluster succeeded on attempt {} via {}: {}", attempt,
                                      target, reply->message);
      joining_peer_.clear();
      return absl::OkStatus();
    } else if (reply->current_leader_id > 0) {
      // Non-leader redirect: follow the returned leader id without
      // consuming extra backoff (the target was simply wrong).
      std::string new_target = resolve_leader_target(reply->current_leader_id);
      if (!new_target.empty() && new_target != target) {
        common::Logger::Instance().Info("JoinCluster redirected to leader_id={} ({})",
                                        reply->current_leader_id, new_target);
        target = new_target;
        continue;
      }
      common::Logger::Instance().Warn(
          "JoinCluster redirect to leader_id={} unresolvable (grpc_peers has {} entries); "
          "will retry on same target",
          reply->current_leader_id, config_.coordinator_grpc_peers.size());
    } else {
      // Non-success without a leader hint: usually CONFIG_CHANGING from
      // a concurrent add_srv. The message surfaces the NuRaft code for
      // diagnosability. Back off and retry the same target (it may still
      // be leader, just busy).
      common::Logger::Instance().Warn(
          "JoinCluster attempt {} denied: {} (leader_id=0; likely concurrent config change)",
          attempt, reply->message);
    }

    // Exponential backoff, capped at kMaxBackoff. Don't oversleep the deadline.
    auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::milliseconds(0)) break;
    auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    auto sleep_for = std::min(backoff, remaining_ms);
    std::this_thread::sleep_for(sleep_for);
    // kMaxBackoff is seconds; cast to milliseconds for comparison.
    auto max_backoff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(kMaxBackoff);
    backoff = std::min(backoff * 2, max_backoff_ms);
  }

  return absl::UnavailableError(
      absl::StrCat("AnnounceJoinToCluster: no coordinator accepted the join after ", attempt,
                   " attempts within ", kTotalBudget.count(), "s"));
}

}  // namespace gvdb::consensus
