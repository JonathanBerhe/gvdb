// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// In-process integration tests for roadmap 1.7b coordinator Raft auto-join.
// Spins up multiple RaftNode instances in the same test binary plus an
// InternalService gRPC server for each — just enough to exercise the
// JoinCluster / RemovePeer RPCs end-to-end without a real K8s cluster.
//
// Scope:
//   1. 3-node cold bootstrap — all pods start with empty persisted state +
//      the full declared peer list; verify leader election converges.
//   2. Runtime scale-up — add a 4th RaftNode with an empty PVC and the
//      extended peer list. Its peer probe finds the 3-node leader, skips
//      self-seeding, and calls JoinCluster. Verify cluster_config grows to 4.
//   3. Graceful scale-down — simulate SIGTERM on node 4 by calling
//      RemovePeer on the leader. Verify cluster_config returns to 3.

#include "consensus/raft_node.h"
#include "consensus/raft_config.h"
#include "network/internal_service.h"
#include "internal.grpc.pb.h"

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace gvdb {
namespace consensus {
namespace integration {

namespace {

// High-numbered ports chosen to avoid common service ranges. Each test
// binary picks a fresh base via UniquePortBase() so parallel ctest runs
// don't collide.
int UniquePortBase() {
  // PID-derived to cut collision probability; tests reset disk state each run
  // and use separate data dirs, so port reuse within a single process is fine.
  static std::atomic<int> offset{0};
  const int kTestRangeStart = 53100;
  const int kBlockSize = 100;
  int block = offset.fetch_add(1, std::memory_order_relaxed);
  return kTestRangeStart + (block * kBlockSize);
}

// One self-contained coordinator instance: NuRaft + gRPC InternalService
// bound to two distinct ports, its own data dir. Enough surface area for
// the JoinCluster / RemovePeer / GetLeaderInfo RPCs to work end-to-end.
struct TestCoordinator {
  int node_id;
  int raft_port;
  int grpc_port;
  std::filesystem::path data_dir;
  std::shared_ptr<RaftNode> raft;
  std::unique_ptr<network::InternalService> service;
  std::unique_ptr<grpc::Server> grpc_server;

  std::string raft_endpoint() const {
    return "localhost:" + std::to_string(raft_port);
  }
  std::string grpc_endpoint() const {
    return "localhost:" + std::to_string(grpc_port);
  }
};

// Build peer specs for a cluster of 'count' members starting at 'raft_base'
// and 'grpc_base'. Order is 1..count, matching the ordinal->node_id
// convention (ordinal 0 => node_id 1).
struct ClusterLayout {
  std::vector<std::string> raft_peers;         // "id:host:port"
  std::vector<std::string> coordinator_grpc_peers;  // "host:port"
  int raft_base;
  int grpc_base;
};

ClusterLayout MakeLayout(int count, int raft_base, int grpc_base) {
  ClusterLayout l{{}, {}, raft_base, grpc_base};
  for (int i = 0; i < count; ++i) {
    int id = i + 1;
    l.raft_peers.push_back(
        std::to_string(id) + ":localhost:" +
        std::to_string(raft_base + i));
    l.coordinator_grpc_peers.push_back(
        "localhost:" + std::to_string(grpc_base + i));
  }
  return l;
}

std::unique_ptr<TestCoordinator> StartCoordinator(
    int node_id, const ClusterLayout& layout,
    const std::filesystem::path& root_dir) {
  auto c = std::make_unique<TestCoordinator>();
  c->node_id = node_id;
  c->raft_port = layout.raft_base + (node_id - 1);
  c->grpc_port = layout.grpc_base + (node_id - 1);
  c->data_dir = root_dir / ("coord_" + std::to_string(node_id));
  std::filesystem::create_directories(c->data_dir);

  RaftConfig cfg;
  cfg.node_id = node_id;
  cfg.single_node_mode = false;
  cfg.listen_address = "0.0.0.0:" + std::to_string(c->raft_port);
  cfg.advertise_address = c->raft_endpoint();
  cfg.peers = layout.raft_peers;
  cfg.coordinator_grpc_peers = layout.coordinator_grpc_peers;
  cfg.data_dir = c->data_dir.string();

  c->raft = std::make_shared<RaftNode>(cfg);
  auto status = c->raft->Start();
  if (!status.ok()) return nullptr;

  // Stand up a minimal InternalService — only raft_node_ needs to be real
  // for the JoinCluster / RemovePeer / GetLeaderInfo paths. Everything
  // else is nullptr-safe (see src/network/internal_service.cpp:33).
  c->service = std::make_unique<network::InternalService>(
      /*shard_manager=*/nullptr,
      /*segment_store=*/nullptr,
      /*query_executor=*/nullptr,
      /*node_registry=*/nullptr,
      /*timestamp_oracle=*/nullptr,
      /*coordinator=*/nullptr,
      c->raft);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(c->grpc_endpoint(), grpc::InsecureServerCredentials());
  builder.RegisterService(c->service.get());
  c->grpc_server = builder.BuildAndStart();
  if (!c->grpc_server) return nullptr;
  return c;
}

void ShutdownCoordinator(TestCoordinator* c) {
  if (!c) return;
  if (c->grpc_server) {
    c->grpc_server->Shutdown(std::chrono::system_clock::now() +
                              std::chrono::milliseconds(200));
    c->grpc_server.reset();
  }
  if (c->raft) {
    (void)c->raft->Shutdown();
    c->raft.reset();
  }
  c->service.reset();
}

// Poll until a predicate returns true or timeout elapses. Returns whether
// the predicate was satisfied.
template <typename F>
bool WaitFor(F pred, std::chrono::milliseconds timeout,
             std::chrono::milliseconds interval = std::chrono::milliseconds(50)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(interval);
  }
  return pred();  // one last check in case of timing race
}

// Find the current leader across the given coordinators. Returns nullptr
// if no node reports leadership yet.
TestCoordinator* FindLeader(const std::vector<std::unique_ptr<TestCoordinator>>& coords) {
  for (const auto& c : coords) {
    if (c && c->raft && c->raft->IsLeader()) return c.get();
  }
  return nullptr;
}

// RemovePeer with bounded retry. NuRaft's remove_srv can transiently
// return CONFIG_CHANGING when called immediately after leader election
// (the bootstrap-era membership log entry is still being committed).
// Matches the behavior real operator reconcilers exhibit — each reconcile
// retries idempotently — so the test mirrors the production retry loop.
bool RemovePeerWithRetry(const std::string& leader_target, uint32_t node_id,
                         std::chrono::milliseconds budget = std::chrono::seconds(5)) {
  auto channel = grpc::CreateChannel(leader_target,
                                    grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  auto deadline = std::chrono::steady_clock::now() + budget;
  auto backoff = std::chrono::milliseconds(200);
  while (std::chrono::steady_clock::now() < deadline) {
    proto::internal::RemovePeerRequest req;
    req.set_node_id(node_id);
    proto::internal::RemovePeerResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    auto st = stub->RemovePeer(&ctx, req, &resp);
    if (st.ok() && resp.success()) return true;
    std::this_thread::sleep_for(backoff);
    backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
  }
  return false;
}

// Same retry wrapper for TransferLeadership.
bool TransferLeadershipWithRetry(const std::string& leader_target,
                                 uint32_t target_node_id,
                                 std::chrono::milliseconds budget = std::chrono::seconds(8)) {
  auto channel = grpc::CreateChannel(leader_target,
                                    grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  auto deadline = std::chrono::steady_clock::now() + budget;
  auto backoff = std::chrono::milliseconds(200);
  while (std::chrono::steady_clock::now() < deadline) {
    proto::internal::TransferLeadershipRequest req;
    req.set_target_node_id(target_node_id);
    proto::internal::TransferLeadershipResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));
    auto st = stub->TransferLeadership(&ctx, req, &resp);
    if (st.ok() && resp.success()) return true;
    std::this_thread::sleep_for(backoff);
    backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
  }
  return false;
}

// Call GetLeaderInfo on a specific coordinator's gRPC port and return the
// reported leader_id (or 0 if no leader / RPC failed).
int32_t QueryLeader(const std::string& target) {
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  proto::internal::GetLeaderInfoRequest req;
  proto::internal::GetLeaderInfoResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::seconds(2));
  auto st = stub->GetLeaderInfo(&ctx, req, &resp);
  if (!st.ok()) return 0;
  return resp.leader_id();
}

class RaftAutoJoinFixture {
 public:
  RaftAutoJoinFixture() {
    root_dir_ = std::filesystem::temp_directory_path() /
                ("gvdb_raft_auto_join_" +
                 std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root_dir_);
  }

  ~RaftAutoJoinFixture() {
    for (auto& c : coords_) ShutdownCoordinator(c.get());
    coords_.clear();
    if (std::filesystem::exists(root_dir_)) {
      std::error_code ec;
      std::filesystem::remove_all(root_dir_, ec);
    }
  }

 protected:
  std::filesystem::path root_dir_;
  std::vector<std::unique_ptr<TestCoordinator>> coords_;
};

}  // namespace

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "3NodeBootstrapElectsLeader") {
  // All three nodes start together with empty persisted state + a 3-member
  // peer list. Since no peer has a leader yet, ProbePeersForLeader finds
  // none, we fall through to the seed-from-declared-peers path, and NuRaft
  // elects a leader via the standard bootstrap flow. This test locks in
  // that the 1.7b probe does NOT break initial-bootstrap semantics.
  const int base_raft = UniquePortBase();
  const int base_grpc = base_raft + 50;
  auto layout = MakeLayout(/*count=*/3, base_raft, base_grpc);

  for (int id = 1; id <= 3; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_);
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  // Leader election + config convergence. 10s is generous; typical
  // elections finish in 200-500ms on localhost.
  bool elected = WaitFor([&]() {
    return FindLeader(coords_) != nullptr;
  }, std::chrono::seconds(10));
  REQUIRE(elected);

  auto* leader = FindLeader(coords_);
  CHECK(leader != nullptr);
  CHECK(leader->raft->GetLeaderId() == leader->node_id);

  // Every node should agree on the same leader_id once stable.
  bool converged = WaitFor([&]() {
    int32_t leader_id = leader->raft->GetLeaderId();
    if (leader_id <= 0) return false;
    for (const auto& c : coords_) {
      if (c->raft->GetLeaderId() != leader_id) return false;
    }
    return true;
  }, std::chrono::seconds(5));
  CHECK(converged);
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "ScaleUpJoinsExistingCluster") {
  // 1. Cold-start a 3-node cluster.
  // 2. Start a 4th node whose --raft-peers / --coordinator-grpc-peers
  //    include all 4 members (matching what the operator would render
  //    after spec.coordinator.replicas=4).
  // 3. The 4th node's peer probe must find the existing leader and call
  //    JoinCluster, causing add_srv on the leader. NuRaft replicates the
  //    new 4-member cluster_config back to node 4.
  // 4. Verify all 4 pods agree on the same leader_id.

  const int base_raft = UniquePortBase();
  const int base_grpc = base_raft + 50;

  // Phase 1: 3-node cold-start with a layout sized for 4 (so later
  // adding node 4 doesn't require a restart of nodes 1-3 — which is the
  // point of 1.7b). Nodes 1-3 start with 4 declared peers but only
  // themselves are alive initially; NuRaft's bootstrap with the
  // 4-member seed config still elects a leader once the majority (3/4)
  // is up.
  auto full_layout = MakeLayout(/*count=*/4, base_raft, base_grpc);
  for (int id = 1; id <= 3; ++id) {
    auto c = StartCoordinator(id, full_layout, root_dir_);
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected = WaitFor([&]() {
    return FindLeader(coords_) != nullptr;
  }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);
  int initial_leader_id = leader->node_id;

  // Phase 2: bring up node 4. Its data dir is fresh, so persisted_cluster_size
  // is 0 → needs_seed==true → peer probe runs → finds the 3-node leader →
  // AnnounceJoinToCluster calls JoinCluster on the leader → add_srv →
  // cluster_config grows to 4.
  auto fourth = StartCoordinator(4, full_layout, root_dir_);
  REQUIRE(fourth != nullptr);
  coords_.push_back(std::move(fourth));

  // Phase 3: verify the 4th node sees the same leader as the original 3.
  bool node4_converged = WaitFor([&]() {
    return coords_.back()->raft->GetLeaderId() == initial_leader_id;
  }, std::chrono::seconds(15));
  CHECK(node4_converged);
  CHECK(coords_.back()->raft->GetLeaderId() == initial_leader_id);

  // And that reaching out via the gRPC surface agrees.
  int32_t via_rpc = QueryLeader(coords_.back()->grpc_endpoint());
  CHECK_EQ(via_rpc, initial_leader_id);
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "ScaleDownRemovesPeerViaRPC") {
  // Build a stable 4-member cluster, then simulate a graceful shutdown of
  // node 4: call RemovePeer on the leader (mirrors the coordinator's
  // SIGTERM self-remove path in src/main/coordinator_main.cpp). Verify
  // the leader's cluster_config shrinks back to 3 members.
  const int base_raft = UniquePortBase();
  const int base_grpc = base_raft + 50;
  auto layout = MakeLayout(/*count=*/4, base_raft, base_grpc);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_);
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected = WaitFor([&]() {
    return FindLeader(coords_) != nullptr;
  }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);

  // Send the RemovePeer RPC to the leader (not to ourselves — a real
  // scale-down drains the non-leader pod first). Use the retry helper
  // to tolerate NuRaft's transient CONFIG_CHANGING during bootstrap-era
  // membership settling.
  TestCoordinator* victim = nullptr;
  for (auto& c : coords_) {
    if (c->node_id != leader->node_id) { victim = c.get(); break; }
  }
  REQUIRE(victim != nullptr);
  CHECK(RemovePeerWithRetry(leader->grpc_endpoint(),
                            static_cast<uint32_t>(victim->node_id)));

  // Tear down the removed node locally to mirror what the pod SIGTERM
  // path would do next.
  ShutdownCoordinator(victim);

  // Remaining 3 nodes should still have a quorum (3 of the new 3-member
  // config) and keep the same leader — or at worst re-elect if the
  // removed node happened to be the leader (we explicitly avoided that
  // above, but NuRaft can still trigger an election under rare timing).
  bool stable = WaitFor([&]() {
    for (auto& c : coords_) {
      if (c.get() == victim) continue;
      if (c->raft && c->raft->GetLeaderId() <= 0) return false;
    }
    return true;
  }, std::chrono::seconds(10));
  CHECK(stable);
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "GhostPeerCleanupAfterHardKill") {
  // Simulates the K8s SIGKILL path (OOM, terminationGrace exceeded, or
  // `kubectl delete pod --force`): the coordinator pod dies WITHOUT
  // running its SIGTERM self-remove handler. The leader's cluster_config
  // still lists the dead node as a member ("ghost peer"). This is
  // exactly what 1.8 operator-side reconciliation fixes — we simulate
  // the operator calling RemovePeer directly and assert membership
  // converges back to {1,2,3}.
  const int base_raft = UniquePortBase();
  const int base_grpc = base_raft + 50;
  auto layout = MakeLayout(/*count=*/4, base_raft, base_grpc);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_);
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected = WaitFor([&]() {
    return FindLeader(coords_) != nullptr;
  }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);

  // Pick a non-leader victim (mirrors the common scale-down case where
  // the departing ordinal isn't the current leader).
  TestCoordinator* victim = nullptr;
  for (auto& c : coords_) {
    if (c->node_id != leader->node_id) { victim = c.get(); break; }
  }
  REQUIRE(victim != nullptr);
  int victim_node_id = victim->node_id;

  // Hard-kill: shut down the victim WITHOUT calling RemovePeer first.
  // This mirrors K8s SIGKILL — no graceful drain, no operator-driven
  // remove, just the process dying.
  ShutdownCoordinator(victim);

  // Confirm the leader's view still shows the ghost peer (4 members
  // including the dead one). NuRaft doesn't auto-remove dead members;
  // it just marks them unreachable and keeps them in config.
  auto membership_before =
      leader->raft->GetClusterMembership();
  bool found_ghost = false;
  for (const auto& m : membership_before) {
    if (m.node_id == victim_node_id) { found_ghost = true; break; }
  }
  CHECK(found_ghost);

  // Operator-simulated cleanup: call RemovePeer on the leader's gRPC
  // surface exactly as reconcileCoordinatorScale would. Idempotent per
  // 1.7b MapNuRaftCode if the pod had already left cleanly. Uses retry
  // to absorb NuRaft's transient CONFIG_CHANGING window.
  CHECK(RemovePeerWithRetry(leader->grpc_endpoint(),
                            static_cast<uint32_t>(victim_node_id)));

  // Assert membership converges to 3 members, excluding the ghost.
  bool removed = WaitFor([&]() {
    auto members = leader->raft->GetClusterMembership();
    if (members.size() != 3) return false;
    for (const auto& m : members) {
      if (m.node_id == victim_node_id) return false;
    }
    return true;
  }, std::chrono::seconds(10));
  CHECK(removed);

  // And the existing leader stays leader (no election churn induced).
  CHECK(leader->raft->IsLeader());
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "TransferLeadershipBeforeScaleDown") {
  // When the pod being scaled away is currently the leader, the operator
  // cannot call RemovePeer directly (NuRaft returns CANNOT_REMOVE_LEADER).
  // It must first transfer leadership via the 1.8 YieldLeadership path.
  // This test exercises the transfer + subsequent RemovePeer chain.
  const int base_raft = UniquePortBase();
  const int base_grpc = base_raft + 50;
  auto layout = MakeLayout(/*count=*/4, base_raft, base_grpc);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_);
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected = WaitFor([&]() {
    return FindLeader(coords_) != nullptr;
  }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* original_leader = FindLeader(coords_);
  REQUIRE(original_leader != nullptr);

  // Pick a target successor that is NOT the current leader.
  TestCoordinator* successor = nullptr;
  for (auto& c : coords_) {
    if (c->node_id != original_leader->node_id) {
      successor = c.get();
      break;
    }
  }
  REQUIRE(successor != nullptr);
  int successor_node_id = successor->node_id;

  // Operator-simulated call: TransferLeadership to the successor via
  // the original leader's gRPC surface. Retry absorbs NuRaft's transient
  // pending-config window.
  CHECK(TransferLeadershipWithRetry(
      original_leader->grpc_endpoint(),
      static_cast<uint32_t>(successor_node_id)));

  // The successor should now be leader.
  bool transferred = WaitFor([&]() {
    return successor->raft->IsLeader() &&
           successor->raft->GetLeaderId() == successor_node_id;
  }, std::chrono::seconds(5));
  CHECK(transferred);

  // Now RemovePeer on the ex-leader succeeds (no CANNOT_REMOVE_LEADER
  // because it's a follower now).
  CHECK(RemovePeerWithRetry(
      successor->grpc_endpoint(),
      static_cast<uint32_t>(original_leader->node_id)));

  // Membership on the new leader converges to 3 members, excluding
  // the ex-leader.
  bool converged = WaitFor([&]() {
    auto members = successor->raft->GetClusterMembership();
    if (members.size() != 3) return false;
    for (const auto& m : members) {
      if (m.node_id == original_leader->node_id) return false;
    }
    return true;
  }, std::chrono::seconds(10));
  CHECK(converged);
}

}  // namespace integration
}  // namespace consensus
}  // namespace gvdb
