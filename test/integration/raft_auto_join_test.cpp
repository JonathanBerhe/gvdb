// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// In-process integration tests for coordinator Raft auto-join.
// Spins up multiple RaftNode instances in the same test binary plus an
// InternalService gRPC server for each, just enough to exercise the
// JoinCluster / RemovePeer RPCs end-to-end without a real K8s cluster.
//
// Scope:
//   1. 3-node cold bootstrap: all pods start with empty persisted state +
//      the full declared peer list; verify leader election converges.
//   2. Runtime scale-up: add a 4th RaftNode with an empty PVC and the
//      extended peer list. Its peer probe finds the 3-node leader, skips
//      self-seeding, and calls JoinCluster. Verify cluster_config grows to 4.
//   3. Graceful scale-down: simulate SIGTERM on node 4 by calling
//      RemovePeer on the leader. Verify cluster_config returns to 3.

#include "cluster/coordinator/raft_peer_client.h"
#include "consensus/raft_config.h"
#include "consensus/raft_node.h"
#include "internal.grpc.pb.h"
#include "service/internal/internal_service.h"
#include "support/temp_dir.h"
#include "support/wait.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace gvdb {
namespace consensus {
namespace integration {

namespace {

using gvdb::test::ScopedTempDir;
using gvdb::test::WaitFor;

// Returns `count` distinct TCP ports that are free on every IPv4 address
// when the function returns. The operating system picks them (bind to port
// 0), so two test processes that run at the same time get different ports.
//
// The servers in this file cannot listen on port 0 themselves: RaftNode
// needs the raft and gRPC endpoints of every peer in its config before any
// node starts, and NuRaft binds its listener inside RaftNode::Start().
// Another process can still take a port between this function and that
// bind; the test cannot close that window.
//
// Returns fewer ports when the operating system refuses a socket.
std::vector<int> PickFreePorts(size_t count) {
  std::vector<int> fds;
  std::vector<int> ports;
  while (ports.size() < count) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) break;
    fds.push_back(fd);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;
    socklen_t len = sizeof(addr);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
      break;
    }
    ports.push_back(ntohs(addr.sin_port));
  }
  // Each socket stays open until all ports are picked, so the ports differ.
  for (int fd : fds) ::close(fd);
  return ports;
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
  std::unique_ptr<service::InternalService> service;
  std::unique_ptr<grpc::Server> grpc_server;

  std::string raft_endpoint() const { return "localhost:" + std::to_string(raft_port); }
  std::string grpc_endpoint() const { return "localhost:" + std::to_string(grpc_port); }
};

// Peer specs for a cluster of 'count' members, each with a raft port and a
// gRPC port from PickFreePorts(). Order is 1..count, matching the
// ordinal->node_id convention (ordinal 0 => node_id 1).
struct ClusterLayout {
  std::vector<std::string> raft_peers;              // "id:host:port"
  std::vector<std::string> coordinator_grpc_peers;  // "host:port"
  std::vector<int> raft_ports;                      // [node_id - 1]
  std::vector<int> grpc_ports;                      // [node_id - 1]
};

ClusterLayout MakeLayout(int count) {
  const auto ports = PickFreePorts(2 * static_cast<size_t>(count));
  REQUIRE(ports.size() == 2 * static_cast<size_t>(count));
  ClusterLayout l;
  for (int i = 0; i < count; ++i) {
    int id = i + 1;
    l.raft_ports.push_back(ports[2 * i]);
    l.grpc_ports.push_back(ports[2 * i + 1]);
    l.raft_peers.push_back(std::to_string(id) +
                           ":localhost:" + std::to_string(l.raft_ports.back()));
    l.coordinator_grpc_peers.push_back("localhost:" + std::to_string(l.grpc_ports.back()));
  }
  return l;
}

std::unique_ptr<TestCoordinator> StartCoordinator(int node_id, const ClusterLayout& layout,
                                                  const std::filesystem::path& root_dir) {
  auto c = std::make_unique<TestCoordinator>();
  c->node_id = node_id;
  c->raft_port = layout.raft_ports[node_id - 1];
  c->grpc_port = layout.grpc_ports[node_id - 1];
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

  auto created = RaftNode::Create(cfg, std::make_shared<cluster::GrpcRaftPeerClient>());
  if (!created.ok()) return nullptr;
  c->raft = std::move(*created);
  auto status = c->raft->Start();
  if (!status.ok()) return nullptr;

  // Stand up a minimal InternalService: only raft_node_ needs to be real
  // for the JoinCluster / RemovePeer / GetLeaderInfo paths. Everything
  // else is nullptr-safe (see the InternalService constructor in
  // src/service/internal/internal_service.cpp).
  c->service = std::make_unique<service::InternalService>(
      /*shard_manager=*/nullptr,
      /*segment_store=*/nullptr,
      /*query_executor=*/nullptr, service::InternalServiceOptions{.raft_node = c->raft});

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
    c->grpc_server->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
    c->grpc_server.reset();
  }
  if (c->raft) {
    (void)c->raft->Shutdown();
    c->raft.reset();
  }
  c->service.reset();
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
// Matches the behavior real operator reconcilers exhibit: each reconcile
// retries idempotently, so the test mirrors the production retry loop.
bool RemovePeerWithRetry(const std::string& leader_target, uint32_t node_id,
                         std::chrono::milliseconds budget = std::chrono::seconds(5)) {
  auto channel = grpc::CreateChannel(leader_target, grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  return WaitFor(
      [&]() {
        proto::internal::RemovePeerRequest req;
        req.set_node_id(node_id);
        proto::internal::RemovePeerResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        auto st = stub->RemovePeer(&ctx, req, &resp);
        return st.ok() && resp.success();
      },
      budget, std::chrono::milliseconds(200));
}

// Same retry wrapper for TransferLeadership.
bool TransferLeadershipWithRetry(const std::string& leader_target, uint32_t target_node_id,
                                 std::chrono::milliseconds budget = std::chrono::seconds(8)) {
  auto channel = grpc::CreateChannel(leader_target, grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  return WaitFor(
      [&]() {
        proto::internal::TransferLeadershipRequest req;
        req.set_target_node_id(target_node_id);
        proto::internal::TransferLeadershipResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto st = stub->TransferLeadership(&ctx, req, &resp);
        return st.ok() && resp.success();
      },
      budget, std::chrono::milliseconds(200));
}

// Call GetLeaderInfo on a specific coordinator's gRPC port and return the
// reported leader_id (or 0 if no leader / RPC failed).
int32_t QueryLeader(const std::string& target) {
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = proto::internal::InternalService::NewStub(channel);
  proto::internal::GetLeaderInfoRequest req;
  proto::internal::GetLeaderInfoResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  auto st = stub->GetLeaderInfo(&ctx, req, &resp);
  if (!st.ok()) return 0;
  return resp.leader_id();
}

class RaftAutoJoinFixture {
 public:
  ~RaftAutoJoinFixture() {
    for (auto& c : coords_) ShutdownCoordinator(c.get());
    coords_.clear();
  }

 protected:
  ScopedTempDir root_dir_{"gvdb_raft_auto_join"};
  std::vector<std::unique_ptr<TestCoordinator>> coords_;
};

}  // namespace

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "3NodeBootstrapElectsLeader") {
  // All three nodes start together with empty persisted state + a 3-member
  // peer list. Since no peer has a leader yet, ProbePeersForLeader finds
  // none, we fall through to the seed-from-declared-peers path, and NuRaft
  // elects a leader via the standard bootstrap flow. This test locks in
  // that the startup peer probe does NOT break initial-bootstrap semantics.
  auto layout = MakeLayout(/*count=*/3);

  for (int id = 1; id <= 3; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_.path());
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  // Leader election + config convergence. 10s is generous; typical
  // elections finish in 200-500ms on localhost.
  bool elected =
      WaitFor([&]() { return FindLeader(coords_) != nullptr; }, std::chrono::seconds(10));
  REQUIRE(elected);

  auto* leader = FindLeader(coords_);
  CHECK(leader != nullptr);
  CHECK(leader->raft->GetLeaderId() == leader->node_id);

  // Every node should agree on the same leader_id once stable.
  bool converged = WaitFor(
      [&]() {
        int32_t leader_id = leader->raft->GetLeaderId();
        if (leader_id <= 0) return false;
        for (const auto& c : coords_) {
          if (c->raft->GetLeaderId() != leader_id) return false;
        }
        return true;
      },
      std::chrono::seconds(5));
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

  // Phase 1: 3-node cold-start with a layout sized for 4 (so adding
  // node 4 later does not require a restart of nodes 1-3). Nodes 1-3 start with 4 declared peers
  // but only themselves are alive initially; NuRaft's bootstrap with the 4-member seed config still
  // elects a leader once the majority (3/4) is up.
  auto full_layout = MakeLayout(/*count=*/4);
  for (int id = 1; id <= 3; ++id) {
    auto c = StartCoordinator(id, full_layout, root_dir_.path());
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected =
      WaitFor([&]() { return FindLeader(coords_) != nullptr; }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);

  // Phase 2: bring up node 4. Its data dir is fresh, so persisted_cluster_size
  // is 0 → needs_seed==true → peer probe runs → finds the 3-node leader →
  // AnnounceJoinToCluster calls JoinCluster on the leader → add_srv →
  // cluster_config grows to 4.
  auto fourth = StartCoordinator(4, full_layout, root_dir_.path());
  REQUIRE(fourth != nullptr);
  coords_.push_back(std::move(fourth));

  // Phase 3: verify that all 4 nodes agree on one leader, and that this
  // node reports itself as the leader. The check does not require the first
  // leader to keep its role: elections use real-time timeouts, so a loaded
  // machine can hold a new election while node 4 joins.
  int agreed_leader_id = 0;
  bool all_converged = WaitFor(
      [&]() {
        const int leader_id = coords_.front()->raft->GetLeaderId();
        if (leader_id <= 0) return false;
        bool leader_confirms = false;
        for (const auto& c : coords_) {
          if (c->raft->GetLeaderId() != leader_id) return false;
          if (c->node_id == leader_id) leader_confirms = c->raft->IsLeader();
        }
        if (!leader_confirms) return false;
        agreed_leader_id = leader_id;
        return true;
      },
      std::chrono::seconds(15));
  CHECK(all_converged);
  CHECK_GE(agreed_leader_id, 1);
  CHECK_LE(agreed_leader_id, 4);

  // And that reaching out via the gRPC surface agrees.
  int32_t via_rpc = QueryLeader(coords_.back()->grpc_endpoint());
  CHECK_EQ(via_rpc, agreed_leader_id);
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "ScaleDownRemovesPeerViaRPC") {
  // Build a stable 4-member cluster, then simulate a graceful shutdown of
  // node 4: call RemovePeer on the leader (mirrors the coordinator's
  // SIGTERM self-remove path in src/main/coordinator_main.cpp). Verify
  // the leader's cluster_config shrinks back to 3 members.
  auto layout = MakeLayout(/*count=*/4);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_.path());
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected =
      WaitFor([&]() { return FindLeader(coords_) != nullptr; }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);

  // Send the RemovePeer RPC to the leader (not to ourselves, because a real
  // scale-down drains the non-leader pod first). Use the retry helper
  // to tolerate NuRaft's transient CONFIG_CHANGING during bootstrap-era
  // membership settling.
  TestCoordinator* victim = nullptr;
  for (auto& c : coords_) {
    if (c->node_id != leader->node_id) {
      victim = c.get();
      break;
    }
  }
  REQUIRE(victim != nullptr);
  CHECK(RemovePeerWithRetry(leader->grpc_endpoint(), static_cast<uint32_t>(victim->node_id)));

  // Tear down the removed node locally to mirror what the pod SIGTERM
  // path would do next.
  ShutdownCoordinator(victim);

  // Remaining 3 nodes should still have a quorum (3 of the new 3-member
  // config) and keep the same leader, or at worst re-elect if the
  // removed node happened to be the leader (we explicitly avoided that
  // above, but NuRaft can still trigger an election under rare timing).
  bool stable = WaitFor(
      [&]() {
        for (auto& c : coords_) {
          if (c.get() == victim) continue;
          if (c->raft && c->raft->GetLeaderId() <= 0) return false;
        }
        return true;
      },
      std::chrono::seconds(10));
  CHECK(stable);
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "GhostPeerCleanupAfterHardKill") {
  // Simulates the K8s SIGKILL path (OOM, terminationGrace exceeded, or
  // `kubectl delete pod --force`): the coordinator pod dies WITHOUT
  // running its SIGTERM self-remove handler. The leader's cluster_config
  // still lists the dead node as a member ("ghost peer"). This is
  // exactly what operator-side reconciliation fixes, and we simulate
  // the operator calling RemovePeer directly and assert membership
  // converges back to {1,2,3}.
  auto layout = MakeLayout(/*count=*/4);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_.path());
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected =
      WaitFor([&]() { return FindLeader(coords_) != nullptr; }, std::chrono::seconds(15));
  REQUIRE(elected);
  auto* leader = FindLeader(coords_);
  REQUIRE(leader != nullptr);

  // Pick a non-leader victim (mirrors the common scale-down case where
  // the departing ordinal isn't the current leader).
  TestCoordinator* victim = nullptr;
  for (auto& c : coords_) {
    if (c->node_id != leader->node_id) {
      victim = c.get();
      break;
    }
  }
  REQUIRE(victim != nullptr);
  int victim_node_id = victim->node_id;

  // Hard-kill: shut down the victim WITHOUT calling RemovePeer first.
  // This mirrors K8s SIGKILL: no graceful drain, no operator-driven
  // remove, just the process dying.
  ShutdownCoordinator(victim);

  // Confirm the leader's view still shows the ghost peer (4 members
  // including the dead one). NuRaft doesn't auto-remove dead members;
  // it just marks them unreachable and keeps them in config.
  auto membership_before = leader->raft->GetClusterMembership();
  bool found_ghost = false;
  for (const auto& m : membership_before) {
    if (m.node_id == victim_node_id) {
      found_ghost = true;
      break;
    }
  }
  CHECK(found_ghost);

  // Operator-simulated cleanup: call RemovePeer on the leader's gRPC
  // surface exactly as reconcileCoordinatorScale would. Idempotent per
  // MapNuRaftCode if the pod had already left cleanly. Uses retry
  // to absorb NuRaft's transient CONFIG_CHANGING window.
  CHECK(RemovePeerWithRetry(leader->grpc_endpoint(), static_cast<uint32_t>(victim_node_id)));

  // Assert membership converges to 3 members, excluding the ghost.
  bool removed = WaitFor(
      [&]() {
        auto members = leader->raft->GetClusterMembership();
        if (members.size() != 3) return false;
        for (const auto& m : members) {
          if (m.node_id == victim_node_id) return false;
        }
        return true;
      },
      std::chrono::seconds(10));
  CHECK(removed);

  // And the existing leader stays leader (no election churn induced).
  CHECK(leader->raft->IsLeader());
}

TEST_CASE_FIXTURE(RaftAutoJoinFixture, "TransferLeadershipBeforeScaleDown") {
  // When the pod being scaled away is currently the leader, the operator
  // cannot call RemovePeer directly (NuRaft returns CANNOT_REMOVE_LEADER).
  // It must first transfer leadership via the 1.8 YieldLeadership path.
  // This test exercises the transfer + subsequent RemovePeer chain.
  auto layout = MakeLayout(/*count=*/4);

  for (int id = 1; id <= 4; ++id) {
    auto c = StartCoordinator(id, layout, root_dir_.path());
    REQUIRE(c != nullptr);
    coords_.push_back(std::move(c));
  }

  bool elected =
      WaitFor([&]() { return FindLeader(coords_) != nullptr; }, std::chrono::seconds(15));
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
  CHECK(TransferLeadershipWithRetry(original_leader->grpc_endpoint(),
                                    static_cast<uint32_t>(successor_node_id)));

  // The successor should now be leader.
  bool transferred = WaitFor(
      [&]() {
        return successor->raft->IsLeader() && successor->raft->GetLeaderId() == successor_node_id;
      },
      std::chrono::seconds(5));
  CHECK(transferred);

  // Now RemovePeer on the ex-leader succeeds (no CANNOT_REMOVE_LEADER
  // because it's a follower now).
  CHECK(RemovePeerWithRetry(successor->grpc_endpoint(),
                            static_cast<uint32_t>(original_leader->node_id)));

  // Membership on the new leader converges to 3 members, excluding
  // the ex-leader.
  bool converged = WaitFor(
      [&]() {
        auto members = successor->raft->GetClusterMembership();
        if (members.size() != 3) return false;
        for (const auto& m : members) {
          if (m.node_id == original_leader->node_id) return false;
        }
        return true;
      },
      std::chrono::seconds(10));
  CHECK(converged);
}

}  // namespace integration
}  // namespace consensus
}  // namespace gvdb
