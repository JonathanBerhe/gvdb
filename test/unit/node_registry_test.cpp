// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/coordinator/node_registry.h"
#include <doctest/doctest.h>
#include <chrono>
#include <set>
#include "internal.pb.h"

using namespace gvdb;
using namespace gvdb::cluster;
using namespace std::chrono_literals;

// ============================================================================
// Helper: build a proto::internal::NodeInfo with the given fields
// ============================================================================
static proto::internal::NodeInfo MakeNodeInfo(uint32_t id, proto::internal::NodeType type,
                                              const std::string& address = "localhost:50051") {
  proto::internal::NodeInfo info;
  info.set_node_id(id);
  info.set_node_type(type);
  info.set_grpc_address(address);
  info.set_status(proto::internal::NODE_STATUS_READY);
  return info;
}

// ============================================================================
// Fixture: creates a NodeRegistry with a short 50ms heartbeat timeout and a
// fake clock. Time moves only when a test calls Advance(), so heartbeat ages
// are exact and do not depend on how fast the machine runs the test.
// ============================================================================
class NodeRegistryTest {
 public:
  NodeRegistryTest() : registry_(50ms, [this] { return now_; }) {}

 protected:
  void Advance(std::chrono::milliseconds duration) { now_ += duration; }

  std::chrono::steady_clock::time_point now_ = std::chrono::steady_clock::time_point{} + 1h;
  NodeRegistry registry_;
};

// ============================================================================
// 1. RegisterNewNode via UpdateNode - adds node, GetNode retrieves it
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RegisterNewNode") {
  auto info = MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE, "host1:9000");
  registry_.UpdateNode(info);

  RegisteredNode node;
  bool found = registry_.GetNode(1, &node);
  REQUIRE(found);
  CHECK_EQ(node.info.node_id(), 1);
  CHECK_EQ(node.info.node_type(), proto::internal::NODE_TYPE_DATA_NODE);
  CHECK_EQ(node.info.grpc_address(), "host1:9000");
  CHECK_EQ(node.total_heartbeats, 1);
  CHECK_EQ(node.missed_heartbeats, 0);
}

// ============================================================================
// 2. UpdateExistingNode refreshes heartbeat - second call increments heartbeats
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "UpdateExistingNodeIncrementsHeartbeats") {
  auto info = MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE);
  registry_.UpdateNode(info);

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.total_heartbeats, 1);

  // Update again
  registry_.UpdateNode(info);

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.total_heartbeats, 2);

  // And a third time
  registry_.UpdateNode(info);

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.total_heartbeats, 3);

  // A heartbeat with resource figures overwrites the stored ones.
  auto with_resources = info;
  with_resources.set_memory_total_bytes(8000);
  with_resources.set_memory_used_bytes(3000);
  with_resources.set_disk_total_bytes(90000);
  with_resources.set_disk_used_bytes(40000);
  registry_.UpdateNode(with_resources);

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.total_heartbeats, 4);
  CHECK_EQ(node.info.memory_total_bytes(), 8000u);
  CHECK_EQ(node.info.memory_used_bytes(), 3000u);
  CHECK_EQ(node.info.disk_total_bytes(), 90000u);
  CHECK_EQ(node.info.disk_used_bytes(), 40000u);

  // A heartbeat without them (zero in proto3) keeps the stored figures.
  registry_.UpdateNode(info);

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.total_heartbeats, 5);
  CHECK_EQ(node.info.memory_total_bytes(), 8000u);
  CHECK_EQ(node.info.disk_used_bytes(), 40000u);
}

// ============================================================================
// 3. GetNode returns false for unknown ID
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetNodeReturnsFalseForUnknownId") {
  RegisteredNode node;
  CHECK_FALSE(registry_.GetNode(999, &node));
}

// ============================================================================
// 4. GetAllNodes returns all registered (register 3 nodes)
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetAllNodesReturnsAll") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_PROXY));

  auto all = registry_.GetAllNodes();
  CHECK_EQ(all.size(), 3);
}

// ============================================================================
// 5. GetHealthyNodes returns recently-updated nodes
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesReturnsRecent") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  // Both just registered, should be healthy
  auto healthy = registry_.GetHealthyNodes();
  CHECK_EQ(healthy.size(), 2);
}

// ============================================================================
// 6. GetHealthyNodes excludes stale nodes (time moved past timeout)
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesExcludesStale") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Move past the 50ms heartbeat timeout
  Advance(100ms);

  auto healthy = registry_.GetHealthyNodes();
  CHECK_EQ(healthy.size(), 0);
}

// ============================================================================
// 7. GetHealthyNodesByType combines type + health filtering
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesByTypeCombinesFilters") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  // Move past the timeout so both become stale
  Advance(100ms);

  // Refresh only the data node
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  auto healthy_data = registry_.GetHealthyNodesByType(proto::internal::NODE_TYPE_DATA_NODE);
  CHECK_EQ(healthy_data.size(), 1);
  CHECK_EQ(healthy_data[0].info.node_id(), 1);

  // Query node is stale - should be empty
  auto healthy_query = registry_.GetHealthyNodesByType(proto::internal::NODE_TYPE_QUERY_NODE);
  CHECK_EQ(healthy_query.size(), 0);
}

// ============================================================================
// 8. RemoveNode makes GetNode return false
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RemoveNodeMakesItGone") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  registry_.RemoveNode(1);

  CHECK_FALSE(registry_.GetNode(1, &node));

  auto all = registry_.GetAllNodes();
  CHECK_EQ(all.size(), 0);
}

// ============================================================================
// 9. RemoveNode on unknown ID is no-op
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RemoveUnknownNodeIsNoOp") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Remove a non-existent node - should not crash or affect existing nodes
  registry_.RemoveNode(999);

  auto all = registry_.GetAllNodes();
  CHECK_EQ(all.size(), 1);

  RegisteredNode node;
  CHECK(registry_.GetNode(1, &node));
}

// ============================================================================
// 10. GetClusterStats counts by type
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetClusterStatsCountsByType") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_COORDINATOR));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(4, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(5, proto::internal::NODE_TYPE_PROXY));

  auto stats = registry_.GetClusterStats();
  CHECK_EQ(stats.total_nodes, 5);
  CHECK_EQ(stats.coordinators, 1);
  CHECK_EQ(stats.data_nodes, 2);
  CHECK_EQ(stats.query_nodes, 1);
  CHECK_EQ(stats.proxies, 1);
}

// ============================================================================
// 11. GetClusterStats healthy vs failed
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetClusterStatsHealthyVsFailed") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_PROXY));

  // Move past the timeout so all become stale
  Advance(100ms);

  // Refresh only node 1
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  auto stats = registry_.GetClusterStats();
  CHECK_EQ(stats.total_nodes, 3);
  CHECK_EQ(stats.healthy_nodes, 1);
  CHECK_EQ(stats.failed_nodes, 2);
}

// ============================================================================
// 12. DetectFailedNodes returns stale IDs
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "DetectFailedNodesReturnsStaleIds") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_PROXY));

  // Move past the timeout
  Advance(100ms);

  // Refresh only node 2
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  auto failed_ids = registry_.DetectFailedNodes();
  CHECK_EQ(failed_ids.size(), 2);

  // Should contain nodes 1 and 3 but not 2
  bool has_node1 = false;
  bool has_node3 = false;
  for (auto id : failed_ids) {
    if (id == 1) has_node1 = true;
    if (id == 3) has_node3 = true;
    CHECK_NE(id, 2);
  }
  CHECK(has_node1);
  CHECK(has_node3);
}

// ============================================================================
// 13. DetectFailedNodes increments missed_heartbeats
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "DetectFailedNodesIncrementsMissedHeartbeats") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Move past the timeout
  Advance(100ms);

  // First detection
  auto failed1 = registry_.DetectFailedNodes();
  CHECK_EQ(failed1.size(), 1);

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.missed_heartbeats, 1);

  // Second detection (still stale)
  auto failed2 = registry_.DetectFailedNodes();
  CHECK_EQ(failed2.size(), 1);

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.missed_heartbeats, 2);
}

// ============================================================================
// 14. RegisteredNode IsHealthy boundary test
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RegisteredNodeIsHealthyBoundary") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  // Just registered - should be healthy with a reasonable timeout
  CHECK(node.IsHealthy(50ms, now_));

  // Should not be healthy with zero timeout (always expired)
  CHECK_FALSE(node.IsHealthy(0ms, now_));

  // Let 30ms pass and check with tight timeout
  Advance(30ms);
  REQUIRE(registry_.GetNode(1, &node));

  // 30ms elapsed, 50ms timeout - still healthy
  CHECK(node.IsHealthy(50ms, now_));

  // 30ms elapsed, 20ms timeout - no longer healthy
  CHECK_FALSE(node.IsHealthy(20ms, now_));
}

// ============================================================================
// 15. RegisteredNode TimeSinceLastHeartbeat
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RegisteredNodeTimeSinceLastHeartbeat") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  // Just registered: no time has passed
  auto elapsed = node.TimeSinceLastHeartbeat(now_);
  CHECK_EQ(elapsed.count(), 0);

  // Let 50ms pass and check again
  Advance(50ms);
  REQUIRE(registry_.GetNode(1, &node));

  elapsed = node.TimeSinceLastHeartbeat(now_);
  CHECK_EQ(elapsed.count(), 50);
}

// ============================================================================
// Drain signaling: NODE_STATUS_DRAINING
// ============================================================================

// Helper that mirrors MakeNodeInfo but sets a DRAINING status.
static proto::internal::NodeInfo MakeDrainingNodeInfo(
    uint32_t id, proto::internal::NodeType type, const std::string& address = "localhost:50051") {
  auto info = MakeNodeInfo(id, type, address);
  info.set_status(proto::internal::NODE_STATUS_DRAINING);
  return info;
}

// 16. A node transitions to DRAINING when it sends a heartbeat with that
//     status. The stored info.status() reflects the latest heartbeat.
TEST_CASE_FIXTURE(NodeRegistryTest, "UpdateNodeTransitionsToDraining") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.info.status(), proto::internal::NODE_STATUS_READY);
  CHECK_FALSE(node.IsDraining());

  // Second heartbeat with DRAINING status — node should be marked as draining.
  registry_.UpdateNode(MakeDrainingNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  REQUIRE(registry_.GetNode(1, &node));
  CHECK_EQ(node.info.status(), proto::internal::NODE_STATUS_DRAINING);
  CHECK(node.IsDraining());
}

// 17. IsRoutable requires both healthy heartbeat AND non-draining status.
TEST_CASE_FIXTURE(NodeRegistryTest, "IsRoutableRequiresHealthyAndNotDraining") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  CHECK(node.IsHealthy(50ms, now_));
  CHECK(node.IsRoutable(50ms, now_));

  // Mark as draining via another heartbeat.
  registry_.UpdateNode(MakeDrainingNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  REQUIRE(registry_.GetNode(1, &node));

  // Still healthy (recent heartbeat) but no longer routable.
  CHECK(node.IsHealthy(50ms, now_));
  CHECK_FALSE(node.IsRoutable(50ms, now_));
}

// 18. GetRoutableNodesByType combines type filter with drain exclusion.
TEST_CASE_FIXTURE(NodeRegistryTest, "GetRoutableNodesByTypeCombinesFilters") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeDrainingNodeInfo(2, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeDrainingNodeInfo(4, proto::internal::NODE_TYPE_QUERY_NODE));

  auto routable_data = registry_.GetRoutableNodesByType(proto::internal::NODE_TYPE_DATA_NODE);
  REQUIRE_EQ(routable_data.size(), 1);
  CHECK_EQ(routable_data[0].info.node_id(), 1u);

  auto routable_query = registry_.GetRoutableNodesByType(proto::internal::NODE_TYPE_QUERY_NODE);
  REQUIRE_EQ(routable_query.size(), 1);
  CHECK_EQ(routable_query[0].info.node_id(), 3u);

  // Healthy-by-type still counts draining nodes (for observability).
  auto healthy_data = registry_.GetHealthyNodesByType(proto::internal::NODE_TYPE_DATA_NODE);
  CHECK_EQ(healthy_data.size(), 2);
}

// 19. A draining node whose heartbeat eventually times out is no longer
//     routable AND no longer healthy — belt and braces.
TEST_CASE_FIXTURE(NodeRegistryTest, "DrainingNodeBecomesFailedAfterTimeout") {
  registry_.UpdateNode(MakeDrainingNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  CHECK_FALSE(registry_.IsNodeRoutable(1));

  // Let the heartbeat age past the 50ms timeout.
  Advance(100ms);

  CHECK_FALSE(registry_.IsNodeRoutable(1));

  auto healthy_after = registry_.GetHealthyNodes();
  CHECK_EQ(healthy_after.size(), 0);

  CHECK_EQ(registry_.GetClusterStats().failed_nodes, 1);
}

// 20. GetDrainingNodes returns only nodes that are both DRAINING AND still
//     alive per heartbeat timeout. Stale draining nodes belong to the
//     failure path, not the graceful drain path.
TEST_CASE_FIXTURE(NodeRegistryTest, "GetDrainingNodesExcludesStaleDraining") {
  // Node 1: ready (non-draining). Should never appear in GetDrainingNodes.
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  // Node 2: actively draining + fresh heartbeat. Should appear.
  registry_.UpdateNode(MakeDrainingNodeInfo(2, proto::internal::NODE_TYPE_DATA_NODE));
  // Node 3: draining but heartbeat will age past timeout below. Should
  // be excluded (stale draining → failure-path territory).
  registry_.UpdateNode(MakeDrainingNodeInfo(3, proto::internal::NODE_TYPE_DATA_NODE));

  // Fresh — all three present.
  auto fresh = registry_.GetDrainingNodes();
  REQUIRE_EQ(fresh.size(), 2);
  std::set<uint32_t> fresh_ids;
  for (const auto& n : fresh) fresh_ids.insert(n.info.node_id());
  CHECK(fresh_ids.count(2) == 1);
  CHECK(fresh_ids.count(3) == 1);

  // Let only node 3 age out: refresh 1 and 2 after the timeout; leave 3 stale.
  Advance(100ms);
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeDrainingNodeInfo(2, proto::internal::NODE_TYPE_DATA_NODE));

  auto after = registry_.GetDrainingNodes();
  REQUIRE_EQ(after.size(), 1);
  CHECK_EQ(after[0].info.node_id(), 2u);

  // Stale draining node 3 is now in the failed set.
  auto failed = registry_.DetectFailedNodes();
  REQUIRE_EQ(failed.size(), 1);
  CHECK_EQ(failed[0], 3u);
}
