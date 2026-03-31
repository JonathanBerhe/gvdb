// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>
#include "cluster/node_registry.h"
#include "internal.pb.h"
#include <chrono>
#include <thread>

using namespace gvdb;
using namespace gvdb::cluster;
using namespace std::chrono_literals;

// ============================================================================
// Helper: build a proto::internal::NodeInfo with the given fields
// ============================================================================
static proto::internal::NodeInfo MakeNodeInfo(
    uint32_t id,
    proto::internal::NodeType type,
    const std::string& address = "localhost:50051") {
  proto::internal::NodeInfo info;
  info.set_node_id(id);
  info.set_node_type(type);
  info.set_grpc_address(address);
  info.set_status(proto::internal::NODE_STATUS_READY);
  return info;
}

// ============================================================================
// Fixture: creates a NodeRegistry with a short 50ms heartbeat timeout
// ============================================================================
class NodeRegistryTest {
 public:
  NodeRegistryTest()
      : registry_(50ms) {}

 protected:
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
// 5. GetNodesByType filters correctly (2 data + 1 query)
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetNodesByTypeFilters") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_QUERY_NODE));

  auto data_nodes = registry_.GetNodesByType(proto::internal::NODE_TYPE_DATA_NODE);
  CHECK_EQ(data_nodes.size(), 2);

  auto query_nodes = registry_.GetNodesByType(proto::internal::NODE_TYPE_QUERY_NODE);
  CHECK_EQ(query_nodes.size(), 1);
  CHECK_EQ(query_nodes[0].info.node_id(), 3);

  auto proxies = registry_.GetNodesByType(proto::internal::NODE_TYPE_PROXY);
  CHECK_EQ(proxies.size(), 0);
}

// ============================================================================
// 6. GetHealthyNodes returns recently-updated nodes
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesReturnsRecent") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  // Both just registered, should be healthy
  auto healthy = registry_.GetHealthyNodes();
  CHECK_EQ(healthy.size(), 2);
}

// ============================================================================
// 7. GetHealthyNodes excludes stale nodes (sleep past timeout)
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesExcludesStale") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Wait past the 50ms heartbeat timeout
  std::this_thread::sleep_for(100ms);

  auto healthy = registry_.GetHealthyNodes();
  CHECK_EQ(healthy.size(), 0);
}

// ============================================================================
// 8. GetHealthyNodesByType combines type + health filtering
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetHealthyNodesByTypeCombinesFilters") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  // Wait past timeout so both become stale
  std::this_thread::sleep_for(100ms);

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
// 9. GetFailedNodes returns stale nodes
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetFailedNodesReturnsStale") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));

  // Wait past timeout
  std::this_thread::sleep_for(100ms);

  // Refresh node 1 only
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  auto failed = registry_.GetFailedNodes();
  CHECK_EQ(failed.size(), 1);
  CHECK_EQ(failed[0].info.node_id(), 2);
}

// ============================================================================
// 10. RemoveNode makes GetNode return false
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
// 11. RemoveNode on unknown ID is no-op
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
// 12. GetClusterStats counts by type
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
// 13. GetClusterStats healthy vs failed
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "GetClusterStatsHealthyVsFailed") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_PROXY));

  // Wait past timeout so all become stale
  std::this_thread::sleep_for(100ms);

  // Refresh only node 1
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  auto stats = registry_.GetClusterStats();
  CHECK_EQ(stats.total_nodes, 3);
  CHECK_EQ(stats.healthy_nodes, 1);
  CHECK_EQ(stats.failed_nodes, 2);
}

// ============================================================================
// 14. DetectFailedNodes returns stale IDs
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "DetectFailedNodesReturnsStaleIds") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));
  registry_.UpdateNode(MakeNodeInfo(2, proto::internal::NODE_TYPE_QUERY_NODE));
  registry_.UpdateNode(MakeNodeInfo(3, proto::internal::NODE_TYPE_PROXY));

  // Wait past timeout
  std::this_thread::sleep_for(100ms);

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
// 15. DetectFailedNodes increments missed_heartbeats
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "DetectFailedNodesIncrementsMissedHeartbeats") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Wait past timeout
  std::this_thread::sleep_for(100ms);

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
// 16. SetHeartbeatTimeout changes detection window
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "SetHeartbeatTimeoutChangesWindow") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  // Wait 60ms - past default 50ms timeout
  std::this_thread::sleep_for(60ms);

  // Node should be unhealthy with the 50ms timeout
  auto healthy_before = registry_.GetHealthyNodes();
  CHECK_EQ(healthy_before.size(), 0);

  // Increase timeout to 200ms - node should become healthy again
  registry_.SetHeartbeatTimeout(200ms);

  auto healthy_after = registry_.GetHealthyNodes();
  CHECK_EQ(healthy_after.size(), 1);
}

// ============================================================================
// 17. RegisteredNode IsHealthy boundary test
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RegisteredNodeIsHealthyBoundary") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  // Just registered - should be healthy with a reasonable timeout
  CHECK(node.IsHealthy(50ms));

  // Should not be healthy with zero timeout (always expired)
  CHECK_FALSE(node.IsHealthy(0ms));

  // Wait a bit and check with tight timeout
  std::this_thread::sleep_for(30ms);
  REQUIRE(registry_.GetNode(1, &node));

  // 30ms elapsed, 50ms timeout - still healthy
  CHECK(node.IsHealthy(50ms));

  // 30ms elapsed, 20ms timeout - no longer healthy
  CHECK_FALSE(node.IsHealthy(20ms));
}

// ============================================================================
// 18. RegisteredNode TimeSinceLastHeartbeat
// ============================================================================
TEST_CASE_FIXTURE(NodeRegistryTest, "RegisteredNodeTimeSinceLastHeartbeat") {
  registry_.UpdateNode(MakeNodeInfo(1, proto::internal::NODE_TYPE_DATA_NODE));

  RegisteredNode node;
  REQUIRE(registry_.GetNode(1, &node));

  // Just registered, should be very small
  auto elapsed = node.TimeSinceLastHeartbeat();
  CHECK(elapsed < 20ms);

  // Wait and check again
  std::this_thread::sleep_for(50ms);
  REQUIRE(registry_.GetNode(1, &node));

  elapsed = node.TimeSinceLastHeartbeat();
  CHECK(elapsed >= 50ms);
  CHECK(elapsed < 200ms);
}
