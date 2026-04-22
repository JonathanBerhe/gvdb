/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"
	"time"

	"gvdb/operator/internal/gvdbclient"
)

// healthyCluster builds a ClusterHealth claiming full shard health and the
// given data-node ordinals as NODE_STATUS_READY + NODE_TYPE_DATA_NODE.
// Gate A passes trivially. Pass zero shards to exercise the bootstrap
// short-circuit.
func healthyCluster(totalShards uint32, dataNodeOrdinals ...int32) gvdbclient.ClusterHealth {
	nodes := make([]gvdbclient.NodeHealth, 0, len(dataNodeOrdinals))
	for _, o := range dataNodeOrdinals {
		nodes = append(nodes, gvdbclient.NodeHealth{
			NodeID:     uint32(DataNodeBaseNodeID + o),
			Ready:      true,
			IsDataNode: true,
		})
	}
	return gvdbclient.ClusterHealth{
		ClusterStatus: "healthy",
		TotalShards:   totalShards,
		HealthyShards: totalShards,
		Nodes:         nodes,
	}
}

// shardPrimaryReplicas builds a ShardAssignment matching the real proto
// semantics (internal_service.cpp:108-111): primary_node_id is populated
// from shard_info.primary_node and node_ids[] from shard_info.replica_nodes
// — they are DISJOINT sets. The primary is the node at ordinal
// primaryOrdinal; replicas are at the given replicaOrdinals (exclusive of
// the primary).
func shardPrimaryReplicas(shardID uint32, primaryOrdinal int32, replicaOrdinals ...int32) gvdbclient.ShardAssignment {
	ids := make([]uint32, len(replicaOrdinals))
	for i, o := range replicaOrdinals {
		ids[i] = uint32(DataNodeBaseNodeID + o)
	}
	return gvdbclient.ShardAssignment{
		ShardID:       shardID,
		PrimaryNodeID: uint32(DataNodeBaseNodeID + primaryOrdinal),
		NodeIDs:       ids,
	}
}

func TestDataNodeDesiredPartition_Stable(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r1", 3)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if !step.Done {
		t.Fatalf("stable STS: expected Done=true")
	}
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("stable STS: expected partition=replicas-1=2, got %v", step.Partition)
	}
	if step.Reason != ReasonStable {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_SingleReplicaPassthrough(t *testing.T) {
	// Single-replica data-node (e.g. dev cluster). No safety to preserve;
	// render leaves partition unset and we don't manage it.
	sts := mkSTS(1, nil, "r1", "r2", 0)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if !step.Done || step.Partition != nil {
		t.Fatalf("single-replica: expected Done=true partition=nil, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_FirstDetectionPinsToHighestOrdinal(t *testing.T) {
	sts := mkSTS(3, nil, "r1", "r2", 0)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Done || step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected partition=2 Done=false, got %+v", step)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_ClampOversizedPartition(t *testing.T) {
	sts := mkSTS(3, ptrI32(5), "r1", "r2", 3)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected clamp to replicas-1=2, got %v", step.Partition)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_ScaleDownDuringRolloutClamps(t *testing.T) {
	sts := mkSTS(2, ptrI32(2), "r1", "r2", 1)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("expected clamp to replicas-1=1, got %v", step.Partition)
	}
}

func TestDataNodeDesiredPartition_ScaleUpDuringRollout(t *testing.T) {
	// Mid-rollout scale-up: partition was 1 on the old replicas=3; now
	// replicas=4, updatedReplicas=2 (pods 2 and 3 just joined fresh).
	// expectedUpdated = 4 - 1 = 3, got 2 → hold partition.
	sts := mkSTS(4, ptrI32(1), "r1", "r2", 2)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 1 with WaitingForPod, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_WaitForPodCatchUp(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 0)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 2 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 2 with WaitingForPod, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_FinalPodWaitsForCompletion(t *testing.T) {
	// Partition is already 0; the last pod is still updating.
	sts := mkSTS(3, ptrI32(0), "r1", "r2", 2)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 0 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 0 with WaitingForPod, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_WaitingForCoordinatorRollout(t *testing.T) {
	// Mechanical state is ready to advance, but coordinator rollout is still
	// in progress — never two rollouts at once.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredDataNodePartition(sts, healthyCluster(0), nil, 0, time.Now(), false)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForCoordinatorRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_WaitingForClusterHealth(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(4, 0, 1, 2)
	health.HealthyShards = 3 // one shard degraded
	step := desiredDataNodePartition(sts, health, nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForClusterHealth {
		t.Fatalf("reason %q", step.Reason)
	}
	if step.Message == "" {
		t.Fatalf("expected message with healthy_shards detail")
	}
}

func TestDataNodeDesiredPartition_NoShardsAdvances(t *testing.T) {
	// Bootstrap: no shards yet, so the safety gates are vacuous. Advance.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredDataNodePartition(sts, healthyCluster(0, 0, 1, 2), nil, 0, time.Now(), true)
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("expected advance to 1, got %v", step.Partition)
	}
	if step.Reason != ReasonNoShards {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_WaitingForRebalanceQuiescence(t *testing.T) {
	// A rebalance just completed 2s ago — quiescence window is 15s.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(2, 0, 1, 2)
	// 2 shards each living on all 3 nodes so Gate C would pass.
	shards := []gvdbclient.ShardAssignment{
		shardPrimaryReplicas(1, 0, 1, 2),
		shardPrimaryReplicas(2, 0, 1, 2),
	}
	health.LastRebalanceUnixMs = now.Add(-2 * time.Second).UnixMilli()
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForRebalanceQuiescence {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_QuiescenceRatchetFromAnnotation(t *testing.T) {
	// Coordinator claims no recent rebalance (LastRebalanceUnixMs=0), but
	// our persisted observation says a rebalance happened 5s ago. The
	// ratchet should still block.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(2, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2), shardPrimaryReplicas(2, 0, 1, 2)}
	observed := now.Add(-5 * time.Second).UnixMilli()
	step := desiredDataNodePartition(sts, health, shards, observed, now, true)
	if step.Reason != ReasonWaitingForRebalanceQuiescence {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_FirstObservationNoRebalance(t *testing.T) {
	// No prior observation and no rebalance seen: Gate B passes trivially.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(2, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2), shardPrimaryReplicas(2, 0, 1, 2)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonAdvancing {
		t.Fatalf("expected advance to 1, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_WaitingForReplicaSafety(t *testing.T) {
	// Target ordinal is currentPartition-1 = 1 → target_node_id=102.
	// Shard primary=102, replica on node 103; node 103 is DOWN. Dropping
	// 102 would leave no Ready node to serve the shard. Block.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(1, 0, 1) // only ordinals 0,1 ready; 2 (node 103) absent
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 1, 2)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForReplicaSafety {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeDesiredPartition_RF1Blocked(t *testing.T) {
	// Target ordinal 1 → target_node_id=102. Shard 1 has RF=1 as its
	// primary-only copy on node 102 (no replicas). Rollout is stuck;
	// distinguishable reason with shard id in the message.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(1, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 1)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold, got %v", step.Partition)
	}
	if step.Reason != ReasonRF1Blocked {
		t.Fatalf("reason %q", step.Reason)
	}
	if step.Message == "" {
		t.Fatalf("expected shard-id in message")
	}
}

func TestDataNodeDesiredPartition_AdvanceWhenAllGatesPass(t *testing.T) {
	// Mechanical state ready, coordinator stable, cluster healthy, no recent
	// rebalance, shard has a safe failover replica on another ready node.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(2, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{
		shardPrimaryReplicas(1, 0, 1, 2),
		shardPrimaryReplicas(2, 0, 1, 2),
	}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonAdvancing {
		t.Fatalf("expected advance to 1, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_ReplicaSafetyIgnoresNonReadyNonTargetReplica(t *testing.T) {
	// Defends against counting a replica on a DOWN node as a healthy
	// failover. shard primary=102 (target), replica=[103]; 103 is DOWN.
	// Even though effectiveSize==2 (not RF=1), healthyOther==0 → block.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(1, 0, 1) // 2 (node 103) is not in Ready set
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 1, 2)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Reason != ReasonWaitingForReplicaSafety {
		t.Fatalf("reason %q: expected WaitingForReplicaSafety (DOWN replica must not count as failover)", step.Reason)
	}
}

func TestDataNodeDesiredPartition_UnreachableCoordinatorBlocks(t *testing.T) {
	// Operator couldn't reach any coordinator → refreshCoordinatorStatus
	// returns zero-valued ClusterHealth{} (ClusterStatus==""). This must
	// NOT be interpreted as "empty cluster, safe to advance" — Gate A
	// is ordered to catch this first.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	empty := gvdbclient.ClusterHealth{} // simulates total RPC failure
	step := desiredDataNodePartition(sts, empty, nil, 0, now, true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForClusterHealth {
		t.Fatalf("reason %q: must not advance when ClusterStatus is empty", step.Reason)
	}
}

func TestDataNodeDesiredPartition_PrimaryIsTargetWithHealthyReplica(t *testing.T) {
	// Drain target (ordinal 1, node 102) is the shard's PRIMARY. A replica
	// exists on node 103 and is Ready — HandleDrainingNode will promote it.
	// Safe to advance.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	health := healthyCluster(1, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 1, 2)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonAdvancing {
		t.Fatalf("expected advance to 1, got %+v", step)
	}
}

func TestDataNodeDesiredPartition_NonDataNodeReadyIgnored(t *testing.T) {
	// A coordinator pod happens to have the same numeric node_id as a
	// drain-target data node (unlikely today given 101+ convention, but
	// defensive). Gate C filters by IsDataNode, so the coordinator's
	// Ready status must NOT count as a healthy failover replica.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	// Data node at ordinal 0 is Ready; data node at ordinal 2 is absent.
	// But a coordinator with node_id 103 IS Ready — should not count.
	health := gvdbclient.ClusterHealth{
		ClusterStatus: "healthy",
		TotalShards:   1,
		HealthyShards: 1,
		Nodes: []gvdbclient.NodeHealth{
			{NodeID: 101, Ready: true, IsDataNode: true},  // data ordinal 0
			{NodeID: 102, Ready: true, IsDataNode: true},  // data ordinal 1 (target)
			{NodeID: 103, Ready: true, IsDataNode: false}, // coordinator/query, NOT data
		},
	}
	// Shard primary=102 (target), replica=[103] (coordinator id, not data).
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 1, 2)}
	step := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if step.Reason != ReasonWaitingForReplicaSafety {
		t.Fatalf("reason %q: non-data-node Ready entries must not count as failover", step.Reason)
	}
}

func TestDataNodeDesiredPartition_WalkThroughThreeReplicaRollout(t *testing.T) {
	// End-to-end walk: new image → detect → pin → advance → advance → park.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	health := healthyCluster(1, 0, 1, 2)
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)}

	// Step 1: rollout just detected, partition missing, updatedReplicas=0.
	sts := mkSTS(3, nil, "r1", "r2", 0)
	s1 := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if s1.Partition == nil || *s1.Partition != 2 || s1.Reason != ReasonPinningForRollout {
		t.Fatalf("step1: %+v", s1)
	}

	// Step 2: pod-2 rolled to r2 (updatedReplicas=1 at partition=2). Advance.
	sts = mkSTS(3, ptrI32(2), "r1", "r2", 1)
	s2 := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if s2.Partition == nil || *s2.Partition != 1 || s2.Reason != ReasonAdvancing {
		t.Fatalf("step2: %+v", s2)
	}

	// Step 3: pod-1 rolled (updatedReplicas=2 at partition=1). Advance.
	sts = mkSTS(3, ptrI32(1), "r1", "r2", 2)
	s3 := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if s3.Partition == nil || *s3.Partition != 0 || s3.Reason != ReasonAdvancing {
		t.Fatalf("step3: %+v", s3)
	}

	// Step 4: pod-0 still rolling (partition=0, updatedReplicas=2). Hold.
	sts = mkSTS(3, ptrI32(0), "r1", "r2", 2)
	s4 := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if s4.Partition == nil || *s4.Partition != 0 || s4.Reason != ReasonWaitingForPod {
		t.Fatalf("step4: %+v", s4)
	}

	// Step 5: all pods updated. Revisions match → park at replicas-1.
	sts = mkSTS(3, ptrI32(0), "r2", "r2", 3)
	s5 := desiredDataNodePartition(sts, health, shards, 0, now, true)
	if !s5.Done || s5.Partition == nil || *s5.Partition != 2 || s5.Reason != ReasonStable {
		t.Fatalf("step5: %+v", s5)
	}
}
