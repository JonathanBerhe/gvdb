/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"fmt"
	"time"

	"gvdb/operator/internal/gvdbclient"

	appsv1 "k8s.io/api/apps/v1"
)

// DataNodeBaseNodeID is the starting node-id for data-node pods: the k-th
// ordinal pod has node_id = DataNodeBaseNodeID + k, matching the convention
// encoded in the data-node startup script
// (operator/internal/render/datanode.go:33). The rollout's replica-safety
// gate translates ordinals to node_ids using this base so it can match the
// values reported in ShardAssignment.node_ids.
const DataNodeBaseNodeID = 101

// DataNodeRolloutStep is the desired next state of a data-node StatefulSet's
// rolling-update partition plus whether the rollout is complete. Mirrors
// RolloutStep for the coordinator, with an extra Message so the reconciler
// can surface shard-level detail in the condition (e.g. which shard blocked).
type DataNodeRolloutStep struct {
	// Partition is the value the reconciler should write to
	// spec.updateStrategy.rollingUpdate.partition. nil means "unpin".
	Partition *int32

	// Done is true when the rollout is complete or not in progress.
	Done bool

	// Reason is a short machine-readable string for the
	// ConditionDataNodeRolloutReady condition.
	Reason string

	// Message carries optional detail (shard id, stale timestamp) surfaced on
	// the condition for operator UX. Empty when Reason is self-explanatory.
	Message string
}

// Reason constants for ConditionDataNodeRolloutReady. The state-machine
// shares four values with the coordinator's ReasonStable / ReasonPinning…
// set (defined in rollout.go) so tools watching both conditions can
// pattern-match consistently; data-node-specific gates get new reason codes.
const (
	// ReasonWaitingForCoordinatorRollout: coordinator rollout is in flight;
	// never two rollouts at once because GetShardAssignments goes to the
	// coordinator leader and racing it with its own drain produces
	// inconsistent reads.
	ReasonWaitingForCoordinatorRollout = "WaitingForCoordinatorRollout"

	// ReasonWaitingForClusterHealth: cluster_status != "healthy" OR
	// healthy_shards < total_shards — some shard is degraded or migrating.
	ReasonWaitingForClusterHealth = "WaitingForClusterHealth"

	// ReasonWaitingForRebalanceQuiescence: a rebalance completed recently
	// enough (< RebalanceQuiescenceWindow) that a follow-up replica
	// migration may still be scheduled; wait before evicting the next pod.
	ReasonWaitingForRebalanceQuiescence = "WaitingForRebalanceQuiescence"

	// ReasonWaitingForReplicaSafety: dropping the drain-target pod would
	// leave at least one shard with no Ready replica on another node.
	ReasonWaitingForReplicaSafety = "WaitingForReplicaSafety"

	// ReasonRF1Blocked: drain target is the sole replica of a shard
	// (replication factor 1). Rollout is permanently stuck until the user
	// raises RF or accepts data loss. Emit a Kubernetes Event on this
	// reason so the user sees why without reading operator logs.
	ReasonRF1Blocked = "RF1Blocked"

	// ReasonNoShards: cluster has no shards yet (bootstrap); safety gates
	// are skipped because there's no data to lose.
	ReasonNoShards = "NoShards"
)

// DataNodeRolloutObservedRebalanceAnnotation stores the most recent
// lastRebalance timestamp (Unix ms) the operator has observed from the
// coordinator. The rebalance-quiescence gate blocks advance until at least
// RebalanceQuiescenceWindow has passed since this timestamp. Persisting it
// on the data-node StatefulSet survives operator restart and keeps the
// ratchet monotonic against a flapping coordinator report.
const DataNodeRolloutObservedRebalanceAnnotation = "gvdb.io/datanode-rollout-observed-rebalance"

// RebalanceQuiescenceWindow is the minimum time since the last rebalance
// before advancing the partition. 15s = 2× coordinator health-check cycle
// (5s in src/cluster/coordinator.cpp:20) + margin — HandleDrainingNode runs
// once per cycle and replacement-replica scheduling happens on the next.
const RebalanceQuiescenceWindow = 15 * time.Second

// desiredDataNodePartition is a pure function computing the next rollout
// step for a data-node StatefulSet given its live status, cluster-wide
// health, shard layout, and the previously-observed rebalance timestamp.
//
// Mirrors the coordinator's desiredPartition() (rollout.go:84) but replaces
// the Raft-leader + term-stability gates with three data-node-specific gates:
//
//	A) cluster health (cluster_status == "healthy" AND healthy_shards == total_shards)
//	B) rebalance quiescence (now - max(clusterLast, observedRebalance) >= 15s)
//	C) replica safety — dropping the drain target still leaves every shard
//	   with at least one Ready replica elsewhere (coordinator.cpp:903 only
//	   promotes routable/READY replicas, so set-membership alone isn't
//	   enough — cross-reference per-node status).
//
// The coordinator-rollout guard sits between the mechanical state (1–5) and
// the gates (A–C): we can park/clamp/wait-for-pod without coordinator data,
// but advancing requires the coordinator to be stable so its reads of
// cluster health and shard assignments are consistent.
//
// Ownership: the rollout path is the sole writer of
// spec.updateStrategy.rollingUpdate.partition on the data-node StatefulSet.
// The render layer intentionally does NOT set partition — see
// operator/internal/render/datanode.go.
func desiredDataNodePartition(
	sts *appsv1.StatefulSet,
	clusterHealth gvdbclient.ClusterHealth,
	shardAssignments []gvdbclient.ShardAssignment,
	observedRebalanceUnixMs int64,
	now time.Time,
	coordinatorRolloutReady bool,
) DataNodeRolloutStep {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	// Single-replica: no safety to preserve, render leaves partition unset.
	if replicas <= 1 {
		return DataNodeRolloutStep{Partition: nil, Done: true, Reason: ReasonStable}
	}

	// Stable: park at replicas-1 so the NEXT rollout starts with only the
	// highest-ordinal pod eligible to update.
	if sts.Status.UpdateRevision == "" ||
		sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		parked := replicas - 1
		return DataNodeRolloutStep{Partition: &parked, Done: true, Reason: ReasonStable}
	}

	currentPartition := int32(0)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}

	// Clamp into legal range (stale write or scale-down race).
	if currentPartition > replicas-1 {
		p := replicas - 1
		return DataNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// Pin when rollout just started but partition was left at 0.
	if currentPartition == 0 && sts.Status.UpdatedReplicas == 0 {
		p := replicas - 1
		return DataNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// Wait for already-rolling pods to finish before touching the next one.
	expectedUpdated := replicas - currentPartition
	if sts.Status.UpdatedReplicas < expectedUpdated {
		p := currentPartition
		return DataNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}

	// Coordinator-rollout guard: never two rollouts in flight.
	if !coordinatorRolloutReady {
		p := currentPartition
		return DataNodeRolloutStep{
			Partition: &p, Done: false,
			Reason: ReasonWaitingForCoordinatorRollout,
		}
	}

	// Gate A: cluster health. Order matters — ClusterStatus is checked FIRST
	// because a zero-valued ClusterHealth (empty string status) means "we
	// couldn't reach any coordinator", NOT "empty cluster". Taking the
	// TotalShards==0 shortcut before this check would let the rollout
	// advance blind whenever the operator loses coordinator visibility.
	if clusterHealth.ClusterStatus != "healthy" {
		p := currentPartition
		return DataNodeRolloutStep{
			Partition: &p, Done: false, Reason: ReasonWaitingForClusterHealth,
			Message: fmt.Sprintf("cluster_status=%q healthy_shards=%d/%d",
				clusterHealth.ClusterStatus,
				clusterHealth.HealthyShards, clusterHealth.TotalShards),
		}
	}
	// Genuinely healthy + no shards: bootstrap, nothing to lose.
	if clusterHealth.TotalShards == 0 {
		next := currentPartition - 1
		if next < 0 {
			p := int32(0)
			return DataNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
		}
		return DataNodeRolloutStep{Partition: &next, Done: false, Reason: ReasonNoShards}
	}
	if clusterHealth.HealthyShards != clusterHealth.TotalShards {
		p := currentPartition
		return DataNodeRolloutStep{
			Partition: &p, Done: false, Reason: ReasonWaitingForClusterHealth,
			Message: fmt.Sprintf("cluster_status=%q healthy_shards=%d/%d",
				clusterHealth.ClusterStatus,
				clusterHealth.HealthyShards, clusterHealth.TotalShards),
		}
	}

	// Gate B: rebalance quiescence. Ratchet against the persisted
	// observation — the coordinator's clusterLast can briefly report a stale
	// value (e.g. right after a coordinator restart before state reloads);
	// the annotation keeps us from advancing in that window.
	refMs := clusterHealth.LastRebalanceUnixMs
	if observedRebalanceUnixMs > refMs {
		refMs = observedRebalanceUnixMs
	}
	if refMs > 0 {
		since := now.Sub(time.UnixMilli(refMs))
		if since < RebalanceQuiescenceWindow {
			p := currentPartition
			return DataNodeRolloutStep{
				Partition: &p, Done: false, Reason: ReasonWaitingForRebalanceQuiescence,
				Message: fmt.Sprintf("%s since last rebalance (need %s)",
					since.Round(time.Second), RebalanceQuiescenceWindow),
			}
		}
	}

	// Gate C: replica safety. The drain target is the pod whose ordinal is
	// currentPartition-1 — advancing partition eligibilises it to roll.
	// The clamp at the top of the function guarantees currentPartition
	// <= replicas-1, so targetOrdinal can only be negative (when partition
	// already sits at 0 on the final pod); the >= 0 guard handles that.
	targetOrdinal := currentPartition - 1
	if targetOrdinal >= 0 {
		targetNodeID := uint32(DataNodeBaseNodeID + targetOrdinal)
		// Build the "ready failover candidates" set from DATA nodes only.
		// GetClusterHealth returns every node type (coordinator, query,
		// proxy, data) — filtering by IsDataNode avoids any implicit
		// coupling to the node-id range convention in render/datanode.go.
		ready := make(map[uint32]bool, len(clusterHealth.Nodes))
		for _, n := range clusterHealth.Nodes {
			if n.Ready && n.IsDataNode {
				ready[n.NodeID] = true
			}
		}
		for _, sa := range shardAssignments {
			// internal_service.cpp:108-111 populates primary_node_id from
			// shard_info.primary_node and node_ids[] from replica_nodes —
			// they are DISJOINT. The effective node set for availability
			// reasoning is {primary} ∪ replicas; the RF=1 case is when
			// that effective set has a single member and it is the target.
			healthyOther := 0
			containsTarget := false
			effectiveSize := 0
			if sa.PrimaryNodeID != 0 {
				effectiveSize++
				if sa.PrimaryNodeID == targetNodeID {
					containsTarget = true
				} else if ready[sa.PrimaryNodeID] {
					healthyOther++
				}
			}
			for _, nid := range sa.NodeIDs {
				effectiveSize++
				if nid == targetNodeID {
					containsTarget = true
					continue
				}
				if ready[nid] {
					healthyOther++
				}
			}
			if healthyOther >= 1 {
				continue
			}
			if containsTarget && effectiveSize == 1 {
				p := currentPartition
				return DataNodeRolloutStep{
					Partition: &p, Done: false, Reason: ReasonRF1Blocked,
					Message: fmt.Sprintf(
						"shard %d has RF=1 and only replica is on drain target node %d",
						sa.ShardID, targetNodeID),
				}
			}
			p := currentPartition
			return DataNodeRolloutStep{
				Partition: &p, Done: false, Reason: ReasonWaitingForReplicaSafety,
				Message: fmt.Sprintf("shard %d has no ready replica besides node %d",
					sa.ShardID, targetNodeID),
			}
		}
	}

	// All gates passed — advance one pod down.
	next := currentPartition - 1
	if next < 0 {
		p := int32(0)
		return DataNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}
	return DataNodeRolloutStep{Partition: &next, Done: false, Reason: ReasonAdvancing}
}
