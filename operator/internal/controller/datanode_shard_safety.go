/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"gvdb/operator/internal/gvdbclient"
)

// ShardOrphanResult summarises the shards at risk if a single data-node
// were removed from the cluster. Consumed by both:
//
//   - desiredDataNodePartition (rollout): blocks advance when the drain-target
//     pod would leave a shard without a ready failover.
//   - desiredDataNodeScaleStep  (scale): blocks a scale-down from shrinking
//     spec.replicas past a safe floor.
//
// Keeping both callers on a single pure helper guarantees rollout and scale
// agree on what "unsafe" means — there is exactly one place that encodes
// the {primary} ∪ node_ids[] disjoint union, the IsDataNode filter, and
// the RF=1 special case.
type ShardOrphanResult struct {
	// Orphaned lists shard ids where the victim is in the replica set,
	// there is at least one OTHER member, but none of them is ready.
	// Transient — a rebalance that schedules a new replica resolves it.
	Orphaned []uint32

	// RF1Shards lists shard ids where the victim is the ONLY member of the
	// effective replica set {primary} ∪ node_ids[]. Permanent — the user
	// must raise replication_factor before the operator can proceed.
	RF1Shards []uint32
}

// Safe reports whether removing the victim is safe. Equivalent to
// len(Orphaned) == 0 && len(RF1Shards) == 0.
func (r ShardOrphanResult) Safe() bool {
	return len(r.Orphaned) == 0 && len(r.RF1Shards) == 0
}

// RF1Blocked reports whether at least one shard is permanently pinned to
// the victim. This is the signal the reconciler uses to emit a Warning
// Event — unlike Orphaned, the user must act before the operator can
// make progress.
func (r ShardOrphanResult) RF1Blocked() bool {
	return len(r.RF1Shards) > 0
}

// shardsOrphanedBy classifies every shard assignment for a given victim
// node. It does not mutate its inputs and performs no I/O.
//
// Semantics per shard:
//
//  1. The effective replica set is {primary_node_id} ∪ node_ids[] — the
//     two fields are disjoint (see src/network/internal_service.cpp:108-111
//     where primary_node_id is populated from shard_info.primary_node and
//     node_ids[] from replica_nodes). Either may be zero-valued when absent.
//
//  2. A member is a "healthy failover" iff it is present in clusterHealth
//     with Ready=true AND IsDataNode=true — HandleDrainingNode in
//     src/cluster/coordinator.cpp:903 will only promote routable+ready
//     replicas, so set-membership alone is insufficient.
//
//  3. If the victim is not in the effective set, the shard is unaffected
//     and skipped.
//
//  4. If the victim IS in the effective set AND no other member is a
//     healthy failover:
//     - effective-set size == 1 → RF1Shards   (permanent)
//     - effective-set size  > 1 → Orphaned    (transient)
//
//  5. Otherwise (victim in set, at least one other healthy) → safe, skipped.
//
// The helper iterates shards in input order; Orphaned and RF1Shards
// preserve that order so callers that pick "the first" match rollout's
// pre-refactor message semantics.
func shardsOrphanedBy(
	victimNodeID uint32,
	shardAssignments []gvdbclient.ShardAssignment,
	clusterHealth gvdbclient.ClusterHealth,
) ShardOrphanResult {
	ready := make(map[uint32]bool, len(clusterHealth.Nodes))
	for _, n := range clusterHealth.Nodes {
		if n.Ready && n.IsDataNode {
			ready[n.NodeID] = true
		}
	}

	var out ShardOrphanResult
	for _, sa := range shardAssignments {
		healthyOther := 0
		containsVictim := false
		effectiveSize := 0

		if sa.PrimaryNodeID != 0 {
			effectiveSize++
			if sa.PrimaryNodeID == victimNodeID {
				containsVictim = true
			} else if ready[sa.PrimaryNodeID] {
				healthyOther++
			}
		}
		for _, nid := range sa.NodeIDs {
			effectiveSize++
			if nid == victimNodeID {
				containsVictim = true
				continue
			}
			if ready[nid] {
				healthyOther++
			}
		}

		if !containsVictim {
			continue
		}
		if healthyOther >= 1 {
			continue
		}
		if effectiveSize == 1 {
			out.RF1Shards = append(out.RF1Shards, sa.ShardID)
		} else {
			out.Orphaned = append(out.Orphaned, sa.ShardID)
		}
	}
	return out
}
