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

	"gvdb/operator/internal/gvdbclient"
)

// DataNodeScaleAction is the single next action the data-node scale
// reconciler should take this pass. Mirrors CoordinatorScaleAction for
// ConsistencyOfSurface — both conditions share the same state-machine
// shape so operators watching one learn the other at no extra cost.
type DataNodeScaleAction string

const (
	// DataNodeScaleStable: STS.Spec.Replicas == spec.dataNode.replicas AND
	// every relevant data-node is Ready. Nothing to do; condition is True.
	DataNodeScaleStable DataNodeScaleAction = "Stable"

	// DataNodeScaleGrowing: spec > current. Issue one SSA write to bring
	// spec.replicas up to target; subsequent reconciles spin in
	// WaitingForConvergence until K8s has all new pods Ready. Also the
	// bootstrap path: STS absent (current==0) falls through here so the
	// first Apply creates the STS at the desired replica count rather
	// than at K8s's default of 1 — a transient 1-replica state breaks
	// the coordinator-first startup contract for multi-data-node clusters.
	DataNodeScaleGrowing DataNodeScaleAction = "Growing"

	// DataNodeScaleShrinking: converged and the highest-ordinal pod's
	// removal would NOT orphan any shard. SSA-write spec.replicas =
	// current-1. One ordinal per reconcile; the next pass re-reads
	// shard assignments before the next step.
	DataNodeScaleShrinking DataNodeScaleAction = "Shrinking"

	// DataNodeScaleUnsafeShrink: converged, but removing the victim
	// ordinal would leave at least one shard without a Ready replica.
	// Hold spec.replicas and (throttled) fire RebalanceShards so the
	// coordinator plans replicas onto surviving ordinals. Transient:
	// resolves when rebalance completes and the next reconcile sees
	// the shard set shifted.
	DataNodeScaleUnsafeShrink DataNodeScaleAction = "UnsafeShrink"

	// DataNodeScaleRF1Blocked: the victim ordinal holds a shard whose
	// effective replica set has cardinality 1 — no rebalance can preserve
	// availability. Permanent until the user either raises
	// collection.replication_factor or cancels the scale intent. Emit a
	// Warning Event on transition (deduplicated via annotation) and
	// hold spec.replicas indefinitely.
	DataNodeScaleRF1Blocked DataNodeScaleAction = "RF1Blocked"

	// DataNodeScaleWaitingForConvergence: observedReadyReplicas <
	// spec.replicas (pods still mid-create on scale-up or mid-terminate
	// on scale-down). Hold without making new decisions; next pass
	// re-evaluates after K8s converges.
	DataNodeScaleWaitingForConvergence DataNodeScaleAction = "WaitingForConvergence"

	// DataNodeScaleWaitingForRollout: a data-node rollout is in flight.
	// Scale and rollout write different fields (replicas vs.
	// updateStrategy.rollingUpdate.partition) under separate SSA field
	// managers, but their *pod-level* effects can interleave
	// destructively — rollout's pod termination would race a scale's
	// stepwise shrink. Yield to rollout until it completes.
	DataNodeScaleWaitingForRollout DataNodeScaleAction = "WaitingForRollout"

	// DataNodeScaleWaitingForHealth: some data-node pod at an ordinal <
	// min(spec, current) is not Ready per the coordinator's heartbeat
	// view. Even though K8s convergence may report the STS settled, the
	// coordinator-side health check is a second opinion; proceeding
	// while a data-node is unhealthy risks amplifying a degraded state.
	DataNodeScaleWaitingForHealth DataNodeScaleAction = "WaitingForHealth"
)

// Reason constants for ConditionDataNodeScaleReady. ReasonStable is
// shared with rollout.go for consistency across conditions.
const (
	ReasonScalingUp                      = "ScalingUp"
	ReasonScalingDownOneStep             = "ScalingDownOneStep"
	ReasonUnsafeShardDistribution        = "UnsafeShardDistribution"
	ReasonRF1ShardPinnedToScaleVictim    = "RF1ShardPinnedToScaleVictim"
	ReasonWaitingForPodTermination       = "WaitingForPodTermination"
	ReasonScaleWaitingForDataNodeRollout = "WaitingForDataNodeRollout"
	ReasonScaleWaitingForDataNodeHealth  = "WaitingForDataNodeHealth"
)

// DataNodeScaleStep is the product of desiredDataNodeScaleStep — a
// self-describing next-action packet the reconciler can dispatch without
// re-deriving anything.
type DataNodeScaleStep struct {
	// Action describes what the reconciler should do this pass.
	Action DataNodeScaleAction

	// EffectiveReplicas is the value the scale reconciler should
	// SSA-write to STS.Spec.Replicas. On holding actions (Unsafe*,
	// RF1Blocked, Waiting*) this echoes the current value so the SSA
	// apply is idempotent. On Growing it equals specReplicas. On
	// Shrinking it is current - 1.
	EffectiveReplicas int32

	// VictimOrdinal is the data-node ordinal targeted by this step. Set
	// on Shrinking (the ordinal about to be removed), UnsafeShrink, and
	// RF1Blocked (the ordinal whose removal is blocked). Zero when
	// irrelevant.
	VictimOrdinal int32

	// UnsafeShards lists the shard ids that would be left without a
	// Ready replica if the scale-down proceeded. Non-empty only on
	// UnsafeShrink. Callers surface at most the first few on the
	// condition message.
	UnsafeShards []uint32

	// RF1Shards lists the shard ids permanently pinned to VictimOrdinal.
	// Non-empty only on RF1Blocked.
	RF1Shards []uint32

	// ShouldTriggerRebalance signals the reconciler to fire a
	// RebalanceShards RPC this pass — subject to the reconciler's own
	// external throttle (quiescence window against
	// ClusterHealth.LastRebalanceUnixMs + the persisted trigger
	// annotation). Set only on UnsafeShrink.
	ShouldTriggerRebalance bool

	// Reason is the machine-readable code for the condition.
	Reason string

	// Message carries human-readable detail (shard ids, counts) for the
	// condition's message field.
	Message string

	// Done is true only for Stable. The condition flips to True and
	// requeue drops to the idle cadence.
	Done bool
}

// desiredDataNodeScaleStep inspects the desired replica count against the
// live StatefulSet and returns the next action to converge them safely.
// Pure: no I/O, no time reads. The reconciler supplies snapshots and the
// function returns a step.
//
// Decision ladder (first match wins):
//
//  1. Bootstrap (current == 0) → Growing to spec. Shard safety is trivial
//     when there are no shards yet.
//  2. Rollout in flight → WaitingForRollout. Hold.
//  3. Pods not converged (observedReady < current) → WaitingForConvergence.
//  4. Any surviving data-node unhealthy → WaitingForHealth.
//  5. current == spec → Stable, Done=true.
//  6. current < spec → Growing to spec in one write.
//  7. current > spec → compute victim = current-1; classify via
//     shardsOrphanedBy:
//     - RF1Blocked       → RF1Blocked (permanent)
//     - Orphaned non-empty → UnsafeShrink (transient; trigger rebalance)
//     - Safe              → Shrinking by 1
//
// The invariant "shrink only one ordinal per reconcile" keeps every step
// gated on a fresh shard-assignment read — the coordinator's migration
// completion is observed externally via GetShardAssignments on the next
// pass, not deduced from internal state.
func desiredDataNodeScaleStep(
	specReplicas int32,
	currentSpecReplicas int32,
	observedReadyReplicas int32,
	shardAssignments []gvdbclient.ShardAssignment,
	clusterHealth gvdbclient.ClusterHealth,
	dataNodeRolloutReady bool,
) DataNodeScaleStep {
	// 1. Bootstrap: STS doesn't exist (or is at zero). Set it directly
	// to the desired count; no shards yet to protect.
	if currentSpecReplicas == 0 {
		return DataNodeScaleStep{
			Action:            DataNodeScaleGrowing,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonScalingUp,
			Message:           "bootstrap: creating data-node StatefulSet at target replicas",
		}
	}

	// 2. Rollout guard — never race a pod-by-pod rollout walk.
	if !dataNodeRolloutReady {
		return DataNodeScaleStep{
			Action:            DataNodeScaleWaitingForRollout,
			EffectiveReplicas: currentSpecReplicas,
			Reason:            ReasonScaleWaitingForDataNodeRollout,
			Message:           "data-node rollout in progress; scale reconciler yielding",
		}
	}

	// 3. Convergence guard — K8s is still creating or terminating pods.
	// Do not stack a second scale decision on top.
	if observedReadyReplicas < currentSpecReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleWaitingForConvergence,
			EffectiveReplicas: currentSpecReplicas,
			Reason:            ReasonWaitingForPodTermination,
		}
	}

	// 4. Health guard — the coordinator must see every surviving
	// ordinal as Ready before we perturb the membership again.
	minOrdinal := specReplicas
	if currentSpecReplicas < minOrdinal {
		minOrdinal = currentSpecReplicas
	}
	ready := make(map[uint32]bool, len(clusterHealth.Nodes))
	for _, n := range clusterHealth.Nodes {
		if n.Ready && n.IsDataNode {
			ready[n.NodeID] = true
		}
	}
	for i := int32(0); i < minOrdinal; i++ {
		if !ready[uint32(DataNodeBaseNodeID)+uint32(i)] {
			return DataNodeScaleStep{
				Action:            DataNodeScaleWaitingForHealth,
				EffectiveReplicas: currentSpecReplicas,
				Reason:            ReasonScaleWaitingForDataNodeHealth,
				Message:           "surviving data-node ordinal not Ready per coordinator heartbeat",
			}
		}
	}

	// 5. Already at target — stable.
	if currentSpecReplicas == specReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleStable,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonStable,
			Done:              true,
		}
	}

	// 6. Scale up — single-write growth.
	if currentSpecReplicas < specReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleGrowing,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonScalingUp,
		}
	}

	// 7. Scale down — classify the victim and shrink-or-hold.
	victim := currentSpecReplicas - 1
	victimNodeID := uint32(DataNodeBaseNodeID) + uint32(victim)
	risk := shardsOrphanedBy(victimNodeID, shardAssignments, clusterHealth)

	if risk.RF1Blocked() {
		// Permanent block: surface every RF=1 shard id so the user
		// knows which collections to bump.
		return DataNodeScaleStep{
			Action:            DataNodeScaleRF1Blocked,
			EffectiveReplicas: currentSpecReplicas,
			VictimOrdinal:     victim,
			RF1Shards:         risk.RF1Shards,
			Reason:            ReasonRF1ShardPinnedToScaleVictim,
			Message:           formatRF1ScaleMessage(victimNodeID, risk.RF1Shards),
		}
	}

	if !risk.Safe() {
		// Transient block: ask the coordinator to re-plan. The
		// reconciler applies a quiescence throttle so we don't
		// hammer rebalance every 2-5s.
		return DataNodeScaleStep{
			Action:                 DataNodeScaleUnsafeShrink,
			EffectiveReplicas:      currentSpecReplicas,
			VictimOrdinal:          victim,
			UnsafeShards:           risk.Orphaned,
			ShouldTriggerRebalance: true,
			Reason:                 ReasonUnsafeShardDistribution,
			Message:                formatUnsafeScaleMessage(victimNodeID, risk.Orphaned),
		}
	}

	// Safe — shrink by one ordinal.
	return DataNodeScaleStep{
		Action:            DataNodeScaleShrinking,
		EffectiveReplicas: currentSpecReplicas - 1,
		VictimOrdinal:     victim,
		Reason:            ReasonScalingDownOneStep,
	}
}

// formatRF1ScaleMessage builds a stable, test-comparable condition message
// that names the victim node and up to 5 shard ids.
func formatRF1ScaleMessage(victimNodeID uint32, rf1Shards []uint32) string {
	return formatScaleShardMessage("RF=1 shard(s) pinned to drain-target node",
		victimNodeID, rf1Shards)
}

// formatUnsafeScaleMessage builds the message for UnsafeShrink.
func formatUnsafeScaleMessage(victimNodeID uint32, orphaned []uint32) string {
	return formatScaleShardMessage("shard(s) would lose their only ready replica if node",
		victimNodeID, orphaned)
}

// formatScaleShardMessage renders "<phrase> <node>: [<ids>]" with up to 5
// shard ids shown. The API server limits condition.message size, so we
// truncate defensively and let the operator log carry the full list.
// Iteration order is stable (shardsOrphanedBy preserves input order).
func formatScaleShardMessage(phrase string, victimNodeID uint32, shards []uint32) string {
	const maxShown = 5
	show := shards
	truncated := false
	if len(show) > maxShown {
		show = show[:maxShown]
		truncated = true
	}
	if truncated {
		return fmt.Sprintf("%s %d: %v (+%d more)", phrase, victimNodeID, show, len(shards)-maxShown)
	}
	return fmt.Sprintf("%s %d: %v", phrase, victimNodeID, show)
}
