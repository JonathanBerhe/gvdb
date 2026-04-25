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
// reconciler should take this pass. Mirrors CoordinatorScaleAction so
// operators watching one condition recognise the same shape on the
// other — same state-machine surface, same Reason naming convention.
type DataNodeScaleAction string

const (
	// DataNodeScaleStable: STS.Spec.Replicas == spec.dataNode.replicas AND
	// every relevant data-node is Ready. Nothing to do; condition is True.
	DataNodeScaleStable DataNodeScaleAction = "Stable"

	// DataNodeScaleGrowing: spec > current. Issue one SSA write to bring
	// spec.replicas up to target; subsequent reconciles spin in
	// WaitingForConvergence (when scale-down) or just observe the new
	// pods coming Ready (when scale-up). Includes the post-bootstrap
	// case where the main render created the STS at K8s's default of 1
	// and this reconciler immediately jumps it to the target count.
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
	// ReasonWaitingForShardVisibility: a scale-down decision is pending
	// but no coordinator answered GetShardAssignments. We refuse to act
	// without visibility — making a "safe" call against an empty shard
	// list could misclassify an actually-orphaned shard as Safe.
	ReasonWaitingForShardVisibility = "WaitingForShardVisibility"
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
//  1. Bootstrap (current == 0, STS absent) → Growing to spec. Reachable
//     only when the reconciler runs before the main render's apply has
//     created the STS, or when the STS has been deleted out from under us.
//     The reconciler's caller short-circuits in that case (it cannot SSA-
//     create a StatefulSet from a Replicas-only patch — required fields
//     like Selector are missing); this branch exists for callers that
//     pre-seed the STS via a different path.
//  2. Rollout in flight → WaitingForRollout. Hold.
//  3. current == spec → Stable, Done=true. Stable is reported even when
//     the coordinator is unreachable — alignment with reconcileCoordinator
//     Scale's "leave condition unchanged on visibility loss" stance for
//     stable clusters; we don't want a brief coordinator blip to flip the
//     condition and trigger user alarms.
//  4. current < spec → Growing to spec in one write. Scale-up is always
//     safe (adds capacity, can't orphan shards) so it bypasses both the
//     convergence and health guards. K8s's StatefulSet controller paces
//     pod creation; subsequent reconciles spin in WaitingForConvergence
//     until pods are Ready.
//  5. current > spec (scale-down) — additional safety gates apply:
//     a. Convergence guard: observedReady < current → WaitingForConvergence.
//     Don't stack a second shrink on top of one that K8s hasn't paced
//     through yet.
//     b. Health guard: any surviving ordinal not Ready in coordinator's
//     heartbeat view → WaitingForHealth.
//     c. Classify victim = current-1 via shardsOrphanedBy:
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
	// 1. Bootstrap: STS doesn't exist. The reconciler's caller refuses
	// to SSA-apply a Replicas-only patch in that state (required STS
	// spec fields would be missing on CREATE), but we still emit a
	// Growing step so the condition reflects intent rather than a misleading
	// Stable/Waiting state.
	if currentSpecReplicas == 0 {
		return DataNodeScaleStep{
			Action:            DataNodeScaleGrowing,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonScalingUp,
			Message:           "bootstrap: data-node StatefulSet absent; main render will create it",
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

	// 3. Already at target — stable. Coordinator-visibility / pod-readiness
	// blips don't flip Stable; we only report False when there is real
	// scale work to do.
	if currentSpecReplicas == specReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleStable,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonStable,
			Done:              true,
		}
	}

	// 4. Scale up — single-write growth. Bypasses convergence/health
	// guards because growing is always safe (adds replicas, can't orphan
	// shards). Crucially this also covers the post-bootstrap case where
	// the main render created the STS at the K8s-default of 1 and the
	// scale reconciler now jumps it to the target — without this branch
	// firing, the convergence guard would block here and the cluster
	// would come up as 1 pod, wait, then grow.
	if currentSpecReplicas < specReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleGrowing,
			EffectiveReplicas: specReplicas,
			Reason:            ReasonScalingUp,
		}
	}

	// 5. Scale down — classify the victim and shrink-or-hold.

	// 5a. Convergence guard — K8s still pacing the previous shrink.
	if observedReadyReplicas < currentSpecReplicas {
		return DataNodeScaleStep{
			Action:            DataNodeScaleWaitingForConvergence,
			EffectiveReplicas: currentSpecReplicas,
			Reason:            ReasonWaitingForPodTermination,
		}
	}

	// 5b. Health guard — every surviving ordinal must be Ready in the
	// coordinator's heartbeat view before we shrink further.
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

	// 5c. Safety classification.
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
