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

	"gvdb/operator/internal/gvdbclient"
)

// TestDataNodeScale_BootstrapCreatesWithTarget verifies that when the STS
// does not exist yet (currentSpec==0) the reconciler proposes Growing
// straight to the target replica count — rather than relying on K8s's
// default of 1, which would bring up the coordinator-first cluster with
// only one data-node.
func TestDataNodeScale_BootstrapCreatesWithTarget(t *testing.T) {
	step := desiredDataNodeScaleStep(
		3,                          // specReplicas
		0,                          // currentSpecReplicas (STS absent)
		0,                          // observedReadyReplicas
		nil,                        // no shards yet
		gvdbclient.ClusterHealth{}, // no health info during bootstrap
		true,                       // rollout ready
	)
	if step.Action != DataNodeScaleGrowing {
		t.Fatalf("expected Growing on bootstrap, got %+v", step)
	}
	if step.EffectiveReplicas != 3 {
		t.Fatalf("expected EffectiveReplicas=3, got %d", step.EffectiveReplicas)
	}
}

func TestDataNodeScale_Stable(t *testing.T) {
	step := desiredDataNodeScaleStep(
		3, 3, 3,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)},
		healthyCluster(1, 0, 1, 2),
		true,
	)
	if step.Action != DataNodeScaleStable || !step.Done {
		t.Fatalf("expected Stable+Done, got %+v", step)
	}
	if step.EffectiveReplicas != 3 {
		t.Fatalf("expected EffectiveReplicas=3, got %d", step.EffectiveReplicas)
	}
	if step.Reason != ReasonStable {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeScale_ScaleUp(t *testing.T) {
	// 3 → 5: single SSA write to grow. Effective=5 even though shards live
	// only on the current three ordinals — new pods auto-join and auto-
	// rebalance picks up from there.
	step := desiredDataNodeScaleStep(
		5, 3, 3,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)},
		healthyCluster(1, 0, 1, 2),
		true,
	)
	if step.Action != DataNodeScaleGrowing {
		t.Fatalf("expected Growing, got %+v", step)
	}
	if step.EffectiveReplicas != 5 {
		t.Fatalf("expected EffectiveReplicas=5, got %d", step.EffectiveReplicas)
	}
}

func TestDataNodeScale_SafeShrinkOneStep(t *testing.T) {
	// Scale 5 → 3. Victim ordinal 4 (node 105). Shard is RF=3 on {0,1,2};
	// node 4 doesn't own anything → safe, shrink to 4.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)},
		healthyCluster(1, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleShrinking {
		t.Fatalf("expected Shrinking, got %+v", step)
	}
	if step.EffectiveReplicas != 4 {
		t.Fatalf("expected EffectiveReplicas=4, got %d", step.EffectiveReplicas)
	}
	if step.VictimOrdinal != 4 {
		t.Fatalf("expected VictimOrdinal=4, got %d", step.VictimOrdinal)
	}
	if step.Reason != ReasonScalingDownOneStep {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeScale_UnsafeShrinkSingleRiskyShard(t *testing.T) {
	// Scale 5 → 3. Victim ordinal 4 (node 105). Shard 1's only members
	// are {primary=105, replica=[106]} and node 106 isn't in the cluster
	// (no ordinal 5 healthy). Removing ordinal 4 would leave the shard
	// with zero Ready replicas — block, ask for rebalance. We intentionally
	// only check the IMMEDIATE victim (ordinal 4) at this step; ordinal 3
	// becomes the next step's concern.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 4, 5)},
		healthyCluster(1, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleUnsafeShrink {
		t.Fatalf("expected UnsafeShrink, got %+v", step)
	}
	if step.EffectiveReplicas != 5 {
		t.Fatalf("expected hold at 5, got %d", step.EffectiveReplicas)
	}
	if !step.ShouldTriggerRebalance {
		t.Fatalf("expected ShouldTriggerRebalance=true")
	}
	if len(step.UnsafeShards) != 1 || step.UnsafeShards[0] != 1 {
		t.Fatalf("expected UnsafeShards=[1], got %v", step.UnsafeShards)
	}
	if step.Reason != ReasonUnsafeShardDistribution {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeScale_UnsafeShrinkMultipleRisky(t *testing.T) {
	// Two risky shards — both should be surfaced. Victim is ordinal 4
	// (node 105). Shard 1 has primary=105 + replica=[106]; shard 7 has
	// primary=107 + replicas=[105, 106]. Neither 106 nor 107 is in the
	// cluster's Ready set, so removing ordinal 4 would orphan both.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{
			shardPrimaryReplicas(1, 4, 5),    // primary=105(victim), replica=[106 NotReady]
			shardPrimaryReplicas(7, 6, 4, 5), // primary=107 NotReady, replicas=[105 victim, 106 NotReady]
		},
		healthyCluster(2, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleUnsafeShrink {
		t.Fatalf("expected UnsafeShrink, got %+v", step)
	}
	if len(step.UnsafeShards) != 2 {
		t.Fatalf("expected 2 UnsafeShards, got %v", step.UnsafeShards)
	}
}

func TestDataNodeScale_RF1BlockedPermanent(t *testing.T) {
	// Victim ordinal 4 (node 105). Shard 1 has effective RF=1 —
	// only its primary is set, no replicas. Permanent block: user must
	// bump RF or cancel scale intent.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 4)},
		healthyCluster(1, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleRF1Blocked {
		t.Fatalf("expected RF1Blocked, got %+v", step)
	}
	if step.EffectiveReplicas != 5 {
		t.Fatalf("expected hold at 5, got %d", step.EffectiveReplicas)
	}
	if len(step.RF1Shards) != 1 || step.RF1Shards[0] != 1 {
		t.Fatalf("expected RF1Shards=[1], got %v", step.RF1Shards)
	}
	if step.Reason != ReasonRF1ShardPinnedToScaleVictim {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeScale_WaitingForConvergence(t *testing.T) {
	// STS Spec.Replicas=4 (scale-down in flight from 5), Status.ReadyReplicas=3
	// (pod 4 still terminating). Don't stack a second shrink decision.
	step := desiredDataNodeScaleStep(
		3, 4, 3,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)},
		healthyCluster(1, 0, 1, 2, 3),
		true,
	)
	if step.Action != DataNodeScaleWaitingForConvergence {
		t.Fatalf("expected WaitingForConvergence, got %+v", step)
	}
	if step.EffectiveReplicas != 4 {
		t.Fatalf("expected hold at 4, got %d", step.EffectiveReplicas)
	}
	if step.Reason != ReasonWaitingForPodTermination {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDataNodeScale_WaitingForRollout(t *testing.T) {
	// Rollout owns partition; scale must yield regardless of any other state.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)},
		healthyCluster(1, 0, 1, 2, 3, 4),
		false, // rollout NOT ready
	)
	if step.Action != DataNodeScaleWaitingForRollout {
		t.Fatalf("expected WaitingForRollout, got %+v", step)
	}
	if step.EffectiveReplicas != 5 {
		t.Fatalf("expected hold at 5, got %d", step.EffectiveReplicas)
	}
}

func TestDataNodeScale_WaitingForHealth(t *testing.T) {
	// 5→3 but ordinal 1 is unhealthy per the coordinator. Even though K8s
	// status says pods are Ready, the coordinator's heartbeat view is a
	// second opinion — don't scale against a degraded cluster.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 2)},
		healthyCluster(1, 0, 2, 3, 4), // ordinal 1 missing from Nodes
		true,
	)
	if step.Action != DataNodeScaleWaitingForHealth {
		t.Fatalf("expected WaitingForHealth, got %+v", step)
	}
	if step.EffectiveReplicas != 5 {
		t.Fatalf("expected hold at 5, got %d", step.EffectiveReplicas)
	}
}

func TestDataNodeScale_WalkThrough5to3SafeStart(t *testing.T) {
	// Happy-path walk: start 5→3 with safe distribution, shrink by one
	// ordinal per reconcile with convergence between steps.
	shards := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1, 2)}

	// Round 1: 5 pods, victim=4, safe → shrink to 4.
	round1 := desiredDataNodeScaleStep(
		3, 5, 5, shards, healthyCluster(1, 0, 1, 2, 3, 4), true)
	if round1.Action != DataNodeScaleShrinking || round1.EffectiveReplicas != 4 {
		t.Fatalf("round1: %+v", round1)
	}

	// Round 2: STS Spec=4 but pod still terminating (Ready=3) → wait.
	round2 := desiredDataNodeScaleStep(
		3, 4, 3, shards, healthyCluster(1, 0, 1, 2, 3), true)
	if round2.Action != DataNodeScaleWaitingForConvergence {
		t.Fatalf("round2: %+v", round2)
	}

	// Round 3: K8s converged, Ready=4, victim=3, safe → shrink to 3.
	round3 := desiredDataNodeScaleStep(
		3, 4, 4, shards, healthyCluster(1, 0, 1, 2, 3), true)
	if round3.Action != DataNodeScaleShrinking || round3.EffectiveReplicas != 3 {
		t.Fatalf("round3: %+v", round3)
	}

	// Round 4: Converged at 3; at target → Stable.
	round4 := desiredDataNodeScaleStep(
		3, 3, 3, shards, healthyCluster(1, 0, 1, 2), true)
	if round4.Action != DataNodeScaleStable || !round4.Done {
		t.Fatalf("round4: %+v", round4)
	}
}

func TestDataNodeScale_WalkThroughWithRebalance(t *testing.T) {
	// Start: 5 pods, 3→target, but shard 1 is pinned to ordinal 4 (unsafe).
	// The reconciler triggers rebalance; after the coordinator moves the
	// shard to ordinal 0 (simulated), the next call becomes Safe.

	// Round 1: shard on (primary=105 victim, replica=[106 NotReady]) → Unsafe,
	// trigger rebalance. Ordinal 3 is healthy but the shard doesn't live
	// there yet.
	before := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 4, 5)}
	round1 := desiredDataNodeScaleStep(
		3, 5, 5, before, healthyCluster(1, 0, 1, 2, 3, 4), true)
	if round1.Action != DataNodeScaleUnsafeShrink {
		t.Fatalf("round1 expected UnsafeShrink, got %+v", round1)
	}
	if !round1.ShouldTriggerRebalance {
		t.Fatalf("round1 expected ShouldTriggerRebalance=true")
	}

	// Round 2 (after rebalance completed): shard now on ordinal 0 → Safe.
	after := []gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0, 1)}
	round2 := desiredDataNodeScaleStep(
		3, 5, 5, after, healthyCluster(1, 0, 1, 2, 3, 4), true)
	if round2.Action != DataNodeScaleShrinking || round2.EffectiveReplicas != 4 {
		t.Fatalf("round2: %+v", round2)
	}
}

func TestDataNodeScale_ScaleToSameReplicas(t *testing.T) {
	// spec == current; no-op even if some shards live on the "top" ordinal.
	step := desiredDataNodeScaleStep(
		3, 3, 3,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 2)},
		healthyCluster(1, 0, 1, 2),
		true,
	)
	if step.Action != DataNodeScaleStable || !step.Done {
		t.Fatalf("expected Stable+Done, got %+v", step)
	}
}

func TestDataNodeScale_ScaleDownWithNoShardsIsSafe(t *testing.T) {
	// Bootstrap-era cluster: no shards yet, user scales 3→1. No data to
	// protect → safe shrink.
	step := desiredDataNodeScaleStep(
		1, 3, 3,
		nil, // no shards
		healthyCluster(0, 0, 1, 2),
		true,
	)
	if step.Action != DataNodeScaleShrinking || step.EffectiveReplicas != 2 {
		t.Fatalf("expected Shrinking to 2, got %+v", step)
	}
}

func TestDataNodeScale_VictimIsPrimaryWithHealthyReplica(t *testing.T) {
	// Victim ordinal 4 (node 105) is the shard's PRIMARY; replica is on
	// ordinal 0 (node 101) which survives the scale-down. HandleDrainingNode
	// will promote that replica — safe to shrink.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 4, 0)},
		healthyCluster(1, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleShrinking {
		t.Fatalf("expected Shrinking (replica on surviving ordinal is safe), got %+v", step)
	}
	if step.EffectiveReplicas != 4 {
		t.Fatalf("expected EffectiveReplicas=4, got %d", step.EffectiveReplicas)
	}
}

func TestDataNodeScale_RF1OnNonVictimShardDoesNotBlock(t *testing.T) {
	// Defensive: a separate shard has RF=1 pinned to a surviving ordinal
	// (0). Victim is ordinal 4. Scale must still proceed — the RF=1
	// shard is on a node that's NOT going away.
	step := desiredDataNodeScaleStep(
		3, 5, 5,
		[]gvdbclient.ShardAssignment{shardPrimaryReplicas(1, 0)}, // RF=1 on ordinal 0 (safe)
		healthyCluster(1, 0, 1, 2, 3, 4),
		true,
	)
	if step.Action != DataNodeScaleShrinking {
		t.Fatalf("expected Shrinking (RF=1 on non-victim is fine), got %+v", step)
	}
}
