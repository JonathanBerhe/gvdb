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

	appsv1 "k8s.io/api/apps/v1"
)

// ptrI32 returns a pointer to the given int32. Using a local helper instead
// of pulling in k8s.io/utils/ptr keeps the test self-contained.
func ptrI32(v int32) *int32 { return &v }

// mkSTS builds a coordinator StatefulSet with the fields the rollout state
// machine cares about. Any field the tests don't set stays at zero value.
func mkSTS(replicas int32, partition *int32, current, update string, updatedReplicas int32) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	sts.Spec.Replicas = &replicas
	if partition != nil {
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{
			Partition: partition,
		}
	}
	sts.Status.CurrentRevision = current
	sts.Status.UpdateRevision = update
	sts.Status.UpdatedReplicas = updatedReplicas
	return sts
}

func TestDesiredPartition_Stable(t *testing.T) {
	// updateRevision == currentRevision → rollout is done/not in progress.
	// Partition is parked at replicas-1 so the NEXT rollout pins safely
	// before K8s can roll multiple pods in parallel.
	sts := mkSTS(3, ptrI32(2), "r1", "r1", 3)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if !step.Done {
		t.Fatalf("stable STS: expected Done=true")
	}
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("stable STS: expected partition=replicas-1=2 (parked), got %v", step.Partition)
	}
	if step.Reason != ReasonStable {
		t.Fatalf("stable STS: reason %q", step.Reason)
	}
}

func TestDesiredPartition_FirstDetectionPinsToHighestOrdinal(t *testing.T) {
	// updateRevision != currentRevision, no pods yet updated → we pin the
	// partition at replicas-1 so only the highest-ordinal pod rolls first.
	sts := mkSTS(3, nil, "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Done {
		t.Fatalf("fresh rollout: expected Done=false")
	}
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("fresh rollout: expected partition=2, got %v", step.Partition)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDesiredPartition_WaitForPodToCatchUp(t *testing.T) {
	// partition=2, but updatedReplicas still 0 → the pinned pod (pod-2)
	// hasn't finished updating yet. Hold the partition.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Done {
		t.Fatalf("expected Done=false")
	}
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected partition stays at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForPod {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDesiredPartition_WaitForLeaderBeforeAdvancing(t *testing.T) {
	// partition=2, updatedReplicas=1 (pod-2 done), but no leader yet →
	// do NOT decrement partition. This is the quorum-safety gate.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 0}, 0)
	if step.Done {
		t.Fatalf("expected Done=false")
	}
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected partition stays at 2 (no leader), got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForLeader {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDesiredPartition_AdvanceWhenLeaderPresent(t *testing.T) {
	// partition=2, updatedReplicas=1 (pod-2 done), leader present →
	// decrement to 1 so pod-1 can now update.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Done {
		t.Fatalf("expected Done=false")
	}
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("expected partition=1, got %v", step.Partition)
	}
	if step.Reason != ReasonAdvancing {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestDesiredPartition_FinalPodWaitsForCompletion(t *testing.T) {
	// partition=0, updatedReplicas=2 (pods 2 + 1), pod-0 is being updated.
	// Leader is present. We're already at partition==0 — don't go negative;
	// just wait for the final update to complete.
	sts := mkSTS(3, ptrI32(0), "r1", "r2", 2)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Done {
		t.Fatalf("expected Done=false — final pod still updating")
	}
	if step.Partition == nil || *step.Partition != 0 {
		t.Fatalf("expected partition=0, got %v", step.Partition)
	}
}

func TestDesiredPartition_NoRollingUpdateStrategy(t *testing.T) {
	// STS has no rollingUpdate struct set at all (default OnDelete or
	// equivalent). First detection still pins at replicas-1.
	sts := mkSTS(5, nil, "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Partition == nil || *step.Partition != 4 {
		t.Fatalf("expected partition=4 for 5-replica rollout, got %v", step.Partition)
	}
}

func TestDesiredPartition_SingleReplicaNotManaged(t *testing.T) {
	// 1 replica: no quorum to preserve, so the state machine leaves
	// partition untouched (Done=true, Partition=nil).
	sts := mkSTS(1, nil, "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if !step.Done {
		t.Fatalf("1-replica: expected Done=true")
	}
	if step.Partition != nil {
		t.Fatalf("1-replica: expected partition=nil (unmanaged), got %v", *step.Partition)
	}
}

func TestDesiredPartition_TermChangeBlocksAdvance(t *testing.T) {
	// partition=2, updatedReplicas=1 (pod-2 done), leader present, but the
	// term we see NOW (7) differs from what we observed last reconcile
	// (5) — a just-elected leader could be about to lose quorum again.
	// Refuse to advance.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	leader := gvdbclient.LeaderInfo{LeaderID: 1, CurrentTerm: 7}
	step := desiredPartition(sts, leader, 5)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("term change: partition should stay at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForTerm {
		t.Fatalf("term change: reason %q want %q", step.Reason, ReasonWaitingForTerm)
	}
}

func TestDesiredPartition_TermStableAllowsAdvance(t *testing.T) {
	// Same state as above but term matches the prior observation → advance.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	leader := gvdbclient.LeaderInfo{LeaderID: 1, CurrentTerm: 5}
	step := desiredPartition(sts, leader, 5)
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("term stable: expected advance to 1, got %v", step.Partition)
	}
}

func TestDesiredPartition_FirstObservationDoesNotBlock(t *testing.T) {
	// On the very first reconcile we have observedTerm=0 (no prior
	// observation). The gate must not block forever waiting for a match;
	// it should pass once the STS sees a non-zero current term.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	leader := gvdbclient.LeaderInfo{LeaderID: 1, CurrentTerm: 3}
	step := desiredPartition(sts, leader, 0)
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("first observation: expected advance, got %v / %s", step.Partition, step.Reason)
	}
}

func TestDesiredPartition_ClampOversizedPartition(t *testing.T) {
	// Someone (user, stale write, scale-down) left partition at 7 on a
	// 3-replica STS. Snap back to replicas-1=2 instead of waiting N
	// reconciles to walk down.
	sts := mkSTS(3, ptrI32(7), "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("clamp: expected partition=2, got %v", step.Partition)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("clamp: reason %q", step.Reason)
	}
}

func TestDesiredPartition_ScaleUpDuringRollout(t *testing.T) {
	// User scaled 3→5 while a rollout is in flight. partition=2 means
	// pods 2,3,4 are eligible. updatedReplicas=1 — we're still waiting.
	// Keep the existing partition; don't re-pin since the rollout is
	// already mid-flight.
	sts := mkSTS(5, ptrI32(2), "r1", "r2", 1)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("scale-up: expected partition=2 (keep), got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForPod {
		t.Fatalf("scale-up: reason %q", step.Reason)
	}
}

func TestDesiredPartition_ScaleDownDuringRolloutClamps(t *testing.T) {
	// User scaled 5→3 while partition was 4. New replicas-1=2, so our
	// clamp kicks in and pulls partition down to 2.
	sts := mkSTS(3, ptrI32(4), "r1", "r2", 0)
	step := desiredPartition(sts, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("scale-down: expected clamp to 2, got %v", step.Partition)
	}
}

func TestDesiredPartition_WalkThroughThreeReplicaRollout(t *testing.T) {
	// Simulate the full pod-by-pod walk to prove the state machine is
	// monotonic: partition goes replicas-1 → 0 as pods catch up.
	replicas := int32(3)

	// 1. First detection (no partition, 0 updated).
	s1 := mkSTS(replicas, nil, "r1", "r2", 0)
	st1 := desiredPartition(s1, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if st1.Partition == nil || *st1.Partition != 2 {
		t.Fatalf("step 1: partition=%v", st1.Partition)
	}

	// 2. Partition=2, pod-2 not done yet.
	s2 := mkSTS(replicas, ptrI32(2), "r1", "r2", 0)
	st2 := desiredPartition(s2, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if *st2.Partition != 2 || st2.Reason != ReasonWaitingForPod {
		t.Fatalf("step 2: %v %s", st2.Partition, st2.Reason)
	}

	// 3. Partition=2, pod-2 done, leader present → advance to 1.
	s3 := mkSTS(replicas, ptrI32(2), "r1", "r2", 1)
	st3 := desiredPartition(s3, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if *st3.Partition != 1 {
		t.Fatalf("step 3: partition=%v", st3.Partition)
	}

	// 4. Partition=1, pod-1 done, leader present → advance to 0.
	s4 := mkSTS(replicas, ptrI32(1), "r1", "r2", 2)
	st4 := desiredPartition(s4, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if *st4.Partition != 0 {
		t.Fatalf("step 4: partition=%v", st4.Partition)
	}

	// 5. Partition=0, pod-0 still updating.
	s5 := mkSTS(replicas, ptrI32(0), "r1", "r2", 2)
	st5 := desiredPartition(s5, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if *st5.Partition != 0 || st5.Done {
		t.Fatalf("step 5: %v done=%v", st5.Partition, st5.Done)
	}

	// 6. Pod-0 done (updatedReplicas=3), revisions match → park partition
	// back at replicas-1 so the NEXT rollout starts safely.
	s6 := mkSTS(replicas, ptrI32(0), "r2", "r2", 3)
	st6 := desiredPartition(s6, gvdbclient.LeaderInfo{LeaderID: 1}, 0)
	if !st6.Done || st6.Partition == nil || *st6.Partition != 2 {
		t.Fatalf("step 6: want done+parked-at-2, got partition=%v done=%v", st6.Partition, st6.Done)
	}
}
