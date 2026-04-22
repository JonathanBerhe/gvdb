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
)

func TestQueryNodeDesiredPartition_Stable(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r1", 3)
	step := desiredQueryNodePartition(sts, true)
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

func TestQueryNodeDesiredPartition_SingleReplicaPassthrough(t *testing.T) {
	// Single-replica query-node: no parallel-roll hazard. Render leaves
	// partition unset and we don't manage it.
	sts := mkSTS(1, nil, "r1", "r2", 0)
	step := desiredQueryNodePartition(sts, true)
	if !step.Done || step.Partition != nil {
		t.Fatalf("single-replica: expected Done=true partition=nil, got %+v", step)
	}
}

func TestQueryNodeDesiredPartition_FirstDetectionPinsToHighestOrdinal(t *testing.T) {
	sts := mkSTS(3, nil, "r1", "r2", 0)
	step := desiredQueryNodePartition(sts, true)
	if step.Done || step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected partition=2 Done=false, got %+v", step)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestQueryNodeDesiredPartition_ClampOversizedPartition(t *testing.T) {
	sts := mkSTS(3, ptrI32(5), "r1", "r2", 3)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected clamp to replicas-1=2, got %v", step.Partition)
	}
	if step.Reason != ReasonPinningForRollout {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestQueryNodeDesiredPartition_ScaleDownDuringRolloutClamps(t *testing.T) {
	sts := mkSTS(2, ptrI32(2), "r1", "r2", 1)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 1 {
		t.Fatalf("expected clamp to replicas-1=1, got %v", step.Partition)
	}
}

func TestQueryNodeDesiredPartition_ScaleUpDuringRollout(t *testing.T) {
	// Scale-up from 3→4 mid-rollout: partition=1, updatedReplicas=2.
	// expectedUpdated = 4 - 1 = 3, got 2 → hold partition.
	sts := mkSTS(4, ptrI32(1), "r1", "r2", 2)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 1 with WaitingForPod, got %+v", step)
	}
}

func TestQueryNodeDesiredPartition_WaitForPodCatchUp(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 0)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 2 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 2 with WaitingForPod, got %+v", step)
	}
}

func TestQueryNodeDesiredPartition_FinalPodWaitsForCompletion(t *testing.T) {
	// Partition is already 0; the last pod is still updating.
	sts := mkSTS(3, ptrI32(0), "r1", "r2", 2)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 0 || step.Reason != ReasonWaitingForPod {
		t.Fatalf("expected hold at 0 with WaitingForPod, got %+v", step)
	}
}

func TestQueryNodeDesiredPartition_WaitingForPriorRollouts(t *testing.T) {
	// Mechanical state is ready to advance, but coordinator OR data-node
	// rollout is still in flight. Hold for single-phase UX.
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredQueryNodePartition(sts, false)
	if step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected hold at 2, got %v", step.Partition)
	}
	if step.Reason != ReasonWaitingForPriorRollouts {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestQueryNodeDesiredPartition_PriorRolloutsGateDoesNotBlockParking(t *testing.T) {
	// Stable STS during another component's rollout must still park at
	// replicas-1 — the gate only affects the advance step, not the
	// mechanical "park when revisions match" behavior.
	sts := mkSTS(3, ptrI32(0), "r1", "r1", 3)
	step := desiredQueryNodePartition(sts, false)
	if !step.Done || step.Partition == nil || *step.Partition != 2 {
		t.Fatalf("expected park at 2 with Done=true, got %+v", step)
	}
	if step.Reason != ReasonStable {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestQueryNodeDesiredPartition_AdvanceWhenPriorRolloutsReady(t *testing.T) {
	sts := mkSTS(3, ptrI32(2), "r1", "r2", 1)
	step := desiredQueryNodePartition(sts, true)
	if step.Partition == nil || *step.Partition != 1 || step.Reason != ReasonAdvancing {
		t.Fatalf("expected advance to 1, got %+v", step)
	}
}

func TestQueryNodeDesiredPartition_WalkThroughThreeReplicaRollout(t *testing.T) {
	// End-to-end walk: new image → detect → pin → advance → advance → park.
	// Step 1: rollout just detected, partition missing.
	sts := mkSTS(3, nil, "r1", "r2", 0)
	s1 := desiredQueryNodePartition(sts, true)
	if s1.Partition == nil || *s1.Partition != 2 || s1.Reason != ReasonPinningForRollout {
		t.Fatalf("step1: %+v", s1)
	}

	// Step 2: pod-2 rolled (updatedReplicas=1 at partition=2). Advance.
	sts = mkSTS(3, ptrI32(2), "r1", "r2", 1)
	s2 := desiredQueryNodePartition(sts, true)
	if s2.Partition == nil || *s2.Partition != 1 || s2.Reason != ReasonAdvancing {
		t.Fatalf("step2: %+v", s2)
	}

	// Step 3: pod-1 rolled (updatedReplicas=2 at partition=1). Advance.
	sts = mkSTS(3, ptrI32(1), "r1", "r2", 2)
	s3 := desiredQueryNodePartition(sts, true)
	if s3.Partition == nil || *s3.Partition != 0 || s3.Reason != ReasonAdvancing {
		t.Fatalf("step3: %+v", s3)
	}

	// Step 4: pod-0 still rolling (partition=0, updatedReplicas=2). Hold.
	sts = mkSTS(3, ptrI32(0), "r1", "r2", 2)
	s4 := desiredQueryNodePartition(sts, true)
	if s4.Partition == nil || *s4.Partition != 0 || s4.Reason != ReasonWaitingForPod {
		t.Fatalf("step4: %+v", s4)
	}

	// Step 5: all pods updated. Revisions match → park at replicas-1.
	sts = mkSTS(3, ptrI32(0), "r2", "r2", 3)
	s5 := desiredQueryNodePartition(sts, true)
	if !s5.Done || s5.Partition == nil || *s5.Partition != 2 || s5.Reason != ReasonStable {
		t.Fatalf("step5: %+v", s5)
	}
}
