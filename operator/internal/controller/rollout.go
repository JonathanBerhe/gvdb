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

	appsv1 "k8s.io/api/apps/v1"
)

// RolloutStep is the desired next state of a coordinator StatefulSet's
// rolling-update partition plus whether the rollout is complete.
type RolloutStep struct {
	// Partition is the value the reconciler should write to
	// spec.updateStrategy.rollingUpdate.partition. nil means "unpin" (clear
	// the partition so K8s treats this as a stable StatefulSet).
	Partition *int32

	// Done is true when the rollout is complete or not in progress.
	Done bool

	// Reason is a short machine-readable string for the
	// CoordinatorRolloutReady condition (e.g. "WaitingForLeader",
	// "WaitingForPod", "Stable").
	Reason string
}

// Reason constants mirrored on the CoordinatorRolloutReady condition.
const (
	ReasonStable             = "Stable"
	ReasonPinningForRollout  = "PinningForRollout"
	ReasonWaitingForPod      = "WaitingForPod"
	ReasonWaitingForLeader   = "WaitingForLeader"
	ReasonWaitingForTerm     = "WaitingForStableTerm"
	ReasonAdvancing          = "Advancing"
)

// RolloutObservedTermAnnotation stores the Raft term the operator saw on
// the last reconcile pass. The state-machine refuses to advance partition
// when the currently-observed term differs, so a flapping leader blocks
// the rollout instead of driving it pod-by-pod into quorum loss.
const RolloutObservedTermAnnotation = "gvdb.io/rollout-observed-term"

// desiredPartition is a pure function computing the next rollout step for a
// coordinator StatefulSet given its live status and a leader-info probe.
//
// Ownership: the reconciler's rollout path is the sole writer of
// spec.updateStrategy.rollingUpdate.partition. The render layer intentionally
// does NOT set partition so SSA doesn't create a write-loop with this
// state machine. See operator/internal/render/coordinator.go.
//
// Strategy (roadmap 0b.6.C):
//
//  1. If updateRevision == currentRevision, the StatefulSet is stable →
//     park partition at replicas-1 so the NEXT rollout starts with only
//     the highest-ordinal pod eligible to update.
//  2. Otherwise wait until updatedReplicas reaches what the current
//     partition permits (replicas - partition).
//  3. Then gate on BOTH leader presence AND term stability (no term
//     change since the last observation). A fresh election is a quorum
//     hazard: advancing partition while the leader just flipped risks
//     draining a pod that's about to lose connectivity to the new
//     leader mid-append.
//  4. When ready to advance, decrement partition by 1. At partition==0,
//     the final pod starts updating; we keep partition at 0 until
//     updateRevision==currentRevision, then snap it back up to
//     replicas-1 for the next rollout.
//
// `observedTerm` is the term we saw on the previous reconcile pass
// (zero means "no prior observation"); the caller is responsible for
// persisting it onto the StatefulSet via RolloutObservedTermAnnotation
// so the invariant survives operator restarts.
//
// The function takes pointers for testability (tests can construct a
// StatefulSet literal without reaching for the Scheme or deep-copies).
func desiredPartition(
	sts *appsv1.StatefulSet,
	leader gvdbclient.LeaderInfo,
	observedTerm uint64,
) RolloutStep {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	// For single-replica there's no quorum to preserve; the render layer
	// leaves partition unset and we don't manage it.
	if replicas <= 1 {
		return RolloutStep{Partition: nil, Done: true, Reason: ReasonStable}
	}

	// Stable: park partition at replicas-1 so the next rollout is safe.
	if sts.Status.UpdateRevision == "" ||
		sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		parked := replicas - 1
		return RolloutStep{Partition: &parked, Done: true, Reason: ReasonStable}
	}

	currentPartition := int32(0)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}

	// Clamp partition into the legal range. A user (or a stale write)
	// could set partition > replicas-1 during a scale-down — without this,
	// the state machine needs N reconciles to walk it back.
	if currentPartition > replicas-1 {
		p := replicas - 1
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// Recover from a misconfigured STS (partition missing or 0 at the very
	// start of a rollout). Pin to replicas-1 so only the top pod rolls.
	if currentPartition == 0 && sts.Status.UpdatedReplicas == 0 {
		p := replicas - 1
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// At partition N, pods with ordinal >= N are on (or rolling to) the new
	// revision. Wait for them all to finish before touching the next pod.
	expectedUpdated := replicas - currentPartition
	if sts.Status.UpdatedReplicas < expectedUpdated {
		p := currentPartition
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}

	// All pods at or above the partition are updated. Two gates before
	// advancing, and both need to hold:
	//   (a) a leader is present RIGHT NOW; and
	//   (b) the term has not changed since we last looked — a just-
	//       elected leader (term bumped) could be about to lose quorum
	//       again, so we wait one more reconcile cycle to confirm it
	//       stuck.
	// The two gates together give us "a stable leader" in a way that
	// mere presence doesn't — important during cascading failures.
	if !leader.HasLeader() {
		p := currentPartition
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForLeader}
	}
	// Only enforce term-stability when the coordinator reports a non-zero
	// term AND we have a prior observation — otherwise we'd block forever
	// on the very first reconcile (observedTerm=0 by default).
	if leader.CurrentTerm != 0 && observedTerm != 0 && leader.CurrentTerm != observedTerm {
		p := currentPartition
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForTerm}
	}

	// Advance one pod down. When we hit -1, it means the rollout is on the
	// last pod (partition==0 during the final update) or fully complete.
	next := currentPartition - 1
	if next < 0 {
		// partition already 0 and the final pod is still updating —
		// updateRevision != currentRevision per check 1. Keep partition at 0
		// and wait for the StatefulSet controller to finish.
		p := int32(0)
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}
	return RolloutStep{Partition: &next, Done: false, Reason: ReasonAdvancing}
}
