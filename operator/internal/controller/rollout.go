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
	ReasonStable           = "Stable"
	ReasonPinningForRollout = "PinningForRollout"
	ReasonWaitingForPod    = "WaitingForPod"
	ReasonWaitingForLeader = "WaitingForLeader"
	ReasonAdvancing        = "Advancing"
)

// desiredPartition is a pure function computing the next rollout step for a
// coordinator StatefulSet given its live status and a leader-info probe.
//
// Strategy (roadmap 0b.6.C):
//
//  1. The render layer pre-pins partition to replicas-1 on every apply so
//     K8s never rolls multiple pods concurrently when a new revision ships
//     — we just need to decrement it pod-by-pod, each time verifying a
//     Raft leader is present.
//  2. If updateRevision == currentRevision, the StatefulSet is stable →
//     ensure partition is parked at replicas-1, ready for the next
//     rollout.
//  3. Otherwise wait until updatedReplicas reaches what the current
//     partition permits (replicas - partition), then gate on leader
//     before decrementing.
//  4. When ready to advance, decrement partition by 1. At partition==0,
//     the final pod starts updating; we keep partition at 0 until
//     updateRevision==currentRevision, then snap it back up to
//     replicas-1 for the next rollout.
//
// The function takes pointers for testability (tests can construct a
// StatefulSet literal without reaching for the Scheme or deep-copies).
func desiredPartition(sts *appsv1.StatefulSet, leader gvdbclient.LeaderInfo) RolloutStep {
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

	// All pods at or above the partition are updated. Gate advancing on a
	// Raft leader being elected: don't drain the next pod until quorum
	// is verified on the current state.
	if !leader.HasLeader() {
		p := currentPartition
		return RolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForLeader}
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
