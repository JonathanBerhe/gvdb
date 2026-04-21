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
//  1. If updateRevision == currentRevision, the StatefulSet is stable →
//     clear partition.
//  2. Otherwise a rollout is in flight. If partition is unset (or 0) at
//     the START of a rollout, clamp it to replicas-1 so only the
//     highest-ordinal pod updates first.
//  3. Wait until updatedReplicas reaches what the current partition
//     permits (replicas - partition).
//  4. Once enough pods have caught up, gate on leader: don't decrement
//     partition unless a Raft leader is currently elected.
//  5. When ready to advance, decrement partition by 1. At partition==0,
//     the final pod starts updating; once updateRevision==currentRevision
//     we unpin.
//
// The function takes pointers for testability (tests can construct a
// StatefulSet literal without reaching for the Scheme or deep-copies).
func desiredPartition(sts *appsv1.StatefulSet, leader gvdbclient.LeaderInfo) RolloutStep {
	// No rollout in progress — clear any partition we left behind.
	if sts.Status.UpdateRevision == "" ||
		sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		return RolloutStep{Partition: nil, Done: true, Reason: ReasonStable}
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	currentPartition := int32(0)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}

	// Start-of-rollout: default partition is 0, meaning "update all pods".
	// Pin to replicas-1 so only the highest-ordinal pod rolls first.
	// We detect this by: rollout is in flight (passed check 1) but no pods
	// are yet on the new revision AND partition is at its default.
	if currentPartition == 0 && sts.Status.UpdatedReplicas == 0 {
		p := replicas - 1
		if p < 0 {
			p = 0
		}
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
