/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	appsv1 "k8s.io/api/apps/v1"
)

// QueryNodeRolloutStep is the desired next state of a query-node
// StatefulSet's rolling-update partition plus whether the rollout is
// complete. Mirrors RolloutStep / DataNodeRolloutStep; query-nodes need no
// Message field because the state machine has no per-shard diagnostics.
type QueryNodeRolloutStep struct {
	Partition *int32
	Done      bool
	Reason    string
}

// ReasonWaitingForPriorRollouts holds the query-node partition while either
// the coordinator or the data-node StatefulSet is mid-rollout. Query-nodes
// have no hard dependency on either (they hold no durable state and reads
// already fail over to Ready replicas via prefer_routable_replica in
// RouteQuery — see src/network/internal_service.cpp:618-652). The gate
// exists purely for single-phase rollout UX: users see conditions flip in
// order (coordinator → data-node → query-node) rather than all at once
// during a full-cluster image bump.
const ReasonWaitingForPriorRollouts = "WaitingForPriorRollouts"

// desiredQueryNodePartition is a pure function computing the next rollout
// step for a query-node StatefulSet. Query-nodes are stateless — they hold
// only an in-memory segment cache loaded on demand (src/cluster/query_node.cpp)
// and the coordinator never assigns them shards (src/cluster/coordinator.cpp)
// — so the state machine has zero safety gates beyond "don't roll all pods
// simultaneously". Partition=replicas-1 already guarantees that.
//
// Structurally identical to the coordinator's desiredPartition() mechanical
// steps (single-replica pass-through, stable-park, clamp, first-detection
// pin, wait-for-pod-catchup, advance) but with no leader/term gates.
//
// Ownership: the rollout path is the sole writer of
// spec.updateStrategy.rollingUpdate.partition on the query-node StatefulSet.
// The render layer intentionally does NOT set partition — see
// operator/internal/render/querynode.go.
func desiredQueryNodePartition(
	sts *appsv1.StatefulSet,
	priorRolloutsReady bool,
) QueryNodeRolloutStep {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	// Single-replica: no parallel-roll hazard, render leaves partition unset.
	if replicas <= 1 {
		return QueryNodeRolloutStep{Partition: nil, Done: true, Reason: ReasonStable}
	}

	// Stable: park at replicas-1 so the NEXT rollout starts safely.
	if sts.Status.UpdateRevision == "" ||
		sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		parked := replicas - 1
		return QueryNodeRolloutStep{Partition: &parked, Done: true, Reason: ReasonStable}
	}

	currentPartition := int32(0)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}

	// Clamp into legal range (stale write or scale-down race).
	if currentPartition > replicas-1 {
		p := replicas - 1
		return QueryNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// Pin when rollout just started but partition was left at 0.
	if currentPartition == 0 && sts.Status.UpdatedReplicas == 0 {
		p := replicas - 1
		return QueryNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonPinningForRollout}
	}

	// Wait for already-rolling pods to finish before touching the next one.
	expectedUpdated := replicas - currentPartition
	if sts.Status.UpdatedReplicas < expectedUpdated {
		p := currentPartition
		return QueryNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}

	// Sequencing gate: keep the partition pinned while either of the prior
	// rollouts (coordinator or data-node) is still in flight. The mechanical
	// state above is allowed to run — we still want to park the partition
	// on a stable STS — but we never *advance* partition during a prior
	// rollout, so the UX stays linear.
	if !priorRolloutsReady {
		p := currentPartition
		return QueryNodeRolloutStep{
			Partition: &p, Done: false, Reason: ReasonWaitingForPriorRollouts,
		}
	}

	// All gates passed — advance one pod down.
	next := currentPartition - 1
	if next < 0 {
		p := int32(0)
		return QueryNodeRolloutStep{Partition: &p, Done: false, Reason: ReasonWaitingForPod}
	}
	return QueryNodeRolloutStep{Partition: &next, Done: false, Reason: ReasonAdvancing}
}
