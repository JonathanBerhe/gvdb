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

// CoordinatorScaleAction enumerates what the reconciler should do next
// to converge Raft cluster_config toward spec.coordinator.replicas.
type CoordinatorScaleAction string

const (
	// CoordinatorScaleStable: membership matches the declared replica
	// count; nothing to do this reconcile.
	CoordinatorScaleStable CoordinatorScaleAction = "Stable"
	// CoordinatorScaleRemovePeer: call RemovePeer RPC on the leader for
	// TargetNodeID. Idempotent (1.7b MapNuRaftCode treats
	// SERVER_NOT_FOUND / SERVER_IS_LEAVING as OK).
	CoordinatorScaleRemovePeer CoordinatorScaleAction = "RemovePeer"
	// CoordinatorScaleTransferLeadership: the current leader is a ghost
	// peer (node_id > specReplicas). Call TransferLeadership to
	// TargetNodeID (the smallest healthy non-learner member) first, then
	// the next reconcile will observe the new leader and proceed to
	// RemovePeer.
	CoordinatorScaleTransferLeadership CoordinatorScaleAction = "TransferLeadership"
	// CoordinatorScaleWaitingForRollout: a coordinator rollout is in
	// flight; defer scale reconciliation to avoid racing with the
	// partition walk.
	CoordinatorScaleWaitingForRollout CoordinatorScaleAction = "WaitingForRollout"
	// CoordinatorScaleWaitingForSuccessor: the leader is a ghost but no
	// non-learner candidate is ready to take over. Wait for a follower
	// to finish catching up before attempting transfer.
	CoordinatorScaleWaitingForSuccessor CoordinatorScaleAction = "WaitingForSuccessor"
	// CoordinatorScaleWaitingForMembership: GetRaftMembership returned
	// empty — either we couldn't reach any coordinator or the cluster
	// is mid-bootstrap. Don't act without visibility.
	CoordinatorScaleWaitingForMembership CoordinatorScaleAction = "WaitingForMembership"
)

// Reason constants surfaced on ConditionCoordinatorScaleReady. Share
// ReasonStable with the rollout conditions (defined in rollout.go) to
// keep reason strings consistent across the operator's status surface.
const (
	ReasonRemovingGhostPeer         = "RemovingGhostPeer"
	ReasonTransferringLeadership    = "TransferringLeadership"
	ReasonWaitingForScaleSuccessor  = "WaitingForSuccessor"
	ReasonWaitingForMembership      = "WaitingForMembership"
	ReasonScaleWaitingForRollout    = "WaitingForCoordinatorRollout"
)

// CoordinatorScaleStep is the single next action the reconciler should
// take based on the pure-function inspection of Raft membership vs.
// spec.coordinator.replicas.
type CoordinatorScaleStep struct {
	Action       CoordinatorScaleAction
	TargetNodeID int32
	Reason       string
	Message      string
	// Done=true only for CoordinatorScaleStable; the condition flips to
	// True at that point and requeue drops to the idle cadence.
	Done bool
}

// desiredCoordinatorScaleStep inspects Raft membership vs. declared
// replicas and returns the single next action to converge them. Pure:
// no side effects, trivially unit-testable. Roadmap 1.8.
//
// Decision order (explicit to keep concurrent scale-down scenarios
// predictable):
//
//  1. Rollout-in-flight guard — never mutate membership during a pod
//     rollout; partition walk already moves the leader in controlled
//     steps and a racing TransferLeadership would thrash.
//  2. No membership observed → WaitingForMembership. Could be bootstrap
//     or total coordinator outage; either way, don't act.
//  3. No ghosts (every member has node_id ≤ specReplicas) → Stable.
//  4. Leader-is-ghost → pick the smallest healthy non-learner successor
//     and TransferLeadership. If none exists, WaitingForSuccessor
//     (learners are still catching up).
//  5. Otherwise → RemovePeer on the smallest ghost. Converges in at
//     most N reconciles for N ghosts; deterministic ordering.
func desiredCoordinatorScaleStep(
	specReplicas int32,
	membership gvdbclient.RaftMembership,
	coordinatorRolloutReady bool,
) CoordinatorScaleStep {
	if !coordinatorRolloutReady {
		return CoordinatorScaleStep{
			Action: CoordinatorScaleWaitingForRollout,
			Reason: ReasonScaleWaitingForRollout,
		}
	}

	if len(membership.Members) == 0 {
		return CoordinatorScaleStep{
			Action: CoordinatorScaleWaitingForMembership,
			Reason: ReasonWaitingForMembership,
			Message: "GetRaftMembership returned no members; " +
				"coordinator pool may be unreachable",
		}
	}

	// Ghost = any member whose node_id is beyond the declared replica
	// range. Node ids are 1-indexed (ordinal + 1 convention).
	var ghosts []int32
	smallestHealthySuccessor := int32(0) // min { node_id ≤ specReplicas, !is_learner }
	for _, m := range membership.Members {
		nodeID := int32(m.NodeID)
		if nodeID > specReplicas {
			ghosts = append(ghosts, nodeID)
			continue
		}
		if !m.IsLearner {
			if smallestHealthySuccessor == 0 || nodeID < smallestHealthySuccessor {
				smallestHealthySuccessor = nodeID
			}
		}
	}

	if len(ghosts) == 0 {
		return CoordinatorScaleStep{
			Action: CoordinatorScaleStable,
			Reason: ReasonStable,
			Done:   true,
		}
	}

	// Is the current leader one of the ghosts?
	leaderIsGhost := false
	for _, g := range ghosts {
		if g == membership.CurrentLeaderID {
			leaderIsGhost = true
			break
		}
	}
	if leaderIsGhost {
		if smallestHealthySuccessor == 0 {
			return CoordinatorScaleStep{
				Action: CoordinatorScaleWaitingForSuccessor,
				Reason: ReasonWaitingForScaleSuccessor,
				Message: fmt.Sprintf(
					"leader node_id=%d is a ghost but no healthy non-learner "+
						"candidate ≤ %d is ready",
					membership.CurrentLeaderID, specReplicas),
			}
		}
		return CoordinatorScaleStep{
			Action:       CoordinatorScaleTransferLeadership,
			TargetNodeID: smallestHealthySuccessor,
			Reason:       ReasonTransferringLeadership,
			Message: fmt.Sprintf(
				"leader node_id=%d is scheduled for removal; "+
					"transferring leadership to node_id=%d",
				membership.CurrentLeaderID, smallestHealthySuccessor),
		}
	}

	// Leader is healthy; remove the smallest ghost first (stable order
	// across reconciles makes concurrent observers see a predictable
	// sequence, and NuRaft serializes add_srv/remove_srv so there is no
	// benefit to batching).
	smallestGhost := ghosts[0]
	for _, g := range ghosts {
		if g < smallestGhost {
			smallestGhost = g
		}
	}
	return CoordinatorScaleStep{
		Action:       CoordinatorScaleRemovePeer,
		TargetNodeID: smallestGhost,
		Reason:       ReasonRemovingGhostPeer,
		Message: fmt.Sprintf(
			"removing ghost peer node_id=%d (spec.coordinator.replicas=%d)",
			smallestGhost, specReplicas),
	}
}
