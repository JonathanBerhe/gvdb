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

// membership is a terse helper that builds a RaftMembership from a
// list of (node_id, is_learner) pairs plus the current leader id.
func membership(leaderID int32, members ...struct {
	id        uint32
	isLearner bool
}) gvdbclient.RaftMembership {
	out := gvdbclient.RaftMembership{
		CurrentLeaderID: leaderID,
		Members:         make([]gvdbclient.RaftMember, 0, len(members)),
	}
	for _, m := range members {
		out.Members = append(out.Members, gvdbclient.RaftMember{
			NodeID:    m.id,
			IsLearner: m.isLearner,
		})
	}
	return out
}

func healthyMembers(ids ...uint32) []struct {
	id        uint32
	isLearner bool
} {
	out := make([]struct {
		id        uint32
		isLearner bool
	}, 0, len(ids))
	for _, id := range ids {
		out = append(out, struct {
			id        uint32
			isLearner bool
		}{id: id, isLearner: false})
	}
	return out
}

func TestCoordinatorScale_Stable(t *testing.T) {
	// Membership matches spec exactly — no ghosts, no action.
	m := membership(1, healthyMembers(1, 2, 3)...)
	step := desiredCoordinatorScaleStep(3, m, true)
	if step.Action != CoordinatorScaleStable || !step.Done {
		t.Fatalf("expected Stable+Done, got %+v", step)
	}
}

func TestCoordinatorScale_WaitingForRollout(t *testing.T) {
	// A coordinator rollout is in flight — defer regardless of state.
	m := membership(1, healthyMembers(1, 2, 3, 4, 5)...)
	step := desiredCoordinatorScaleStep(3, m, false)
	if step.Action != CoordinatorScaleWaitingForRollout {
		t.Fatalf("expected WaitingForRollout, got %+v", step)
	}
	if step.Done {
		t.Fatalf("WaitingForRollout must not mark Done=true")
	}
}

func TestCoordinatorScale_WaitingForMembership(t *testing.T) {
	// Empty membership — operator lost visibility; don't act.
	m := gvdbclient.RaftMembership{}
	step := desiredCoordinatorScaleStep(3, m, true)
	if step.Action != CoordinatorScaleWaitingForMembership {
		t.Fatalf("expected WaitingForMembership, got %+v", step)
	}
}

func TestCoordinatorScale_SingleGhostLeaderHealthy(t *testing.T) {
	// 5-node membership, spec=3, leader=1 (healthy). Ghosts: 4, 5.
	// Expect RemovePeer on smallest ghost (4) first.
	m := membership(1, healthyMembers(1, 2, 3, 4, 5)...)
	step := desiredCoordinatorScaleStep(3, m, true)
	if step.Action != CoordinatorScaleRemovePeer {
		t.Fatalf("expected RemovePeer, got %+v", step)
	}
	if step.TargetNodeID != 4 {
		t.Fatalf("expected smallest ghost (4), got %d", step.TargetNodeID)
	}
	if step.Reason != ReasonRemovingGhostPeer {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestCoordinatorScale_LeaderIsGhostTransferFirst(t *testing.T) {
	// Leader=4 is a ghost; must TransferLeadership to the smallest
	// healthy non-learner member (1) before any RemovePeer.
	m := membership(4, healthyMembers(1, 2, 3, 4, 5)...)
	step := desiredCoordinatorScaleStep(3, m, true)
	if step.Action != CoordinatorScaleTransferLeadership {
		t.Fatalf("expected TransferLeadership, got %+v", step)
	}
	if step.TargetNodeID != 1 {
		t.Fatalf("expected smallest healthy (1), got %d", step.TargetNodeID)
	}
	if step.Reason != ReasonTransferringLeadership {
		t.Fatalf("reason %q", step.Reason)
	}
}

func TestCoordinatorScale_LeaderIsGhostAllOthersLearners(t *testing.T) {
	// Degenerate: leader=4 is ghost AND only remaining members ≤ spec
	// are learners still catching up. Must wait, not transfer to a
	// learner (NuRaft would reject or the cluster would lose quorum).
	m := membership(4,
		struct {
			id        uint32
			isLearner bool
		}{id: 1, isLearner: true},
		struct {
			id        uint32
			isLearner bool
		}{id: 2, isLearner: true},
		struct {
			id        uint32
			isLearner bool
		}{id: 3, isLearner: true},
		struct {
			id        uint32
			isLearner bool
		}{id: 4, isLearner: false},
	)
	step := desiredCoordinatorScaleStep(3, m, true)
	if step.Action != CoordinatorScaleWaitingForSuccessor {
		t.Fatalf("expected WaitingForSuccessor, got %+v", step)
	}
}

func TestCoordinatorScale_WalkThrough5to3(t *testing.T) {
	// Scale-down 5 → 3. Leader=1 is healthy.
	// Reconcile N: RemovePeer(4)  (smallest ghost)
	// Reconcile N+1 (after node 4 removed): RemovePeer(5)
	// Reconcile N+2: Stable.
	round1 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3, 4, 5)...), true)
	if round1.Action != CoordinatorScaleRemovePeer || round1.TargetNodeID != 4 {
		t.Fatalf("round1: %+v", round1)
	}

	round2 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3, 5)...), true)
	if round2.Action != CoordinatorScaleRemovePeer || round2.TargetNodeID != 5 {
		t.Fatalf("round2: %+v", round2)
	}

	round3 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3)...), true)
	if round3.Action != CoordinatorScaleStable || !round3.Done {
		t.Fatalf("round3: expected Stable+Done, got %+v", round3)
	}
}

func TestCoordinatorScale_WalkThroughLeaderIsGhost(t *testing.T) {
	// Scale-down 5 → 3 with leader=5 (needs to move first).
	// Reconcile N: TransferLeadership(1).
	// Reconcile N+1 (leader now 1, node 5 still in config): RemovePeer(4).
	// Reconcile N+2: RemovePeer(5).
	// Reconcile N+3: Stable.
	round1 := desiredCoordinatorScaleStep(3,
		membership(5, healthyMembers(1, 2, 3, 4, 5)...), true)
	if round1.Action != CoordinatorScaleTransferLeadership || round1.TargetNodeID != 1 {
		t.Fatalf("round1: %+v", round1)
	}

	round2 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3, 4, 5)...), true)
	if round2.Action != CoordinatorScaleRemovePeer || round2.TargetNodeID != 4 {
		t.Fatalf("round2: %+v", round2)
	}

	round3 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3, 5)...), true)
	if round3.Action != CoordinatorScaleRemovePeer || round3.TargetNodeID != 5 {
		t.Fatalf("round3: %+v", round3)
	}

	round4 := desiredCoordinatorScaleStep(3,
		membership(1, healthyMembers(1, 2, 3)...), true)
	if round4.Action != CoordinatorScaleStable || !round4.Done {
		t.Fatalf("round4: %+v", round4)
	}
}

func TestCoordinatorScale_SingleReplicaNoGhosts(t *testing.T) {
	// 1-replica cluster. No scale reconciliation ever needed.
	m := membership(1, healthyMembers(1)...)
	step := desiredCoordinatorScaleStep(1, m, true)
	if step.Action != CoordinatorScaleStable || !step.Done {
		t.Fatalf("expected Stable, got %+v", step)
	}
}

func TestCoordinatorScale_GhostsPresentButFewerThanSpec(t *testing.T) {
	// Scenario: spec=5 but only {1,2,3,8} are in cluster_config.
	// Node 8 is a ghost from a past scale-up. Node ids 4,5 are
	// legitimately missing (never joined). Ghost detection is based on
	// "> specReplicas", so 8 should be removed even though 4,5 don't
	// exist yet. New pods 4,5 will come up via 1.7b auto-join on the
	// next reconcile.
	m := membership(1,
		struct {
			id        uint32
			isLearner bool
		}{id: 1, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 2, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 3, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 8, isLearner: false},
	)
	step := desiredCoordinatorScaleStep(5, m, true)
	if step.Action != CoordinatorScaleRemovePeer || step.TargetNodeID != 8 {
		t.Fatalf("expected RemovePeer(8), got %+v", step)
	}
}

func TestCoordinatorScale_ScaleUpNoGhosts(t *testing.T) {
	// Scale-up 3 → 5: membership shows current 3 nodes + two joining
	// learners. No node_id > specReplicas → no ghosts → Stable. Join
	// progression is driven by the coordinator binary (1.7b), not this
	// reconciler.
	m := membership(1,
		struct {
			id        uint32
			isLearner bool
		}{id: 1, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 2, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 3, isLearner: false},
		struct {
			id        uint32
			isLearner bool
		}{id: 4, isLearner: true},
		struct {
			id        uint32
			isLearner bool
		}{id: 5, isLearner: true},
	)
	step := desiredCoordinatorScaleStep(5, m, true)
	if step.Action != CoordinatorScaleStable {
		t.Fatalf("expected Stable, got %+v", step)
	}
}
