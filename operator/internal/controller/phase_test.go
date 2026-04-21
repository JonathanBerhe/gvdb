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

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestComputePhase covers the three phase transitions: Pending (nothing
// ready), Degraded (partial), Ready (all workloads at desired count).
func TestComputePhase(t *testing.T) {

	cases := []struct {
		name  string
		nc    gvdbv1alpha1.NodeCountStatus
		want  gvdbv1alpha1.GVDBClusterPhase
		avail metav1.ConditionStatus
	}{
		{
			name: "all-ready",
			nc: gvdbv1alpha1.NodeCountStatus{
				Coordinator: gvdbv1alpha1.WorkloadStatus{Desired: 3, Ready: 3},
				DataNode:    gvdbv1alpha1.WorkloadStatus{Desired: 2, Ready: 2},
				QueryNode:   gvdbv1alpha1.WorkloadStatus{Desired: 1, Ready: 1},
				Proxy:       gvdbv1alpha1.WorkloadStatus{Desired: 1, Ready: 1},
			},
			want:  gvdbv1alpha1.PhaseReady,
			avail: metav1.ConditionTrue,
		},
		{
			name: "partial",
			nc: gvdbv1alpha1.NodeCountStatus{
				Coordinator: gvdbv1alpha1.WorkloadStatus{Desired: 3, Ready: 2},
				DataNode:    gvdbv1alpha1.WorkloadStatus{Desired: 2, Ready: 2},
				QueryNode:   gvdbv1alpha1.WorkloadStatus{Desired: 1, Ready: 1},
				Proxy:       gvdbv1alpha1.WorkloadStatus{Desired: 1, Ready: 1},
			},
			want:  gvdbv1alpha1.PhaseDegraded,
			avail: metav1.ConditionFalse,
		},
		{
			name: "nothing-ready",
			nc: gvdbv1alpha1.NodeCountStatus{
				Coordinator: gvdbv1alpha1.WorkloadStatus{Desired: 3},
				DataNode:    gvdbv1alpha1.WorkloadStatus{Desired: 2},
				QueryNode:   gvdbv1alpha1.WorkloadStatus{Desired: 1},
				Proxy:       gvdbv1alpha1.WorkloadStatus{Desired: 1},
			},
			want:  gvdbv1alpha1.PhasePending,
			avail: metav1.ConditionFalse,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phase, conds := computePhase(tc.nc)
			if phase != tc.want {
				t.Fatalf("phase: got %q, want %q", phase, tc.want)
			}
			for _, c := range conds {
				if c.Type == gvdbv1alpha1.ConditionAvailable && c.Status != tc.avail {
					t.Fatalf("Available status: got %q, want %q", c.Status, tc.avail)
				}
			}
		})
	}
}
