/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	"gvdb/operator/internal/render"
)

// newProxyReconcilerFixture wires a fake client against the minimal scheme
// the reconciler needs (gvdbv1alpha1 + apps/v1) plus an in-memory GVDBCluster
// the reconciler writes condition updates onto. If `dep` is nil, no
// Deployment is pre-seeded, exercising the missing-deployment path.
func newProxyReconcilerFixture(t *testing.T, dep *appsv1.Deployment) (*GVDBClusterReconciler, *gvdbv1alpha1.GVDBCluster) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := gvdbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register gvdb scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("register apps scheme: %v", err)
	}
	cluster := &gvdbv1alpha1.GVDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "gvdb"},
	}
	objs := []runtime.Object{cluster}
	if dep != nil {
		objs = append(objs, dep)
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return &GVDBClusterReconciler{Client: cli, Scheme: scheme}, cluster
}

func proxyDeployment(cluster *gvdbv1alpha1.GVDBCluster, specReplicas, updated, ready, unavailable int32, generation, observed int64, conditions ...appsv1.DeploymentCondition) *appsv1.Deployment {
	r := specReplicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       render.WorkloadName(cluster, render.ProxyComponent),
			Namespace:  cluster.Namespace,
			Generation: generation,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &r,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration:  observed,
			Replicas:            specReplicas,
			UpdatedReplicas:     updated,
			ReadyReplicas:       ready,
			UnavailableReplicas: unavailable,
			Conditions:          conditions,
		},
	}
}

func TestReconcileProxyRolloutStatus_Missing(t *testing.T) {
	r, cluster := newProxyReconcilerFixture(t, nil)
	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil {
		t.Fatalf("expected condition to be set")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != ReasonProxyRolloutMissing {
		t.Fatalf("got status=%s reason=%s, want False/%s", cond.Status, cond.Reason, ReasonProxyRolloutMissing)
	}
}

func TestReconcileProxyRolloutStatus_Stable(t *testing.T) {
	// All four stability checks pass: ObservedGeneration up-to-date,
	// UpdatedReplicas == Replicas, ReadyReplicas == Replicas, Unavailable==0.
	r, cluster := newProxyReconcilerFixture(t, nil)
	dep := proxyDeployment(cluster, 3, 3, 3, 0, 2, 2)
	cli := fake.NewClientBuilder().WithScheme(r.Scheme).
		WithRuntimeObjects(cluster, dep).Build()
	r.Client = cli

	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonStable {
		t.Fatalf("expected Stable True, got %+v", cond)
	}
}

func TestReconcileProxyRolloutStatus_Progressing(t *testing.T) {
	// Mid-rollout: half of the replicas have been updated, others not.
	// No Progressing=False condition, so we report Progressing (not Stalled).
	r, cluster := newProxyReconcilerFixture(t, nil)
	dep := proxyDeployment(cluster, 3, 1, 2, 1, 3, 3,
		appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "ReplicaSetUpdated"},
	)
	cli := fake.NewClientBuilder().WithScheme(r.Scheme).
		WithRuntimeObjects(cluster, dep).Build()
	r.Client = cli

	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonProxyRolloutProgressing {
		t.Fatalf("expected Progressing False, got %+v", cond)
	}
	if cond.Message == "" {
		t.Fatalf("expected detail message with replica counts")
	}
}

func TestReconcileProxyRolloutStatus_Stalled(t *testing.T) {
	// Deployment controller has reported Progressing=False
	// (progressDeadlineSeconds exceeded) AND the cluster is not yet stable.
	r, cluster := newProxyReconcilerFixture(t, nil)
	dep := proxyDeployment(cluster, 3, 1, 1, 2, 3, 3,
		appsv1.DeploymentCondition{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded", Message: "ReplicaSet \"foo\" has timed out",
		},
	)
	cli := fake.NewClientBuilder().WithScheme(r.Scheme).
		WithRuntimeObjects(cluster, dep).Build()
	r.Client = cli

	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonProxyRolloutStalled {
		t.Fatalf("expected Stalled False, got %+v", cond)
	}
}

func TestReconcileProxyRolloutStatus_StableWinsOverLaggingProgressingFalse(t *testing.T) {
	// Rollback-recovery scenario: all replicas are Ready again, but
	// Progressing condition still reads False from the earlier stall.
	// Stability check runs FIRST so we report Stable — if this test fails
	// the ordering bug from code review has regressed.
	r, cluster := newProxyReconcilerFixture(t, nil)
	dep := proxyDeployment(cluster, 3, 3, 3, 0, 3, 3,
		appsv1.DeploymentCondition{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded", Message: "previously stalled but now healthy",
		},
	)
	cli := fake.NewClientBuilder().WithScheme(r.Scheme).
		WithRuntimeObjects(cluster, dep).Build()
	r.Client = cli

	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonStable {
		t.Fatalf("expected Stable True (stability wins over stale Progressing=False), got %+v", cond)
	}
}

func TestReconcileProxyRolloutStatus_StaleObservedGenerationBlocksStable(t *testing.T) {
	// Deployment just updated (Generation=5) but controller hasn't observed
	// it yet (ObservedGeneration=4). UpdatedReplicas/ReadyReplicas look
	// correct for the old generation, but we must NOT claim Stable because
	// the new pods haven't been scheduled yet.
	r, cluster := newProxyReconcilerFixture(t, nil)
	dep := proxyDeployment(cluster, 3, 3, 3, 0, 5, 4,
		appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetCreated"},
	)
	cli := fake.NewClientBuilder().WithScheme(r.Scheme).
		WithRuntimeObjects(cluster, dep).Build()
	r.Client = cli

	if _, err := r.reconcileProxyRolloutStatus(context.Background(), cluster); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cond := meta.FindStatusCondition(cluster.Status.Conditions, gvdbv1alpha1.ConditionProxyRolloutReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonProxyRolloutProgressing {
		t.Fatalf("expected Progressing False (observedGeneration lag), got %+v", cond)
	}
}
