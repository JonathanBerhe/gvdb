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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	"gvdb/operator/internal/gvdbclient"
	"gvdb/operator/internal/render"
)

// finalizerName is attached to GVDBCluster so the operator gets a chance to
// run teardown logic before garbage collection removes the owned resources.
// v1alpha1's teardown is minimal (ownerReferences handle cascade delete);
// the finalizer exists so future logic (e.g. 0b.6.C quorum-aware drain) can
// hook in without a v1beta1 schema bump.
const finalizerName = "gvdb.io/finalizer"

// GVDBClusterReconciler reconciles a GVDBCluster object.
type GVDBClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=get;list;watch;create;update;patch;delete

// Reconcile brings the cluster state in line with the GVDBCluster spec.
func (r *GVDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster gvdbv1alpha1.GVDBCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion path: ownerReferences handle cascade-delete; we just release
	// the finalizer. Future hooks (drain, backup-before-delete) plug in here.
	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, finalizerName) {
			controllerutil.RemoveFinalizer(&cluster, finalizerName)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is set before we create any owned resources, so the
	// user can't delete-and-lose the SA / PDB cleanup hook window.
	if !controllerutil.ContainsFinalizer(&cluster, finalizerName) {
		controllerutil.AddFinalizer(&cluster, finalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Render desired state.
	objs, err := render.All(&cluster)
	if err != nil {
		// Render-time errors are configuration problems — surface as Failed.
		r.markCondition(&cluster, gvdbv1alpha1.ConditionAvailable, metav1.ConditionFalse, "RenderFailed", err.Error())
		cluster.Status.Phase = gvdbv1alpha1.PhaseFailed
		return ctrl.Result{}, r.writeStatus(ctx, &cluster)
	}

	// Apply every rendered object. OwnerReferences make `kubectl delete
	// gvdbcluster` cascade to the managed resources.
	for _, obj := range objs {
		if err := r.applyObject(ctx, &cluster, obj); err != nil {
			log.Error(err, "apply object failed",
				"kind", obj.GetObjectKind().GroupVersionKind().Kind,
				"name", obj.GetName(),
			)
			r.markCondition(&cluster, gvdbv1alpha1.ConditionAvailable, metav1.ConditionFalse, "ApplyFailed", err.Error())
			return ctrl.Result{}, r.writeStatus(ctx, &cluster)
		}
	}

	// Collect status from K8s + GVDB gRPC.
	nodeCounts := r.collectNodeCounts(ctx, &cluster)
	cluster.Status.NodeCounts = nodeCounts

	stats, statsErr := r.fetchGVDBStats(ctx, &cluster)
	if statsErr == nil {
		cluster.Status.CollectionCount = stats.CollectionCount
		cluster.Status.TotalVectors = stats.TotalVectors
	} else {
		// gRPC failures during startup are expected; don't flag the cluster
		// as degraded just because status polling transiently failed.
		log.V(1).Info("fetch GVDB stats failed; will retry", "err", statsErr)
	}

	phase, conditions := computePhase(&cluster, nodeCounts)
	cluster.Status.Phase = phase
	for _, cond := range conditions {
		r.markConditionObj(&cluster, cond)
	}
	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.writeStatus(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue so status reflects ongoing convergence (replica count churn,
	// RPC-derived totals) without waiting for a spec-change event.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// applyObject stamps the cluster as owner and creates-or-updates the object.
// We use CreateOrUpdate because Server-Side Apply requires field-manager
// tuning that isn't worth the complexity for v1alpha1.
func (r *GVDBClusterReconciler) applyObject(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, obj client.Object,
) error {
	// Cluster-scoped objects (PriorityClass) cannot carry namespaced
	// OwnerReferences. Apply them directly; cleanup on delete happens via
	// a separate finalizer pass (future work — 0b.6.C).
	isClusterScoped := obj.GetNamespace() == ""

	if !isClusterScoped {
		if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
			return fmt.Errorf("set owner ref: %w", err)
		}
	}

	desired := obj.DeepCopyObject().(client.Object)
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	current := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Preserve server-assigned fields (resourceVersion) on the desired object
	// before update — without this the update races.
	desired.SetResourceVersion(current.GetResourceVersion())
	return r.Update(ctx, desired)
}

// collectNodeCounts reads ready-replica counts from the owned StatefulSets /
// Deployment. Missing objects (during initial create) report 0 ready — the
// phase computation then surfaces "Progressing" rather than "Ready".
func (r *GVDBClusterReconciler) collectNodeCounts(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) gvdbv1alpha1.NodeCountStatus {
	nc := gvdbv1alpha1.NodeCountStatus{
		Coordinator: gvdbv1alpha1.WorkloadStatus{Desired: coordinatorReplicas(cluster)},
		DataNode:    gvdbv1alpha1.WorkloadStatus{Desired: dataNodeReplicas(cluster)},
		QueryNode:   gvdbv1alpha1.WorkloadStatus{Desired: queryNodeReplicas(cluster)},
		Proxy:       gvdbv1alpha1.WorkloadStatus{Desired: proxyReplicas(cluster)},
	}
	nc.Coordinator.Ready = r.readyReplicasSTS(ctx, cluster, render.CoordinatorComponent)
	nc.DataNode.Ready = r.readyReplicasSTS(ctx, cluster, render.DataNodeComponent)
	nc.QueryNode.Ready = r.readyReplicasSTS(ctx, cluster, render.QueryNodeComponent)
	nc.Proxy.Ready = r.readyReplicasDeployment(ctx, cluster, render.ProxyComponent)
	return nc
}

func (r *GVDBClusterReconciler) readyReplicasSTS(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, c render.Component,
) int32 {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, c),
	}, &sts); err != nil {
		return 0
	}
	return sts.Status.ReadyReplicas
}

func (r *GVDBClusterReconciler) readyReplicasDeployment(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, c render.Component,
) int32 {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, c),
	}, &dep); err != nil {
		return 0
	}
	return dep.Status.ReadyReplicas
}

// fetchGVDBStats connects to the proxy Service (stateless gateway) and asks
// for cluster-wide totals. Using the proxy instead of a coordinator pod
// avoids Raft-leader redirection logic in the operator.
func (r *GVDBClusterReconciler) fetchGVDBStats(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) (gvdbclient.Stats, error) {
	port := cluster.Spec.Proxy.Service.Port
	if port == 0 {
		port = render.ProxyGRPCPort
	}
	target := fmt.Sprintf("%s.%s.svc.%s:%d",
		render.WorkloadName(cluster, render.ProxyComponent),
		cluster.Namespace,
		render.ClusterDomain(cluster),
		port,
	)
	c, err := gvdbclient.Dial(ctx, target)
	if err != nil {
		return gvdbclient.Stats{}, err
	}
	defer c.Close()
	return c.FetchStats(ctx)
}

// writeStatus PATCHes the status subresource only.
func (r *GVDBClusterReconciler) writeStatus(ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster) error {
	return r.Status().Update(ctx, cluster)
}

// markCondition updates one status condition by type.
func (r *GVDBClusterReconciler) markCondition(
	cluster *gvdbv1alpha1.GVDBCluster, condType string, status metav1.ConditionStatus, reason, msg string,
) {
	r.markConditionObj(cluster, metav1.Condition{
		Type:    condType,
		Status:  status,
		Reason:  reason,
		Message: msg,
	})
}

func (r *GVDBClusterReconciler) markConditionObj(
	cluster *gvdbv1alpha1.GVDBCluster, cond metav1.Condition,
) {
	if cond.LastTransitionTime.IsZero() {
		cond.LastTransitionTime = metav1.Now()
	}
	// Replace by type.
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == cond.Type {
			cluster.Status.Conditions[i] = cond
			return
		}
	}
	cluster.Status.Conditions = append(cluster.Status.Conditions, cond)
}

// computePhase derives .status.phase and the standard condition set from the
// nodeCounts view. Returns conditions with empty LastTransitionTime so the
// caller can stamp a consistent timestamp.
func computePhase(
	cluster *gvdbv1alpha1.GVDBCluster, nc gvdbv1alpha1.NodeCountStatus,
) (gvdbv1alpha1.GVDBClusterPhase, []metav1.Condition) {
	allReady := nc.Coordinator.Ready >= nc.Coordinator.Desired &&
		nc.DataNode.Ready >= nc.DataNode.Desired &&
		nc.QueryNode.Ready >= nc.QueryNode.Desired &&
		nc.Proxy.Ready >= nc.Proxy.Desired

	partialReady := nc.Coordinator.Ready > 0 || nc.DataNode.Ready > 0 ||
		nc.QueryNode.Ready > 0 || nc.Proxy.Ready > 0

	switch {
	case allReady:
		return gvdbv1alpha1.PhaseReady, []metav1.Condition{
			{Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionTrue, Reason: "AllReplicasReady", Message: "all workloads report Ready"},
			{Type: gvdbv1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: "Stable", Message: "cluster at desired replica count"},
			{Type: gvdbv1alpha1.ConditionDegraded, Status: metav1.ConditionFalse, Reason: "Healthy", Message: "no workloads under-replicated"},
		}
	case partialReady:
		return gvdbv1alpha1.PhaseDegraded, []metav1.Condition{
			{Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse, Reason: "UnderReplicated", Message: "at least one workload has fewer ready replicas than desired"},
			{Type: gvdbv1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: "Reconciling", Message: "waiting for workloads to reach desired replica count"},
			{Type: gvdbv1alpha1.ConditionDegraded, Status: metav1.ConditionTrue, Reason: "UnderReplicated", Message: "at least one workload has fewer ready replicas than desired"},
		}
	}
	_ = cluster
	return gvdbv1alpha1.PhasePending, []metav1.Condition{
		{Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse, Reason: "NotReady", Message: "no workload replicas are ready yet"},
		{Type: gvdbv1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: "Creating", Message: "workloads are being created"},
		{Type: gvdbv1alpha1.ConditionDegraded, Status: metav1.ConditionFalse, Reason: "Initializing", Message: "cluster has not yet reached any ready replica"},
	}
}

// replica-with-default helpers — mirror the render-layer defaults so status
// shows the effective desired count even when the CR leaves fields blank.
func coordinatorReplicas(c *gvdbv1alpha1.GVDBCluster) int32 {
	if c.Spec.Coordinator.Replicas == 0 {
		return 1
	}
	return c.Spec.Coordinator.Replicas
}

func dataNodeReplicas(c *gvdbv1alpha1.GVDBCluster) int32 {
	if c.Spec.DataNode.Replicas == 0 {
		return 2
	}
	return c.Spec.DataNode.Replicas
}

func queryNodeReplicas(c *gvdbv1alpha1.GVDBCluster) int32 {
	if c.Spec.QueryNode.Replicas == 0 {
		return 1
	}
	return c.Spec.QueryNode.Replicas
}

func proxyReplicas(c *gvdbv1alpha1.GVDBCluster) int32 {
	if c.Spec.Proxy.Replicas == 0 {
		return 1
	}
	return c.Spec.Proxy.Replicas
}

// SetupWithManager wires the controller into the manager. We watch the
// managed resource kinds so external changes (e.g. a user scaling a
// StatefulSet directly) trigger a reconcile to re-enforce the spec.
func (r *GVDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gvdbv1alpha1.GVDBCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Named("gvdbcluster").
		Complete(r)
}
