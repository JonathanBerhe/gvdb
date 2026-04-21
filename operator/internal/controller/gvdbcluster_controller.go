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
	schedv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
// v1alpha1 teardown: delete cluster-scoped PriorityClasses (which cannot be
// cascade-deleted via namespaced OwnerReferences). Namespaced resources
// cascade via OwnerReferences alone.
const finalizerName = "gvdb.io/finalizer"

// fieldManager identifies the operator as the Server-Side-Apply field owner.
// Using a stable value keeps SSA ownership tracking predictable across
// reconciles and across operator-pod restarts.
const fieldManager = "gvdb-operator"

// GVDBClusterReconciler reconciles a GVDBCluster object.
type GVDBClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// DefaultImageTag is the GVDB core image tag used when a CR doesn't set
	// spec.image.tag. Set once at startup from the operator's own release
	// version (lockstep); not mutated afterwards.
	DefaultImageTag string

	// StatsPool caches long-lived gRPC clients so reconciler passes don't
	// re-dial every 30s.
	StatsPool *gvdbclient.Pool
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

	// Deletion: clean up cluster-scoped PriorityClasses that can't ride the
	// namespaced OwnerReferences cascade. Everything else cleans up for free.
	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, finalizerName) {
			if err := r.cleanupClusterScoped(ctx, &cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("cleanup cluster-scoped: %w", err)
			}
			controllerutil.RemoveFinalizer(&cluster, finalizerName)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is set before we create any owned cluster-scoped
	// resources, so the user can't delete-and-lose the cleanup window.
	if !controllerutil.ContainsFinalizer(&cluster, finalizerName) {
		controllerutil.AddFinalizer(&cluster, finalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Render desired state.
	opts := render.Options{DefaultImageTag: r.DefaultImageTag}
	objs, err := render.All(&cluster, opts)
	if err != nil {
		// Render-time errors are configuration problems — surface as Failed.
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse,
			Reason: "RenderFailed", Message: err.Error(),
		})
		cluster.Status.Phase = gvdbv1alpha1.PhaseFailed
		return ctrl.Result{}, r.Status().Update(ctx, &cluster)
	}

	// Apply every rendered object via Server-Side Apply. SSA preserves
	// server-assigned fields (Service ClusterIP, default GCs, etc.) that a
	// naive GET-then-UPDATE would clobber.
	for _, obj := range objs {
		if err := r.applyObject(ctx, &cluster, obj); err != nil {
			log.Error(err, "apply object failed",
				"kind", obj.GetObjectKind().GroupVersionKind().Kind,
				"name", obj.GetName(),
			)
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse,
				Reason: "ApplyFailed", Message: err.Error(),
			})
			return ctrl.Result{}, r.Status().Update(ctx, &cluster)
		}
	}

	// Collect status.
	nodeCounts := r.collectNodeCounts(ctx, &cluster)
	cluster.Status.NodeCounts = nodeCounts

	r.refreshStats(ctx, &cluster)

	phase, conditions := computePhase(nodeCounts)
	cluster.Status.Phase = phase
	for _, cond := range conditions {
		meta.SetStatusCondition(&cluster.Status.Conditions, cond)
	}
	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue so status reflects ongoing convergence without waiting for a
	// spec-change event.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// applyObject Server-Side-Applies the rendered object. SSA is idempotent
// and handles the immutable-field preservation footguns of GET-then-UPDATE
// (e.g. Service.spec.clusterIP) for free.
//
// Namespaced objects get a controller-reference OwnerReference so that
// `kubectl delete gvdbcluster` cascades. Cluster-scoped PriorityClasses are
// labeled with the cluster identity and torn down explicitly in the
// finalizer path.
func (r *GVDBClusterReconciler) applyObject(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, obj client.Object,
) error {
	if obj.GetNamespace() != "" {
		if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
			return fmt.Errorf("set owner ref: %w", err)
		}
	}
	return r.Patch(ctx, obj, client.Apply,
		client.FieldOwner(fieldManager),
		client.ForceOwnership,
	)
}

// cleanupClusterScoped deletes the cluster-scoped objects that can't ride
// the OwnerReference cascade. Today: PriorityClasses labeled with this CR's
// identity. List-by-label is idempotent and survives operator crashes
// mid-delete (stragglers get swept on the next reconcile).
func (r *GVDBClusterReconciler) cleanupClusterScoped(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) error {
	var pcs schedv1.PriorityClassList
	if err := r.List(ctx, &pcs, client.MatchingLabels(render.ClusterSelectorLabels(cluster))); err != nil {
		return fmt.Errorf("list PriorityClasses: %w", err)
	}
	for i := range pcs.Items {
		pc := &pcs.Items[i]
		if err := r.Delete(ctx, pc); err != nil && client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete PriorityClass %s: %w", pc.Name, err)
		}
	}
	return nil
}

// collectNodeCounts reads ready-replica counts from the owned StatefulSets /
// Deployment. Missing objects (during initial create) report 0 ready — the
// phase computation then surfaces "Progressing" rather than "Ready".
func (r *GVDBClusterReconciler) collectNodeCounts(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) gvdbv1alpha1.NodeCountStatus {
	return gvdbv1alpha1.NodeCountStatus{
		Coordinator: gvdbv1alpha1.WorkloadStatus{
			Desired: render.EffectiveReplicas(cluster, render.CoordinatorComponent),
			Ready:   r.readyReplicasSTS(ctx, cluster, render.CoordinatorComponent),
		},
		DataNode: gvdbv1alpha1.WorkloadStatus{
			Desired: render.EffectiveReplicas(cluster, render.DataNodeComponent),
			Ready:   r.readyReplicasSTS(ctx, cluster, render.DataNodeComponent),
		},
		QueryNode: gvdbv1alpha1.WorkloadStatus{
			Desired: render.EffectiveReplicas(cluster, render.QueryNodeComponent),
			Ready:   r.readyReplicasSTS(ctx, cluster, render.QueryNodeComponent),
		},
		Proxy: gvdbv1alpha1.WorkloadStatus{
			Desired: render.EffectiveReplicas(cluster, render.ProxyComponent),
			Ready:   r.readyReplicasDeployment(ctx, cluster, render.ProxyComponent),
		},
	}
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

// refreshStats pulls collectionCount + totalVectors from the proxy (stateless
// gateway, no leader-redirect gymnastics). Populates cluster.Status on
// success; on failure, keeps the last-known-good values and records a
// StatsAvailable=False condition so callers can distinguish "current" from
// "stale" without reading the reconciler's logs.
func (r *GVDBClusterReconciler) refreshStats(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) {
	log := logf.FromContext(ctx)

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

	c, err := r.StatsPool.Get(target)
	if err != nil {
		log.V(1).Info("gvdbclient pool get failed", "err", err)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: "StatsAvailable", Status: metav1.ConditionFalse,
			Reason: "DialFailed", Message: err.Error(),
		})
		return
	}
	stats, err := c.FetchStats(ctx)
	if err != nil {
		log.V(1).Info("FetchStats failed; preserving last-known stats", "err", err)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: "StatsAvailable", Status: metav1.ConditionFalse,
			Reason: "RPCFailed", Message: err.Error(),
		})
		return
	}
	cluster.Status.CollectionCount = stats.CollectionCount
	cluster.Status.TotalVectors = stats.TotalVectors
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "StatsAvailable", Status: metav1.ConditionTrue,
		Reason: "Fresh", Message: "last GetStats RPC succeeded",
	})
}

// computePhase derives .status.phase and the standard condition set from
// the nodeCounts view. Returns conditions with empty LastTransitionTime so
// meta.SetStatusCondition stamps it only when the condition actually
// transitions.
func computePhase(nc gvdbv1alpha1.NodeCountStatus) (gvdbv1alpha1.GVDBClusterPhase, []metav1.Condition) {
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
	return gvdbv1alpha1.PhasePending, []metav1.Condition{
		{Type: gvdbv1alpha1.ConditionAvailable, Status: metav1.ConditionFalse, Reason: "NotReady", Message: "no workload replicas are ready yet"},
		{Type: gvdbv1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: "Creating", Message: "workloads are being created"},
		{Type: gvdbv1alpha1.ConditionDegraded, Status: metav1.ConditionFalse, Reason: "Initializing", Message: "cluster has not yet reached any ready replica"},
	}
}

// SetupWithManager wires the controller into the manager. We watch the
// managed resource kinds so external changes (e.g. a user scaling a
// StatefulSet directly) trigger a reconcile to re-enforce the spec.
func (r *GVDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.StatsPool == nil {
		r.StatsPool = gvdbclient.NewPool()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&gvdbv1alpha1.GVDBCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Named("gvdbcluster").
		Complete(r)
}
