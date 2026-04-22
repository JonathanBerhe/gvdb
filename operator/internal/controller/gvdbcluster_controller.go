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
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
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

	// Recorder emits Kubernetes Events on the GVDBCluster CR. Used to surface
	// rollout decisions that a user needs to see without tailing operator
	// logs (e.g. RF=1 deadlock blocking data-node rollout).
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
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
			// Drop pooled gRPC connections for this cluster so a renamed or
			// recreated CR doesn't inherit stale dial state.
			targets := append(render.CoordinatorPodAddresses(&cluster),
				fmt.Sprintf("%s.%s.svc.%s:%d",
					render.WorkloadName(&cluster, render.ProxyComponent),
					cluster.Namespace,
					render.ClusterDomain(&cluster),
					render.ProxyGRPCPort,
				))
			r.StatsPool.CloseClusterTargets(targets)
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
	leader, clusterHealth := r.refreshCoordinatorStatus(ctx, &cluster)

	// Manage coordinator rolling upgrade: partition-gated pod-by-pod drain
	// that waits for Raft re-election AND term stability between pods
	// (roadmap 0b.6.C). Runs after status refresh so we have the freshest
	// leader view to gate on.
	rolloutRequeue, rErr := r.reconcileCoordinatorRollout(ctx, &cluster, leader)
	if rErr != nil {
		log.Error(rErr, "coordinator rollout failed")
	}

	// Manage data-node rolling upgrade: partition-gated pod-by-pod drain
	// guarded by cluster health, rebalance quiescence, and per-shard replica
	// safety. Runs AFTER the coordinator rollout and gates on its condition
	// — never two rollouts in flight because GetShardAssignments goes to the
	// coordinator leader.
	shardAssignments := r.fetchShardAssignments(ctx, &cluster)
	dnRolloutRequeue, dnErr := r.reconcileDataNodeRollout(ctx, &cluster, clusterHealth, shardAssignments)
	if dnErr != nil {
		log.Error(dnErr, "data-node rollout failed")
	}

	phase, conditions := computePhase(nodeCounts)
	cluster.Status.Phase = phase
	for _, cond := range conditions {
		meta.SetStatusCondition(&cluster.Status.Conditions, cond)
	}
	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// During an active rollout requeue fast so the reconciler can advance
	// the partition without waiting 30s between decisions. Take the minimum
	// of both rollout hints so whichever is active drives the cadence.
	requeue := 30 * time.Second
	if rolloutRequeue > 0 && rolloutRequeue < requeue {
		requeue = rolloutRequeue
	}
	if dnRolloutRequeue > 0 && dnRolloutRequeue < requeue {
		requeue = dnRolloutRequeue
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// rolloutFieldManager is a distinct SSA field owner for the coordinator
// StatefulSet's rollout-only fields (partition + observed-term annotation).
// Keeping this separate from the main `gvdb-operator` owner makes the
// ownership boundary explicit: applyObject SSA cannot accidentally stomp
// the partition, and this path cannot touch template/spec fields.
const rolloutFieldManager = "gvdb-operator-rollout"

// reconcileCoordinatorRollout enforces a pod-by-pod, Raft-quorum-aware
// rolling update of the coordinator StatefulSet.
//
// Returns a hint for how soon to requeue; 0 means "use the default". A
// non-zero value is returned during an active rollout so the reconciler
// advances the partition quickly (typically every 5s) rather than waiting
// for the 30s idle tick.
func (r *GVDBClusterReconciler) reconcileCoordinatorRollout(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
	leader gvdbclient.LeaderInfo,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	// Fetch the live STS — we can't rely on our in-memory render because
	// the K8s StatefulSet controller has been updating .status.
	var sts appsv1.StatefulSet
	key := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, render.CoordinatorComponent),
	}
	if err := r.Get(ctx, key, &sts); err != nil {
		// Likely the first reconcile; we'll pick it up next time.
		return 0, client.IgnoreNotFound(err)
	}

	// observedTerm is the term we wrote on the last reconcile pass. It
	// lives in an STS annotation so the rollout invariant survives an
	// operator-pod restart — we don't need durable operator-side state.
	observedTerm := readObservedTerm(&sts)

	step := desiredPartition(&sts, leader, observedTerm)

	// Mirror the step onto the condition set.
	condStatus := metav1.ConditionFalse
	msg := "coordinator rollout in progress"
	if step.Done {
		condStatus = metav1.ConditionTrue
		msg = "coordinator StatefulSet is stable"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    gvdbv1alpha1.ConditionCoordinatorRolloutReady,
		Status:  condStatus,
		Reason:  step.Reason,
		Message: msg,
	})

	// Write partition + observed-term together via SSA under a distinct
	// field owner. Using SSA here (a) keeps field ownership explicit so
	// applyObject can't accidentally stomp the partition, and (b) is safe
	// against concurrent partial edits from a user kubectl-patching the
	// STS by hand — SSA resolves per-field.
	needsWrite := r.rolloutNeedsWrite(&sts, step, leader.CurrentTerm, observedTerm)
	if needsWrite {
		patch := rolloutPatch(&sts, step.Partition, leader.CurrentTerm)
		if err := r.Patch(ctx, patch, client.Apply,
			client.FieldOwner(rolloutFieldManager),
			client.ForceOwnership,
		); err != nil {
			return 0, fmt.Errorf("SSA apply rollout fragment: %w", err)
		}
		log.Info("coordinator rollout partition advanced",
			"partition", step.Partition, "term", leader.CurrentTerm, "reason", step.Reason)
	}

	if step.Done {
		return 0, nil
	}
	// Active rollout: requeue fast.
	return 5 * time.Second, nil
}

// readObservedTerm reads the operator-observed Raft term from the STS
// annotation, zero if absent or malformed.
func readObservedTerm(sts *appsv1.StatefulSet) uint64 {
	if sts.Annotations == nil {
		return 0
	}
	raw := sts.Annotations[RolloutObservedTermAnnotation]
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// rolloutNeedsWrite decides whether an SSA apply of partition+term is
// required this reconcile. Avoids a no-op Patch round-trip in the common
// stable case.
func (r *GVDBClusterReconciler) rolloutNeedsWrite(
	sts *appsv1.StatefulSet, step RolloutStep, newTerm, observedTerm uint64,
) bool {
	currentPartition := int32(-1) // -1 = "no partition field set"
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}
	desiredPartitionVal := int32(-1)
	if step.Partition != nil {
		desiredPartitionVal = *step.Partition
	}
	if desiredPartitionVal != currentPartition {
		return true
	}
	// Only record the term when we have one (skip zero).
	return newTerm != 0 && newTerm != observedTerm
}

// rolloutPatch builds the minimal SSA fragment the rollout owns.
func rolloutPatch(
	sts *appsv1.StatefulSet, partition *int32, term uint64,
) *appsv1.StatefulSet {
	p := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sts.Name,
			Namespace: sts.Namespace,
		},
	}
	if partition != nil {
		p.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
				Partition: partition,
			},
		}
	}
	if term != 0 {
		p.Annotations = map[string]string{
			RolloutObservedTermAnnotation: strconv.FormatUint(term, 10),
		}
	}
	return p
}

// datanodeRolloutFieldManager is the SSA field owner for the data-node
// StatefulSet's rollout-only fields (partition + observed-rebalance
// annotation). Kept distinct from both the main `gvdb-operator` owner AND
// the coordinator rollout owner so ownership boundaries remain explicit.
const datanodeRolloutFieldManager = "gvdb-operator-datanode-rollout"

// reconcileDataNodeRollout enforces a pod-by-pod, drain-aware rolling update
// of the data-node StatefulSet. Gated on cluster health, rebalance
// quiescence, and per-shard replica safety — see desiredDataNodePartition.
func (r *GVDBClusterReconciler) reconcileDataNodeRollout(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
	clusterHealth gvdbclient.ClusterHealth,
	shardAssignments []gvdbclient.ShardAssignment,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	key := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, render.DataNodeComponent),
	}
	if err := r.Get(ctx, key, &sts); err != nil {
		return 0, client.IgnoreNotFound(err)
	}

	observedRebalance := readObservedRebalance(&sts)
	coordRolloutReady := meta.IsStatusConditionTrue(
		cluster.Status.Conditions, gvdbv1alpha1.ConditionCoordinatorRolloutReady,
	)

	step := desiredDataNodePartition(
		&sts, clusterHealth, shardAssignments,
		observedRebalance, time.Now(), coordRolloutReady,
	)

	// Mirror the step onto the condition set. The Message carries shard-
	// level detail when one of the safety gates blocks.
	condStatus := metav1.ConditionFalse
	msg := "data-node rollout in progress"
	if step.Done {
		condStatus = metav1.ConditionTrue
		msg = "data-node StatefulSet is stable"
	}
	if step.Message != "" {
		msg = step.Message
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    gvdbv1alpha1.ConditionDataNodeRolloutReady,
		Status:  condStatus,
		Reason:  step.Reason,
		Message: msg,
	})

	// RF=1 deadlock is a user-facing stuck state; surface it as a warning
	// Event so `kubectl describe gvdbcluster` shows why without requiring
	// operator logs. The condition reason already carries the signal too.
	if step.Reason == ReasonRF1Blocked && r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, ReasonRF1Blocked, msg)
	}

	// Ratchet the observed-rebalance forward. Coordinator's clusterLast can
	// briefly report a stale value after a coordinator restart; keep the
	// max so Gate B doesn't silently relax its window.
	newObserved := observedRebalance
	if clusterHealth.LastRebalanceUnixMs > newObserved {
		newObserved = clusterHealth.LastRebalanceUnixMs
	}

	needsWrite := datanodeRolloutNeedsWrite(&sts, step, newObserved, observedRebalance)
	if needsWrite {
		patch := datanodeRolloutPatch(&sts, step.Partition, newObserved)
		if err := r.Patch(ctx, patch, client.Apply,
			client.FieldOwner(datanodeRolloutFieldManager),
			client.ForceOwnership,
		); err != nil {
			return 0, fmt.Errorf("SSA apply data-node rollout fragment: %w", err)
		}
		log.Info("data-node rollout partition advanced",
			"partition", step.Partition, "observedRebalance", newObserved, "reason", step.Reason)
	}

	if step.Done {
		return 0, nil
	}
	return 5 * time.Second, nil
}

// readObservedRebalance reads the operator-persisted last-rebalance Unix-ms
// timestamp from the data-node STS annotation. Zero if absent / malformed.
func readObservedRebalance(sts *appsv1.StatefulSet) int64 {
	if sts.Annotations == nil {
		return 0
	}
	raw := sts.Annotations[DataNodeRolloutObservedRebalanceAnnotation]
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// datanodeRolloutNeedsWrite decides whether an SSA apply is required this
// reconcile — avoids a no-op Patch round-trip in the common stable case.
func datanodeRolloutNeedsWrite(
	sts *appsv1.StatefulSet, step DataNodeRolloutStep,
	newObservedRebalance, prevObservedRebalance int64,
) bool {
	currentPartition := int32(-1)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}
	desiredPartitionVal := int32(-1)
	if step.Partition != nil {
		desiredPartitionVal = *step.Partition
	}
	if desiredPartitionVal != currentPartition {
		return true
	}
	return newObservedRebalance > 0 && newObservedRebalance != prevObservedRebalance
}

// datanodeRolloutPatch builds the minimal SSA fragment the data-node
// rollout owns (partition + observed-rebalance annotation).
func datanodeRolloutPatch(
	sts *appsv1.StatefulSet, partition *int32, observedRebalance int64,
) *appsv1.StatefulSet {
	p := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sts.Name,
			Namespace: sts.Namespace,
		},
	}
	if partition != nil {
		p.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
				Partition: partition,
			},
		}
	}
	if observedRebalance > 0 {
		p.Annotations = map[string]string{
			DataNodeRolloutObservedRebalanceAnnotation: strconv.FormatInt(observedRebalance, 10),
		}
	}
	return p
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

// refreshCoordinatorStatus asks any live coordinator pod for leader info
// + last-rebalance timestamp and populates the corresponding CR status
// fields. Fall-through dial across pod-0, pod-1, ... so the reconciler
// keeps working while an individual coordinator pod is being rolled
// (roadmap 0b.6.D + 0b.6.E).
//
// Returns the LeaderInfo observed from whichever coordinator answered
// first (zero value if no pod responded). Callers — specifically the
// rollout state machine — use this for the quorum-aware advance gate.
//
// Note: we trust the first-responding coordinator's view of the leader.
// Under a fresh partition, coordinators could disagree; Raft guarantees
// convergence but not within one reconcile. For diagnostic reads a
// majority quorum query would be more rigorous — kept simple here
// because the rollout gate is belt-and-braces, not the only safety net.
func (r *GVDBClusterReconciler) refreshCoordinatorStatus(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) (gvdbclient.LeaderInfo, gvdbclient.ClusterHealth) {
	log := logf.FromContext(ctx)

	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			log.V(1).Info("coordinator dial failed; trying next", "target", target, "err", err)
			continue
		}

		// Leader info is the must-have; we surface it even if health fails.
		leader, lErr := cc.FetchLeaderInfo(ctx)
		if lErr == nil {
			if leader.HasLeader() {
				// Prefer server-resolved name; fall back to ordinal convention.
				name := leader.LeaderAddress
				if name == "" {
					name = render.CoordinatorPodName(cluster, leader.LeaderID)
				}
				cluster.Status.CoordinatorLeader = name
			} else {
				cluster.Status.CoordinatorLeader = ""
			}
		}

		// Cluster health carries the last-rebalance timestamp PLUS the
		// per-node status used by the data-node rollout's replica-safety
		// gate. Return it alongside the leader so the caller doesn't need
		// to re-dial.
		var health gvdbclient.ClusterHealth
		if h, hErr := cc.FetchClusterHealth(ctx); hErr == nil {
			health = h
			if h.LastRebalanceUnixMs > 0 {
				t := metav1.NewTime(time.UnixMilli(h.LastRebalanceUnixMs))
				cluster.Status.LastRebalance = &t
			}
		}

		// Stop on first pod that answered leader info successfully — any
		// one coordinator knows the current leader.
		if lErr == nil {
			return leader, health
		}
	}
	log.V(1).Info("all coordinator pods unreachable; leader status unchanged")
	return gvdbclient.LeaderInfo{}, gvdbclient.ClusterHealth{}
}

// fetchShardAssignments dials coordinator pods in ordinal order until one
// answers GetShardAssignments. Returns nil on total failure — callers
// (specifically reconcileDataNodeRollout) interpret an empty slice as "no
// data to reason about" and refuse to advance the replica-safety gate.
func (r *GVDBClusterReconciler) fetchShardAssignments(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) []gvdbclient.ShardAssignment {
	log := logf.FromContext(ctx)
	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			continue
		}
		sa, err := cc.FetchShardAssignments(ctx)
		if err != nil {
			log.V(1).Info("FetchShardAssignments failed; trying next coordinator",
				"target", target, "err", err)
			continue
		}
		return sa
	}
	return nil
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
