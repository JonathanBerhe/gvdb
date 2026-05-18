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
	appsv1apply "k8s.io/client-go/applyconfigurations/apps/v1"
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
	// — never two rollouts in flight because GetShardAssignments goes to
	// the coordinator leader. fetchShardAssignments is invoked lazily
	// inside the reconciler only when the STS is actually in a rolling
	// state, avoiding a wasted RPC every 30s when the cluster is stable.
	dnRolloutRequeue, dnErr := r.reconcileDataNodeRollout(ctx, &cluster, clusterHealth)
	if dnErr != nil {
		log.Error(dnErr, "data-node rollout failed")
	}

	// Manage query-node rolling upgrade: pod-by-pod partition walking with
	// no safety gates (query-nodes are stateless, no shard assignments).
	// Sequencing-only: holds the partition while coordinator OR data-node
	// rollouts are in flight, for single-phase rollout UX.
	qnRolloutRequeue, qnErr := r.reconcileQueryNodeRollout(ctx, &cluster)
	if qnErr != nil {
		log.Error(qnErr, "query-node rollout failed")
	}

	// Observe proxy rollout state reflectively. The K8s Deployment controller
	// drives the actual rolling update; the operator just surfaces progress
	// as a condition so users see a uniform per-component rollout status.
	pxRolloutRequeue, pxErr := r.reconcileProxyRolloutStatus(ctx, &cluster)
	if pxErr != nil {
		log.Error(pxErr, "proxy rollout status refresh failed")
	}

	// Reconcile coordinator Raft membership against spec.coordinator.replicas
	// (roadmap 1.8). The SIGTERM self-remove in 1.7b handles graceful
	// scale-down; this reconciler cleans up ghost peers left when K8s
	// SIGKILLs a coordinator pod (OOM, terminationGracePeriod exceeded).
	// Gated on coordinator rollout stability to avoid racing TransferLeadership
	// against a partition walk.
	scaleRequeue, scaleErr := r.reconcileCoordinatorScale(ctx, &cluster)
	if scaleErr != nil {
		log.Error(scaleErr, "coordinator scale reconciliation failed")
	}

	// Reconcile data-node StatefulSet replicas against spec.dataNode.replicas
	// (roadmap 1.8.c). Holds replicas at a safe floor when shrinking would
	// orphan any shard; kicks the coordinator to rebalance off doomed
	// ordinals. Sole writer of Spec.Replicas via datanodeScaleFieldManager —
	// render intentionally omits the field to keep ownership unambiguous.
	dnScaleRequeue, dnScaleErr := r.reconcileDataNodeScale(ctx, &cluster, clusterHealth)
	if dnScaleErr != nil {
		log.Error(dnScaleErr, "data-node scale reconciliation failed")
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
	// across all four rollout hints so whichever is active drives cadence.
	requeue := 30 * time.Second
	if rolloutRequeue > 0 && rolloutRequeue < requeue {
		requeue = rolloutRequeue
	}
	if dnRolloutRequeue > 0 && dnRolloutRequeue < requeue {
		requeue = dnRolloutRequeue
	}
	if qnRolloutRequeue > 0 && qnRolloutRequeue < requeue {
		requeue = qnRolloutRequeue
	}
	if pxRolloutRequeue > 0 && pxRolloutRequeue < requeue {
		requeue = pxRolloutRequeue
	}
	if scaleRequeue > 0 && scaleRequeue < requeue {
		requeue = scaleRequeue
	}
	if dnScaleRequeue > 0 && dnScaleRequeue < requeue {
		requeue = dnScaleRequeue
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// rolloutFieldManager is a distinct SSA field owner for the coordinator
// StatefulSet's rollout-only fields (partition + observed-term annotation).
// Keeping this separate from the main `gvdb-operator` owner makes the
// ownership boundary explicit: applyObject SSA cannot accidentally stomp
// the partition, and this path cannot touch template/spec fields.
const rolloutFieldManager = "gvdb-operator-rollout"

// fmtPartition renders a *int32 partition value as a decimal for logs,
// returning "unset" for nil. logr formats pointer-typed int32 as a memory
// address ("0x140000ab4d8"), which is useless for operators reading logs.
func fmtPartition(p *int32) string {
	if p == nil {
		return "unset"
	}
	return strconv.FormatInt(int64(*p), 10)
}

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
		if err := r.Apply(ctx, patch,
			client.FieldOwner(rolloutFieldManager),
			client.ForceOwnership,
		); err != nil {
			return 0, fmt.Errorf("SSA apply rollout fragment: %w", err)
		}
		log.Info("coordinator rollout partition advanced",
			"partition", fmtPartition(step.Partition), "term", leader.CurrentTerm, "reason", step.Reason)
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

// rolloutPatch builds the minimal SSA fragment the rollout owns. We use the
// generated apply-configuration builders rather than appsv1.StatefulSet —
// the typed API's Selector and Template fields have no `omitempty`, so
// marshaling a partial StatefulSet emits `selector: null` and an empty
// template, which the StatefulSet validator rejects (selector is immutable
// and required; the empty template's labels don't match the live selector).
func rolloutPatch(
	sts *appsv1.StatefulSet, partition *int32, term uint64,
) *appsv1apply.StatefulSetApplyConfiguration {
	p := appsv1apply.StatefulSet(sts.Name, sts.Namespace)
	if partition != nil {
		p.WithSpec(appsv1apply.StatefulSetSpec().
			WithUpdateStrategy(appsv1apply.StatefulSetUpdateStrategy().
				WithType(appsv1.RollingUpdateStatefulSetStrategyType).
				WithRollingUpdate(appsv1apply.RollingUpdateStatefulSetStrategy().
					WithPartition(*partition))))
	}
	if term != 0 {
		p.WithAnnotations(map[string]string{
			RolloutObservedTermAnnotation: strconv.FormatUint(term, 10),
		})
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
//
// Shard assignments are fetched lazily — only when the STS is actually
// rolling (UpdateRevision != CurrentRevision). In the stable case
// (majority of reconciles) the pure state machine returns ReasonStable
// without needing the shard layout, so the extra RPC is skipped.
func (r *GVDBClusterReconciler) reconcileDataNodeRollout(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
	clusterHealth gvdbclient.ClusterHealth,
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

	// Only fetch shard assignments when the STS is rolling — the pure
	// function ignores them in the stable case, so the RPC would be wasted.
	var shardAssignments []gvdbclient.ShardAssignment
	rolling := sts.Status.UpdateRevision != "" &&
		sts.Status.UpdateRevision != sts.Status.CurrentRevision
	if rolling {
		shardAssignments = r.fetchShardAssignments(ctx, cluster)
	}

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
		if err := r.Apply(ctx, patch,
			client.FieldOwner(datanodeRolloutFieldManager),
			client.ForceOwnership,
		); err != nil {
			return 0, fmt.Errorf("SSA apply data-node rollout fragment: %w", err)
		}
		log.Info("data-node rollout partition advanced",
			"partition", fmtPartition(step.Partition), "observedRebalance", newObserved, "reason", step.Reason)
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
// rollout owns (partition + observed-rebalance annotation). See rolloutPatch
// for why this uses the apply-configuration builders instead of the typed
// appsv1.StatefulSet.
func datanodeRolloutPatch(
	sts *appsv1.StatefulSet, partition *int32, observedRebalance int64,
) *appsv1apply.StatefulSetApplyConfiguration {
	p := appsv1apply.StatefulSet(sts.Name, sts.Namespace)
	if partition != nil {
		p.WithSpec(appsv1apply.StatefulSetSpec().
			WithUpdateStrategy(appsv1apply.StatefulSetUpdateStrategy().
				WithType(appsv1.RollingUpdateStatefulSetStrategyType).
				WithRollingUpdate(appsv1apply.RollingUpdateStatefulSetStrategy().
					WithPartition(*partition))))
	}
	if observedRebalance > 0 {
		p.WithAnnotations(map[string]string{
			DataNodeRolloutObservedRebalanceAnnotation: strconv.FormatInt(observedRebalance, 10),
		})
	}
	return p
}

// querynodeRolloutFieldManager is the SSA field owner for the query-node
// StatefulSet's partition field. Distinct from the three other rollout
// field managers so ownership boundaries remain explicit.
const querynodeRolloutFieldManager = "gvdb-operator-querynode-rollout"

// reconcileQueryNodeRollout enforces a pod-by-pod rolling update of the
// query-node StatefulSet. No safety gates beyond single-phase sequencing:
// query-nodes are stateless, the coordinator doesn't assign them shards,
// and reads already fail over to Ready replicas via prefer_routable_replica.
// See desiredQueryNodePartition for the full rationale.
func (r *GVDBClusterReconciler) reconcileQueryNodeRollout(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	key := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, render.QueryNodeComponent),
	}
	if err := r.Get(ctx, key, &sts); err != nil {
		return 0, client.IgnoreNotFound(err)
	}

	// Both prior rollout conditions must be True before we advance the
	// query-node partition. Absent conditions (first reconcile after CR
	// creation, before the coordinator/data-node reconcilers have run on
	// an actual STS) count as "not ready" — this is intentional: the
	// query-node STS doesn't exist yet in that window either, so the Get
	// above returns NotFound and we short-circuit. Do NOT refactor to
	// `IsStatusConditionPresentAndEqual(...True)` — that semantically
	// matches False-when-absent, but makes the bootstrap coupling less
	// obvious.
	priorReady := meta.IsStatusConditionTrue(
		cluster.Status.Conditions, gvdbv1alpha1.ConditionCoordinatorRolloutReady,
	) && meta.IsStatusConditionTrue(
		cluster.Status.Conditions, gvdbv1alpha1.ConditionDataNodeRolloutReady,
	)

	step := desiredQueryNodePartition(&sts, priorReady)

	condStatus := metav1.ConditionFalse
	msg := "query-node rollout in progress"
	if step.Done {
		condStatus = metav1.ConditionTrue
		msg = "query-node StatefulSet is stable"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    gvdbv1alpha1.ConditionQueryNodeRolloutReady,
		Status:  condStatus,
		Reason:  step.Reason,
		Message: msg,
	})

	if querynodeRolloutNeedsWrite(&sts, step) {
		patch := querynodeRolloutPatch(&sts, step.Partition)
		if err := r.Apply(ctx, patch,
			client.FieldOwner(querynodeRolloutFieldManager),
			client.ForceOwnership,
		); err != nil {
			return 0, fmt.Errorf("SSA apply query-node rollout fragment: %w", err)
		}
		log.Info("query-node rollout partition advanced",
			"partition", fmtPartition(step.Partition), "reason", step.Reason)
	}

	if step.Done {
		return 0, nil
	}
	return 5 * time.Second, nil
}

// querynodeRolloutNeedsWrite decides whether an SSA apply is required
// this reconcile — avoids a no-op Patch in the common stable case.
func querynodeRolloutNeedsWrite(sts *appsv1.StatefulSet, step QueryNodeRolloutStep) bool {
	currentPartition := int32(-1)
	if rs := sts.Spec.UpdateStrategy.RollingUpdate; rs != nil && rs.Partition != nil {
		currentPartition = *rs.Partition
	}
	desiredPartitionVal := int32(-1)
	if step.Partition != nil {
		desiredPartitionVal = *step.Partition
	}
	return desiredPartitionVal != currentPartition
}

// querynodeRolloutPatch builds the minimal SSA fragment the query-node
// rollout owns (partition only — no annotation state to persist because
// the state machine has no ratcheted gates). See rolloutPatch for why this
// uses the apply-configuration builders instead of the typed appsv1.StatefulSet.
func querynodeRolloutPatch(
	sts *appsv1.StatefulSet, partition *int32,
) *appsv1apply.StatefulSetApplyConfiguration {
	p := appsv1apply.StatefulSet(sts.Name, sts.Namespace)
	if partition != nil {
		p.WithSpec(appsv1apply.StatefulSetSpec().
			WithUpdateStrategy(appsv1apply.StatefulSetUpdateStrategy().
				WithType(appsv1.RollingUpdateStatefulSetStrategyType).
				WithRollingUpdate(appsv1apply.RollingUpdateStatefulSetStrategy().
					WithPartition(*partition))))
	}
	return p
}

// Reason constants for ConditionProxyRolloutReady.
const (
	ReasonProxyRolloutProgressing = "Progressing"
	ReasonProxyRolloutStalled     = "Stalled"
	ReasonProxyRolloutMissing     = "DeploymentNotFound"
)

// reconcileProxyRolloutStatus is a read-only observer. The K8s Deployment
// controller drives the actual rolling update via maxSurge/maxUnavailable;
// the operator only reports progress as a CR condition so users have a
// uniform per-component rollout status surface. No SSA writes, no strategy
// override — the proxy is stateless, default Deployment semantics are safe.
func (r *GVDBClusterReconciler) reconcileProxyRolloutStatus(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) (time.Duration, error) {
	var dep appsv1.Deployment
	key := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, render.ProxyComponent),
	}
	if err := r.Get(ctx, key, &dep); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return 0, err
		}
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:    gvdbv1alpha1.ConditionProxyRolloutReady,
			Status:  metav1.ConditionFalse,
			Reason:  ReasonProxyRolloutMissing,
			Message: "proxy Deployment not found yet",
		})
		return 5 * time.Second, nil
	}

	// Check stability FIRST, before Stalled. The K8s Deployment controller
	// lags Progressing condition updates behind actual state: during a
	// rollback after a stall, pods become Ready before Progressing flips
	// back to True, so a stability-wins-over-stalled ordering gives the
	// correct verdict as soon as the Deployment is genuinely healthy.
	//
	// specReplicas defaults to 1 only defensively — our render layer always
	// sets Spec.Replicas explicitly (EffectiveReplicas in render/names.go).
	// Readers should trust the spec; fallback handles manually-crafted
	// Deployments that bypass the operator.
	specReplicas := int32(1)
	if dep.Spec.Replicas != nil {
		specReplicas = *dep.Spec.Replicas
	}
	// All four checks matter — UpdatedReplicas alone misses rollbacks where
	// new pods failed to reach Ready; UnavailableReplicas catches pods that
	// crashed post-Ready.
	stable := dep.Status.ObservedGeneration >= dep.Generation &&
		dep.Status.UpdatedReplicas == specReplicas &&
		dep.Status.ReadyReplicas == specReplicas &&
		dep.Status.UnavailableReplicas == 0
	if stable {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:    gvdbv1alpha1.ConditionProxyRolloutReady,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonStable,
			Message: "proxy Deployment is stable",
		})
		return 0, nil
	}

	// Stalled: Deployment controller has given up (progressDeadlineSeconds
	// exceeded). Only report this if we're not already stable.
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:    gvdbv1alpha1.ConditionProxyRolloutReady,
				Status:  metav1.ConditionFalse,
				Reason:  ReasonProxyRolloutStalled,
				Message: c.Message,
			})
			return 0, nil
		}
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:   gvdbv1alpha1.ConditionProxyRolloutReady,
		Status: metav1.ConditionFalse,
		Reason: ReasonProxyRolloutProgressing,
		Message: fmt.Sprintf("updated=%d/%d ready=%d/%d unavailable=%d",
			dep.Status.UpdatedReplicas, specReplicas,
			dep.Status.ReadyReplicas, specReplicas,
			dep.Status.UnavailableReplicas),
	})
	return 5 * time.Second, nil
}

// reconcileCoordinatorScale observes Raft cluster_config via any live
// coordinator pod and drives it back in sync with spec.coordinator.replicas
// by calling RemovePeer / TransferLeadership RPCs on the leader. Handles
// the ghost-peer case that 1.7b's SIGTERM self-remove misses when K8s
// SIGKILLs a pod. Roadmap 1.8.
//
// Returns a requeue hint: 2s during an active scale-down (fast enough
// that TransferLeadership → RemovePeer → Stable converges in <10s), 5s
// while waiting for a rollout to finish, 0 (default idle) when stable.
func (r *GVDBClusterReconciler) reconcileCoordinatorScale(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	// Read spec.coordinator.replicas via render's helper so the default
	// (2) and the nil-pointer edge case are handled consistently with
	// the rest of the reconciler.
	specReplicas := render.EffectiveReplicas(cluster, render.CoordinatorComponent)

	// Fetch membership: fall through each coordinator pod until one
	// answers (some may be mid-restart).
	var membership gvdbclient.RaftMembership
	var gotMembership bool
	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			log.V(1).Info("coordinator dial failed; trying next", "target", target, "err", err)
			continue
		}
		m, err := cc.FetchRaftMembership(ctx)
		if err != nil {
			log.V(1).Info("FetchRaftMembership failed; trying next", "target", target, "err", err)
			continue
		}
		membership = m
		gotMembership = true
		break
	}
	if !gotMembership {
		// No pod answered — leave condition unchanged and requeue soon.
		// (Intentionally do NOT set ScaleReady=False here: a transient
		// coordinator outage shouldn't flip the condition and trigger
		// downstream alarms; the Condition stays at its last-known state.)
		return 5 * time.Second, nil
	}

	coordRolloutReady := meta.IsStatusConditionTrue(
		cluster.Status.Conditions, gvdbv1alpha1.ConditionCoordinatorRolloutReady,
	)

	step := desiredCoordinatorScaleStep(specReplicas, membership, coordRolloutReady)

	condStatus := metav1.ConditionFalse
	msg := "coordinator scale reconciliation in progress"
	if step.Done {
		condStatus = metav1.ConditionTrue
		msg = "Raft membership matches spec.coordinator.replicas"
	}
	if step.Message != "" {
		msg = step.Message
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    gvdbv1alpha1.ConditionCoordinatorScaleReady,
		Status:  condStatus,
		Reason:  step.Reason,
		Message: msg,
	})

	switch step.Action {
	case CoordinatorScaleStable, CoordinatorScaleWaitingForRollout,
		CoordinatorScaleWaitingForSuccessor, CoordinatorScaleWaitingForMembership:
		// No RPC this reconcile. Requeue fast when waiting (so we pick
		// up the rollout completion / successor-ready quickly); idle
		// cadence when stable.
		if step.Done {
			return 0, nil
		}
		return 5 * time.Second, nil

	case CoordinatorScaleRemovePeer:
		if err := r.callRemovePeerOnLeader(ctx, cluster, uint32(step.TargetNodeID)); err != nil {
			log.Error(err, "RemovePeer failed", "target_node_id", step.TargetNodeID)
			// Non-fatal: retry next reconcile.
			return 2 * time.Second, nil
		}
		log.Info("RemovePeer succeeded", "target_node_id", step.TargetNodeID)
		return 2 * time.Second, nil

	case CoordinatorScaleTransferLeadership:
		if err := r.callTransferLeadershipOnLeader(ctx, cluster, uint32(step.TargetNodeID)); err != nil {
			log.Error(err, "TransferLeadership failed", "target_node_id", step.TargetNodeID)
			return 2 * time.Second, nil
		}
		log.Info("TransferLeadership succeeded", "target_node_id", step.TargetNodeID)
		return 2 * time.Second, nil
	}

	return 5 * time.Second, nil
}

// callRemovePeerOnLeader dials coordinator pods in ordinal order and
// calls RemovePeer on the one that claims to be leader. On a NOT_LEADER
// response (success=false with current_leader_id hint) the err is
// returned; the caller treats it as transient and retries next
// reconcile after the leader reelection settles.
func (r *GVDBClusterReconciler) callRemovePeerOnLeader(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, nodeID uint32,
) error {
	var lastErr error
	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			lastErr = err
			continue
		}
		_, err = cc.RemovePeer(ctx, nodeID)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return fmt.Errorf("no coordinator available for RemovePeer(%d)", nodeID)
	}
	return lastErr
}

// callTransferLeadershipOnLeader — same fall-through pattern as
// callRemovePeerOnLeader for the TransferLeadership RPC.
func (r *GVDBClusterReconciler) callTransferLeadershipOnLeader(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, targetNodeID uint32,
) error {
	var lastErr error
	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			lastErr = err
			continue
		}
		_, err = cc.TransferLeadership(ctx, targetNodeID)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return fmt.Errorf("no coordinator available for TransferLeadership(%d)", targetNodeID)
	}
	return lastErr
}

// datanodeScaleFieldManager is the SSA field owner for Replicas on the
// data-node StatefulSet. Distinct from the main `gvdb-operator` field
// manager (which owns everything else the render produces) and from the
// rollout's field manager (which owns updateStrategy.rollingUpdate.partition).
// The render deliberately omits Replicas so this is the sole writer —
// see operator/internal/render/datanode.go.
const datanodeScaleFieldManager = "gvdb-operator-datanode-scale"

// DataNodeScaleLastRebalanceAnnotation records the last time the data-node
// scale reconciler triggered RebalanceShards. The rebalance-quiescence gate
// uses max(annotation, clusterHealth.LastRebalanceUnixMs) so neither a
// flapping coordinator report nor an operator-pod restart drops us below
// the RebalanceQuiescenceWindow ratchet.
const DataNodeScaleLastRebalanceAnnotation = "gvdb.io/datanode-scale-last-rebalance-trigger-unix-ms"

// reconcileDataNodeScale enforces safe scale-down of the data-node
// StatefulSet against spec.dataNode.replicas. Roadmap 1.8.c.
//
// Why this exists: SIGTERM → coordinator DRAINING migration (the happy
// path for scale-down today) promotes primaries and drops stale replica
// entries but does NOT re-replicate. A scale-down where the victim pod is
// the *only* replica holder for some shard orphans data. This reconciler
// is the pre-flight safety gate: it holds spec.replicas at a floor until
// the coordinator has re-distributed shards off ordinals about to be
// removed, then releases one ordinal at a time.
//
// Sequencing notes:
//   - This reconciler is the SOLE writer of Spec.Replicas on the data-node
//     STS; render omits the field (operator/internal/render/datanode.go).
//   - On first-ever reconcile the STS was just created by the main render
//     apply *without* Replicas; K8s defaults it to 1. Before any data-node
//     pod finishes starting, this reconciler patches Replicas to the target.
//     The transient single-pod state lasts less than one reconcile pass
//     (order of milliseconds) and the StatefulSet controller typically
//     never processes a create-at-1 event before we overwrite it.
//   - Yields to the data-node rollout — partition-gated pod eviction and
//     stepwise replica shrink must not interleave.
//
// Returns a requeue hint: 2s during an active scale-down, 5s while waiting
// (unsafe distribution, rollout, health), 0 when stable.
func (r *GVDBClusterReconciler) reconcileDataNodeScale(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
	clusterHealth gvdbclient.ClusterHealth,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	stsKey := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      render.WorkloadName(cluster, render.DataNodeComponent),
	}
	if err := r.Get(ctx, stsKey, &sts); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return 0, fmt.Errorf("get data-node sts: %w", err)
		}
		// STS not yet visible — either the main render apply earlier in
		// this reconcile pass hasn't propagated through the informer
		// cache, or the STS was deleted out from under us. We can't
		// SSA-apply a Replicas-only patch in this state (CREATE would
		// fail on missing required fields like Selector/Template), so
		// requeue quickly and let the next pass observe the recreated
		// STS. The condition stays at its last-known value rather than
		// flapping during a transient cache miss.
		return 2 * time.Second, nil
	}

	specReplicas := render.EffectiveReplicas(cluster, render.DataNodeComponent)

	currentSpecReplicas := int32(0)
	observedReadyReplicas := int32(0)
	if sts.Spec.Replicas != nil {
		currentSpecReplicas = *sts.Spec.Replicas
	}
	observedReadyReplicas = sts.Status.ReadyReplicas

	rolloutReady := meta.IsStatusConditionTrue(
		cluster.Status.Conditions, gvdbv1alpha1.ConditionDataNodeRolloutReady,
	)

	// Fetch shard assignments only when a scale-down decision is pending.
	// In every other path (stable, growing, bootstrap, waiting) the pure
	// function ignores the shard list, so the RPC would be wasted.
	var shardAssignments []gvdbclient.ShardAssignment
	if currentSpecReplicas > 0 && currentSpecReplicas > specReplicas {
		shardAssignments = r.fetchShardAssignments(ctx, cluster)
		// If the cluster reports shards but we got nothing back, we can't
		// make a safe decision — hold and surface the gap.
		if shardAssignments == nil && clusterHealth.TotalShards > 0 {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:   gvdbv1alpha1.ConditionDataNodeScaleReady,
				Status: metav1.ConditionFalse,
				Reason: ReasonWaitingForShardVisibility,
				Message: fmt.Sprintf(
					"could not fetch shard assignments from any coordinator; cluster reports %d total shards",
					clusterHealth.TotalShards),
			})
			return 5 * time.Second, nil
		}
	}

	step := desiredDataNodeScaleStep(
		specReplicas, currentSpecReplicas, observedReadyReplicas,
		shardAssignments, clusterHealth, rolloutReady,
	)

	condStatus := metav1.ConditionFalse
	msg := "data-node scale reconciliation in progress"
	if step.Done {
		condStatus = metav1.ConditionTrue
		msg = fmt.Sprintf("data-node StatefulSet at %d replicas; shard placement safe", specReplicas)
	}
	if step.Message != "" {
		msg = step.Message
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    gvdbv1alpha1.ConditionDataNodeScaleReady,
		Status:  condStatus,
		Reason:  step.Reason,
		Message: msg,
	})

	switch step.Action {
	case DataNodeScaleStable:
		return 0, nil

	case DataNodeScaleGrowing, DataNodeScaleShrinking:
		// Both paths write Replicas via the scale field manager. Growing
		// is a single-shot jump to target; Shrinking drops by exactly one
		// ordinal so each step is re-gated on fresh state next reconcile.
		if err := r.applyDataNodeScalePatch(ctx, cluster, step.EffectiveReplicas, 0); err != nil {
			log.Error(err, "data-node scale SSA apply failed",
				"action", step.Action, "replicas", step.EffectiveReplicas)
			return 2 * time.Second, nil
		}
		log.Info("data-node scale applied",
			"action", step.Action, "replicas", step.EffectiveReplicas,
			"victimOrdinal", step.VictimOrdinal)
		return 2 * time.Second, nil

	case DataNodeScaleUnsafeShrink:
		// (Throttled) ask the coordinator to re-plan so doomed ordinals
		// get drained. The replicas reaffirm + annotation stamp ride in
		// a single SSA apply so the field manager never transiently
		// relinquishes Replicas ownership between two writes.
		var triggerAtMs int64
		if step.ShouldTriggerRebalance && r.shouldTriggerDataNodeRebalance(&sts, clusterHealth, time.Now()) {
			shards, err := r.callRebalanceShardsOnLeader(ctx, cluster, 0)
			if err != nil {
				log.Error(err, "RebalanceShards for data-node scale failed")
			} else {
				log.Info("RebalanceShards triggered for data-node scale",
					"shards_enqueued", shards, "victim_ordinal", step.VictimOrdinal,
					"unsafe_shards", step.UnsafeShards)
				triggerAtMs = time.Now().UnixMilli()
			}
		}
		if err := r.applyDataNodeScalePatch(ctx, cluster, step.EffectiveReplicas, triggerAtMs); err != nil {
			log.V(1).Info("unsafe-shrink replicas reaffirm failed (non-fatal)", "err", err)
		}
		return 5 * time.Second, nil

	case DataNodeScaleRF1Blocked:
		if err := r.applyDataNodeScalePatch(ctx, cluster, step.EffectiveReplicas, 0); err != nil {
			log.V(1).Info("rf1-blocked replicas reaffirm failed (non-fatal)", "err", err)
		}
		// Surface the permanent block as a Warning Event so the user sees
		// it on `kubectl describe gvdbcluster` without reading operator
		// logs. The condition already carries the signal; the event is UX.
		// The event Reason must be a short reason constant (not the
		// condition type) to match the user-facing "REASON" column in
		// `kubectl describe` — follows the rollout's ReasonRF1Blocked
		// precedent at line 477.
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning,
				ReasonRF1ShardPinnedToScaleVictim, msg)
		}
		return 30 * time.Second, nil

	case DataNodeScaleWaitingForConvergence:
		return 2 * time.Second, nil

	case DataNodeScaleWaitingForRollout, DataNodeScaleWaitingForHealth:
		return 5 * time.Second, nil
	}

	return 5 * time.Second, nil
}

// applyDataNodeScalePatch writes spec.replicas on the data-node STS via
// SSA, claiming ownership under datanodeScaleFieldManager. Optionally
// stamps the rebalance-trigger annotation in the same apply so SSA does
// not transiently relinquish Replicas ownership between writes — each
// SSA apply replaces the field manager's owned-set with whatever it sent,
// so a separate annotation-only apply would drop the Replicas claim.
//
// Uses ForceOwnership so that if a pre-1.8.c deployment left Replicas
// owned by the main `gvdb-operator` field manager, the first scale
// reconciler pass transfers ownership cleanly rather than erroring out.
//
// rebalanceTriggerAtMs == 0 means "do not touch the annotation"; non-zero
// means "stamp this Unix-ms value as the latest rebalance trigger".
func (r *GVDBClusterReconciler) applyDataNodeScalePatch(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster,
	replicas int32, rebalanceTriggerAtMs int64,
) error {
	patch := appsv1apply.StatefulSet(
		render.WorkloadName(cluster, render.DataNodeComponent),
		cluster.Namespace,
	).WithSpec(appsv1apply.StatefulSetSpec().WithReplicas(replicas))
	if rebalanceTriggerAtMs > 0 {
		patch.WithAnnotations(map[string]string{
			DataNodeScaleLastRebalanceAnnotation: strconv.FormatInt(rebalanceTriggerAtMs, 10),
		})
	}
	return r.Apply(ctx, patch,
		client.FieldOwner(datanodeScaleFieldManager),
		client.ForceOwnership,
	)
}

// shouldTriggerDataNodeRebalance applies the quiescence throttle: fire
// RebalanceShards at most once per RebalanceQuiescenceWindow so a tight
// reconcile loop doesn't spam the coordinator. Ratchets against
// clusterHealth.LastRebalanceUnixMs (what the coordinator reports as the
// most recent rebalance) AND the operator-persisted last-trigger
// annotation — takes whichever is more recent.
func (r *GVDBClusterReconciler) shouldTriggerDataNodeRebalance(
	sts *appsv1.StatefulSet, clusterHealth gvdbclient.ClusterHealth, now time.Time,
) bool {
	refMs := clusterHealth.LastRebalanceUnixMs
	if sts != nil && sts.Annotations != nil {
		if raw, ok := sts.Annotations[DataNodeScaleLastRebalanceAnnotation]; ok {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > refMs {
				refMs = v
			}
		}
	}
	if refMs <= 0 {
		return true // no prior trigger on record — fire.
	}
	return now.Sub(time.UnixMilli(refMs)) >= RebalanceQuiescenceWindow
}

// callRebalanceShardsOnLeader dials coordinator pods in ordinal order and
// calls RebalanceShards on the first that accepts. RebalanceShards is a
// leader-routed RPC on the coordinator but returns success from any
// healthy pod that can reach the leader — the server-side path does the
// routing. Mirrors callRemovePeerOnLeader / callTransferLeadershipOnLeader.
func (r *GVDBClusterReconciler) callRebalanceShardsOnLeader(
	ctx context.Context, cluster *gvdbv1alpha1.GVDBCluster, collectionID uint32,
) (uint32, error) {
	var lastErr error
	for _, target := range render.CoordinatorPodAddresses(cluster) {
		cc, err := r.StatsPool.GetCoordinator(target)
		if err != nil {
			lastErr = err
			continue
		}
		n, err := cc.RebalanceShards(ctx, collectionID)
		if err == nil {
			return n, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return 0, fmt.Errorf("no coordinator available for RebalanceShards(%d)", collectionID)
	}
	return 0, lastErr
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
