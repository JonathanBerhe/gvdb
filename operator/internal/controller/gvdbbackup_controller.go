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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	"gvdb/operator/internal/gvdbclient"
	"gvdb/operator/internal/render"
)

// pollInterval controls how often the backup reconciler re-polls the
// server while the backup is still in flight. Bound to ~5s so we don't
// thrash the proxy but still surface terminal transitions within an
// operator-friendly window.
const backupPollInterval = 5 * time.Second

// GVDBBackupReconciler reconciles a GVDBBackup object by driving a
// backup against the referenced GVDBCluster's proxy. The reconciler
// holds no state of its own — every reconcile re-derives the desired
// next action from the CR's status (`backupID` set or not) so a
// controller restart never produces a duplicate backup.
type GVDBBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// StatsPool reuses long-lived gRPC clients across reconciles. The
	// gvdbcluster reconciler creates and owns the pool; the operator
	// shares one across all controllers.
	StatsPool *gvdbclient.Pool
}

// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbbackups/finalizers,verbs=update

// Reconcile drives the GVDBBackup CR toward its desired terminal state.
func (r *GVDBBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var bkp gvdbv1alpha1.GVDBBackup
	if err := r.Get(ctx, req.NamespacedName, &bkp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal CRs need no further action. We still observe the
	// generation so a spec change after completion is visible.
	if isTerminalBackupPhase(bkp.Status.Phase) {
		if bkp.Status.ObservedGeneration != bkp.Generation {
			bkp.Status.ObservedGeneration = bkp.Generation
			return ctrl.Result{}, r.Status().Update(ctx, &bkp)
		}
		return ctrl.Result{}, nil
	}

	// Validate spec.target — exactly one of s3 / local must be set. The
	// CRD currently can't express oneOf, so we enforce it in the
	// reconciler and surface the failure on status.
	if err := validateBackupTarget(bkp.Spec.Target); err != nil {
		return r.failBackup(ctx, &bkp, "InvalidTarget", err.Error())
	}

	// Look up the referenced cluster and dial its proxy.
	cluster, err := r.fetchCluster(ctx, &bkp)
	if err != nil {
		return r.failBackup(ctx, &bkp, "ClusterNotFound", err.Error())
	}
	proxyAddr := proxyAddress(cluster)
	cli, err := r.StatsPool.Get(proxyAddr)
	if err != nil {
		log.Error(err, "Dial proxy failed", "addr", proxyAddr)
		// Transient; requeue with the standard interval.
		return ctrl.Result{RequeueAfter: backupPollInterval}, nil
	}

	target := backupTargetFromCR(bkp.Spec.Target)

	// Lazy-start the backup: first reconcile after observing the CR
	// allocates the backup_id; subsequent reconciles poll. The
	// idempotency-key contract means re-invoking StartBackup with the
	// same id is a no-op on the server side, so even if our status
	// write below races a controller restart we won't duplicate work.
	if bkp.Status.BackupID == "" {
		idempotencyKey := backupIdempotencyKey(&bkp)
		backupID, err := cli.StartBackup(
			ctx, bkp.Spec.Collection, target, idempotencyKey)
		if err != nil {
			log.Error(err, "StartBackup failed")
			// A freshly-created cluster's proxy may be Ready as a pod
			// while kube-proxy is still programming its service endpoint,
			// so the first reconcile after the CR lands can hit
			// UNAVAILABLE/connection-refused. Requeue rather than
			// permanently failing the CR on a startup race.
			if isTransientGRPCError(err) {
				return ctrl.Result{RequeueAfter: backupPollInterval}, nil
			}
			return r.failBackup(ctx, &bkp, "StartBackupFailed", err.Error())
		}
		bkp.Status.BackupID = backupID
		bkp.Status.Phase = gvdbv1alpha1.BackupPhaseRunning
		now := metav1.NewTime(time.Now())
		bkp.Status.StartedAt = &now
		bkp.Status.ObservedGeneration = bkp.Generation
		setBackupCondition(&bkp.Status.Conditions, metav1.ConditionUnknown,
			"BackupRunning", "Backup job started: "+backupID)
		if err := r.Status().Update(ctx, &bkp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: backupPollInterval}, nil
	}

	// Poll for current status. NotFound on the server (e.g. coordinator
	// restart) is fatal in v1alpha1 — backup state is in-memory.
	status, err := cli.GetBackupStatus(ctx, bkp.Status.BackupID)
	if err != nil {
		log.Error(err, "GetBackupStatus failed",
			"backupID", bkp.Status.BackupID)
		// Proxy may briefly disappear (rolling restart, drain) without
		// invalidating the in-flight backup. Treat transient gRPC
		// failures as worth re-polling rather than poisoning the CR.
		if isTransientGRPCError(err) {
			return ctrl.Result{RequeueAfter: backupPollInterval}, nil
		}
		return r.failBackup(ctx, &bkp, "StatusUnavailable", err.Error())
	}

	prevPhase := bkp.Status.Phase
	bkp.Status.Phase = phaseFromBackupState(status.State)
	//nolint:gosec // server counts fit int32 at realistic shard cardinalities.
	bkp.Status.ShardsTotal = int32(status.ShardsTotal)
	//nolint:gosec
	bkp.Status.ShardsCompleted = int32(status.ShardsCompleted)
	//nolint:gosec
	bkp.Status.SizeBytes = int64(status.BytesUploaded)
	if status.ManifestURI != "" {
		bkp.Status.ManifestURI = status.ManifestURI
	}
	bkp.Status.ObservedGeneration = bkp.Generation

	if status.State.IsTerminal() {
		now := metav1.NewTime(time.Now())
		bkp.Status.CompletedAt = &now
		switch status.State {
		case gvdbclient.BackupStateCompleted:
			setBackupCondition(&bkp.Status.Conditions, metav1.ConditionTrue,
				"BackupCompleted",
				fmt.Sprintf("Backup completed in %.1fs", status.ElapsedSeconds))
		case gvdbclient.BackupStateFailed:
			msg := status.ErrorMessage
			if msg == "" {
				msg = "server reported FAILED with no error message"
			}
			setBackupCondition(&bkp.Status.Conditions, metav1.ConditionFalse,
				"BackupFailed", msg)
		case gvdbclient.BackupStateCancelled:
			setBackupCondition(&bkp.Status.Conditions, metav1.ConditionFalse,
				"BackupCancelled", "Backup was cancelled")
		}
	} else if prevPhase != bkp.Status.Phase {
		setBackupCondition(&bkp.Status.Conditions, metav1.ConditionUnknown,
			"BackupRunning",
			fmt.Sprintf("Backup running (%d/%d shards complete)",
				status.ShardsCompleted, status.ShardsTotal))
	}

	if err := r.Status().Update(ctx, &bkp); err != nil {
		return ctrl.Result{}, err
	}

	if status.State.IsTerminal() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: backupPollInterval}, nil
}

// SetupWithManager wires the reconciler to the manager.
func (r *GVDBBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gvdbv1alpha1.GVDBBackup{}).
		Named("gvdbbackup").
		Complete(r)
}

// =============================================================================
// Helpers
// =============================================================================

func (r *GVDBBackupReconciler) fetchCluster(ctx context.Context,
	bkp *gvdbv1alpha1.GVDBBackup) (*gvdbv1alpha1.GVDBCluster, error) {
	var cluster gvdbv1alpha1.GVDBCluster
	key := types.NamespacedName{
		Namespace: bkp.Namespace,
		Name:      bkp.Spec.ClusterRef.Name,
	}
	if err := r.Get(ctx, key, &cluster); err != nil {
		return nil, fmt.Errorf("GVDBCluster %s: %w", key, err)
	}
	return &cluster, nil
}

// failBackup transitions the CR to Failed with the supplied reason and
// returns no requeue — the caller's enclosing Reconcile signals "give up".
func (r *GVDBBackupReconciler) failBackup(ctx context.Context,
	bkp *gvdbv1alpha1.GVDBBackup, reason, message string) (ctrl.Result, error) {
	bkp.Status.Phase = gvdbv1alpha1.BackupPhaseFailed
	bkp.Status.ObservedGeneration = bkp.Generation
	now := metav1.NewTime(time.Now())
	bkp.Status.CompletedAt = &now
	setBackupCondition(&bkp.Status.Conditions, metav1.ConditionFalse,
		reason, message)
	if err := r.Status().Update(ctx, bkp); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// proxyAddress reconstructs the cluster-local proxy DNS name. Same shape
// the GVDBCluster reconciler uses to dial GetStats.
func proxyAddress(cluster *gvdbv1alpha1.GVDBCluster) string {
	return fmt.Sprintf("%s.%s.svc.%s:%d",
		render.WorkloadName(cluster, render.ProxyComponent),
		cluster.Namespace,
		render.ClusterDomain(cluster),
		render.ProxyGRPCPort,
	)
}

// backupIdempotencyKey derives a stable id from CR identity. Reused as
// the backup_id across reconciles so a controller restart between
// StartBackup and the status write never produces two server-side jobs.
func backupIdempotencyKey(bkp *gvdbv1alpha1.GVDBBackup) string {
	// uid suffix keeps the key unique across deletions/recreates of the
	// same CR name in the same namespace.
	return fmt.Sprintf("bk-%s-%s", bkp.Name, bkp.UID[:8])
}

func validateBackupTarget(t gvdbv1alpha1.BackupTargetSpec) error {
	if t.S3 != nil && t.Local != nil {
		return fmt.Errorf("spec.target must set exactly one of s3 or local, not both")
	}
	if t.S3 == nil && t.Local == nil {
		return fmt.Errorf("spec.target must set one of s3 or local")
	}
	return nil
}

func backupTargetFromCR(t gvdbv1alpha1.BackupTargetSpec) gvdbclient.BackupTarget {
	if t.S3 != nil {
		return gvdbclient.BackupTarget{S3: &gvdbclient.S3Target{
			Bucket: t.S3.Bucket,
			Prefix: t.S3.Prefix,
		}}
	}
	return gvdbclient.BackupTarget{Local: &gvdbclient.LocalTarget{
		Path: t.Local.Path,
	}}
}

func isTerminalBackupPhase(p gvdbv1alpha1.GVDBBackupPhase) bool {
	return p == gvdbv1alpha1.BackupPhaseCompleted ||
		p == gvdbv1alpha1.BackupPhaseFailed ||
		p == gvdbv1alpha1.BackupPhaseCancelled
}

func phaseFromBackupState(s gvdbclient.BackupState) gvdbv1alpha1.GVDBBackupPhase {
	switch s {
	case gvdbclient.BackupStatePending:
		return gvdbv1alpha1.BackupPhasePending
	case gvdbclient.BackupStateRunning:
		return gvdbv1alpha1.BackupPhaseRunning
	case gvdbclient.BackupStateCompleted:
		return gvdbv1alpha1.BackupPhaseCompleted
	case gvdbclient.BackupStateFailed:
		return gvdbv1alpha1.BackupPhaseFailed
	case gvdbclient.BackupStateCancelled:
		return gvdbv1alpha1.BackupPhaseCancelled
	}
	return gvdbv1alpha1.BackupPhasePending
}

// setBackupCondition records a single "Ready" condition. Mutates the slice
// in place via meta.SetStatusCondition's stable-merge semantics.
func setBackupCondition(conds *[]metav1.Condition, status metav1.ConditionStatus,
	reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
}
