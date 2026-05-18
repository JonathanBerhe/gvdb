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
)

const restorePollInterval = 5 * time.Second

// GVDBRestoreReconciler reconciles a GVDBRestore object by driving a
// restore against the referenced GVDBCluster's proxy. State machine
// mirrors GVDBBackup: first reconcile starts the job, subsequent ones
// poll, terminal states stop the requeue.
type GVDBRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	StatsPool *gvdbclient.Pool
}

// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=gvdb.io,resources=gvdbbackups,verbs=get;list;watch

// Reconcile drives the GVDBRestore CR toward its desired terminal state.
func (r *GVDBRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rst gvdbv1alpha1.GVDBRestore
	if err := r.Get(ctx, req.NamespacedName, &rst); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if isTerminalBackupPhase(rst.Status.Phase) {
		if rst.Status.ObservedGeneration != rst.Generation {
			rst.Status.ObservedGeneration = rst.Generation
			return ctrl.Result{}, r.Status().Update(ctx, &rst)
		}
		return ctrl.Result{}, nil
	}

	// Resolve the source: either dereference FromBackupRef or use the
	// explicit Target + BackupID. Exactly-one is enforced here.
	target, backupID, err := r.resolveSource(ctx, &rst)
	if err != nil {
		return r.failRestore(ctx, &rst, "InvalidSource", err.Error())
	}

	cluster, err := r.fetchCluster(ctx, &rst)
	if err != nil {
		return r.failRestore(ctx, &rst, "ClusterNotFound", err.Error())
	}
	cli, err := r.StatsPool.Get(proxyAddress(cluster))
	if err != nil {
		log.Error(err, "Dial proxy failed")
		return ctrl.Result{RequeueAfter: restorePollInterval}, nil
	}

	mode := rst.Spec.Mode
	if mode == "" {
		mode = gvdbv1alpha1.RestoreModeNewCollection
	}
	overwrite := mode == gvdbv1alpha1.RestoreModeOverwrite

	if rst.Status.RestoreID == "" {
		restoreID, err := cli.StartRestore(ctx, target, backupID,
			rst.Spec.TargetCollection, overwrite)
		if err != nil {
			log.Error(err, "StartRestore failed")
			// Same startup-race rationale as backup: proxy pod can be
			// Ready while the service endpoint isn't programmed yet.
			if isTransientGRPCError(err) {
				return ctrl.Result{RequeueAfter: restorePollInterval}, nil
			}
			return r.failRestore(ctx, &rst, "StartRestoreFailed", err.Error())
		}
		rst.Status.RestoreID = restoreID
		rst.Status.Phase = gvdbv1alpha1.BackupPhaseRunning
		now := metav1.NewTime(time.Now())
		rst.Status.StartedAt = &now
		rst.Status.ObservedGeneration = rst.Generation
		setBackupCondition(&rst.Status.Conditions, metav1.ConditionUnknown,
			"RestoreRunning", "Restore job started: "+restoreID)
		if err := r.Status().Update(ctx, &rst); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: restorePollInterval}, nil
	}

	status, err := cli.GetRestoreStatus(ctx, rst.Status.RestoreID)
	if err != nil {
		log.Error(err, "GetRestoreStatus failed",
			"restoreID", rst.Status.RestoreID)
		if isTransientGRPCError(err) {
			return ctrl.Result{RequeueAfter: restorePollInterval}, nil
		}
		return r.failRestore(ctx, &rst, "StatusUnavailable", err.Error())
	}

	prevPhase := rst.Status.Phase
	rst.Status.Phase = phaseFromBackupState(status.State)
	//nolint:gosec
	rst.Status.ShardsTotal = int32(status.ShardsTotal)
	//nolint:gosec
	rst.Status.ShardsCompleted = int32(status.ShardsCompleted)
	rst.Status.ObservedGeneration = rst.Generation

	if status.State.IsTerminal() {
		now := metav1.NewTime(time.Now())
		rst.Status.CompletedAt = &now
		switch status.State {
		case gvdbclient.BackupStateCompleted:
			setBackupCondition(&rst.Status.Conditions, metav1.ConditionTrue,
				"RestoreCompleted",
				fmt.Sprintf("Restore completed in %.1fs", status.ElapsedSeconds))
		case gvdbclient.BackupStateFailed:
			msg := status.ErrorMessage
			if msg == "" {
				msg = "server reported FAILED with no error message"
			}
			setBackupCondition(&rst.Status.Conditions, metav1.ConditionFalse,
				"RestoreFailed", msg)
		default:
			setBackupCondition(&rst.Status.Conditions, metav1.ConditionFalse,
				"RestoreCancelled", "Restore terminated abnormally")
		}
	} else if prevPhase != rst.Status.Phase {
		setBackupCondition(&rst.Status.Conditions, metav1.ConditionUnknown,
			"RestoreRunning",
			fmt.Sprintf("Restore running (%d/%d shards complete)",
				status.ShardsCompleted, status.ShardsTotal))
	}

	if err := r.Status().Update(ctx, &rst); err != nil {
		return ctrl.Result{}, err
	}

	if status.State.IsTerminal() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: restorePollInterval}, nil
}

// SetupWithManager wires the reconciler.
func (r *GVDBRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gvdbv1alpha1.GVDBRestore{}).
		Named("gvdbrestore").
		Complete(r)
}

// =============================================================================
// Helpers
// =============================================================================

func (r *GVDBRestoreReconciler) fetchCluster(ctx context.Context,
	rst *gvdbv1alpha1.GVDBRestore) (*gvdbv1alpha1.GVDBCluster, error) {
	var cluster gvdbv1alpha1.GVDBCluster
	key := types.NamespacedName{
		Namespace: rst.Namespace,
		Name:      rst.Spec.ClusterRef.Name,
	}
	if err := r.Get(ctx, key, &cluster); err != nil {
		return nil, fmt.Errorf("GVDBCluster %s: %w", key, err)
	}
	return &cluster, nil
}

// resolveSource derefs FromBackupRef when set; otherwise uses the
// explicit Target + BackupID. Exactly-one is required.
func (r *GVDBRestoreReconciler) resolveSource(ctx context.Context,
	rst *gvdbv1alpha1.GVDBRestore) (gvdbclient.BackupTarget, string, error) {
	hasRef := rst.Spec.FromBackupRef != nil
	hasExplicit := rst.Spec.Target != nil || rst.Spec.BackupID != ""
	if hasRef && hasExplicit {
		return gvdbclient.BackupTarget{}, "",
			fmt.Errorf("spec.fromBackupRef and spec.target/backupID are mutually exclusive")
	}
	if !hasRef && !hasExplicit {
		return gvdbclient.BackupTarget{}, "",
			fmt.Errorf("spec must set fromBackupRef or both target and backupID")
	}
	if hasRef {
		var src gvdbv1alpha1.GVDBBackup
		key := types.NamespacedName{
			Namespace: rst.Namespace,
			Name:      rst.Spec.FromBackupRef.Name,
		}
		if err := r.Get(ctx, key, &src); err != nil {
			return gvdbclient.BackupTarget{}, "",
				fmt.Errorf("GVDBBackup %s: %w", key, err)
		}
		if src.Status.BackupID == "" {
			return gvdbclient.BackupTarget{}, "",
				fmt.Errorf("referenced GVDBBackup %s has no status.backupID yet", key)
		}
		if err := validateBackupTarget(src.Spec.Target); err != nil {
			return gvdbclient.BackupTarget{}, "", err
		}
		return backupTargetFromCR(src.Spec.Target), src.Status.BackupID, nil
	}
	if rst.Spec.Target == nil {
		return gvdbclient.BackupTarget{}, "",
			fmt.Errorf("spec.target is required when spec.backupID is set")
	}
	if rst.Spec.BackupID == "" {
		return gvdbclient.BackupTarget{}, "",
			fmt.Errorf("spec.backupID is required when spec.target is set")
	}
	if err := validateBackupTarget(*rst.Spec.Target); err != nil {
		return gvdbclient.BackupTarget{}, "", err
	}
	return backupTargetFromCR(*rst.Spec.Target), rst.Spec.BackupID, nil
}

func (r *GVDBRestoreReconciler) failRestore(ctx context.Context,
	rst *gvdbv1alpha1.GVDBRestore, reason, message string) (ctrl.Result, error) {
	rst.Status.Phase = gvdbv1alpha1.BackupPhaseFailed
	rst.Status.ObservedGeneration = rst.Generation
	now := metav1.NewTime(time.Now())
	rst.Status.CompletedAt = &now
	setBackupCondition(&rst.Status.Conditions, metav1.ConditionFalse,
		reason, message)
	if err := r.Status().Update(ctx, rst); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// Used to silence the linter when meta package is referenced through
// setBackupCondition (defined in the backup controller's file).
var _ = meta.SetStatusCondition
