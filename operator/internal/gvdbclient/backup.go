/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Backup/restore wrappers around the proxy-fronted VectorDBService client.
// The operator's GVDBBackup and GVDBRestore reconcilers call into these.

package gvdbclient

import (
	"context"
	"fmt"
	"time"

	pb "gvdb/operator/internal/gvdbpb"
)

// BackupTarget is the operator-side mirror of proto.BackupTarget. Exactly
// one of S3 or Local is set; the reconciler is responsible for enforcing
// that. Defined here so callers don't depend on the generated pb types.
type BackupTarget struct {
	S3    *S3Target
	Local *LocalTarget
}

type S3Target struct {
	Bucket string
	Prefix string
}

type LocalTarget struct {
	Path string
}

// BackupState mirrors proto.BackupState. Kept as a small set so callers
// switch on it without importing the generated enum.
type BackupState int

const (
	BackupStatePending   BackupState = 0
	BackupStateRunning   BackupState = 1
	BackupStateCompleted BackupState = 2
	BackupStateFailed    BackupState = 3
	BackupStateCancelled BackupState = 4
)

// BackupStatus is the reconciler-relevant subset of GetBackupStatusResponse.
type BackupStatus struct {
	BackupID        string
	State           BackupState
	ShardsTotal     uint32
	ShardsCompleted uint32
	BytesUploaded   uint64
	ErrorMessage    string
	ElapsedSeconds  float32
	ManifestURI     string
}

// RestoreStatus is the reconciler-relevant subset of GetRestoreStatusResponse.
type RestoreStatus struct {
	RestoreID       string
	State           BackupState
	ShardsTotal     uint32
	ShardsCompleted uint32
	ErrorMessage    string
	ElapsedSeconds  float32
}

// StartBackup invokes BackupCollection on the proxy-fronted service.
// `requestedBackupID` is the idempotency key the reconciler reuses across
// reconciles so a controller restart never produces a duplicate run.
func (c *Client) StartBackup(ctx context.Context, collection string,
	target BackupTarget, requestedBackupID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	t, err := backupTargetToProto(target)
	if err != nil {
		return "", err
	}
	resp, err := c.stub.BackupCollection(ctx, &pb.BackupCollectionRequest{
		CollectionName: collection,
		Target:         t,
		BackupId:       requestedBackupID,
	})
	if err != nil {
		return "", fmt.Errorf("backup collection: %w", err)
	}
	return resp.GetBackupId(), nil
}

// GetBackupStatus polls the proxy for the latest job state.
func (c *Client) GetBackupStatus(ctx context.Context,
	backupID string) (BackupStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.stub.GetBackupStatus(ctx,
		&pb.GetBackupStatusRequest{BackupId: backupID})
	if err != nil {
		return BackupStatus{}, fmt.Errorf("get backup status: %w", err)
	}
	return BackupStatus{
		BackupID:        resp.GetBackupId(),
		State:           BackupState(resp.GetState()),
		ShardsTotal:     resp.GetShardsTotal(),
		ShardsCompleted: resp.GetShardsCompleted(),
		BytesUploaded:   resp.GetBytesUploaded(),
		ErrorMessage:    resp.GetErrorMessage(),
		ElapsedSeconds:  resp.GetElapsedSeconds(),
		ManifestURI:     resp.GetManifestUri(),
	}, nil
}

// CancelBackup asks the server to cancel an in-flight backup. Idempotent
// on terminal jobs (returns NotFound vs OK depending on the server's
// retention of the job entry).
func (c *Client) CancelBackup(ctx context.Context, backupID string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := c.stub.CancelBackup(ctx,
		&pb.CancelBackupRequest{BackupId: backupID}); err != nil {
		return fmt.Errorf("cancel backup: %w", err)
	}
	return nil
}

// StartRestore invokes RestoreCollection on the proxy-fronted service.
func (c *Client) StartRestore(ctx context.Context, source BackupTarget,
	backupID, targetCollection string, overwrite bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	t, err := backupTargetToProto(source)
	if err != nil {
		return "", err
	}
	resp, err := c.stub.RestoreCollection(ctx, &pb.RestoreCollectionRequest{
		Source:               t,
		BackupId:             backupID,
		TargetCollectionName: targetCollection,
		Overwrite:            overwrite,
	})
	if err != nil {
		return "", fmt.Errorf("restore collection: %w", err)
	}
	return resp.GetRestoreId(), nil
}

// GetRestoreStatus polls the proxy for the latest restore job state.
func (c *Client) GetRestoreStatus(ctx context.Context,
	restoreID string) (RestoreStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.stub.GetRestoreStatus(ctx,
		&pb.GetRestoreStatusRequest{RestoreId: restoreID})
	if err != nil {
		return RestoreStatus{}, fmt.Errorf("get restore status: %w", err)
	}
	return RestoreStatus{
		RestoreID:       resp.GetRestoreId(),
		State:           BackupState(resp.GetState()),
		ShardsTotal:     resp.GetShardsTotal(),
		ShardsCompleted: resp.GetShardsCompleted(),
		ErrorMessage:    resp.GetErrorMessage(),
		ElapsedSeconds:  resp.GetElapsedSeconds(),
	}, nil
}

// IsTerminal reports whether the state is a final lifecycle state. The
// reconciler stops polling and sets CompletedAt once IsTerminal returns
// true.
func (s BackupState) IsTerminal() bool {
	return s == BackupStateCompleted ||
		s == BackupStateFailed ||
		s == BackupStateCancelled
}

func backupTargetToProto(t BackupTarget) (*pb.BackupTarget, error) {
	if t.S3 != nil && t.Local != nil {
		return nil, fmt.Errorf("BackupTarget has both s3 and local set")
	}
	if t.S3 == nil && t.Local == nil {
		return nil, fmt.Errorf("BackupTarget has neither s3 nor local set")
	}
	out := &pb.BackupTarget{}
	if t.S3 != nil {
		out.Target = &pb.BackupTarget_S3{S3: &pb.S3Target{
			Bucket: t.S3.Bucket,
			Prefix: t.S3.Prefix,
		}}
	} else {
		out.Target = &pb.BackupTarget_Local{Local: &pb.LocalTarget{
			Path: t.Local.Path,
		}}
	}
	return out, nil
}
