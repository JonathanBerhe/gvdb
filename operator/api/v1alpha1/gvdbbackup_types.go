/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LocalObjectReference is a Kubernetes-style same-namespace object reference.
// Defined here (rather than reused from corev1) because v1alpha1 references
// only need the `name` field and we don't want consumers to gain an implicit
// dependency on corev1.LocalObjectReference's optional fields.
type LocalObjectReference struct {
	// Name is the metadata.name of the referenced object.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// S3BackupTarget identifies an S3-compatible bucket the data nodes upload to.
type S3BackupTarget struct {
	// Bucket is the destination bucket. The cluster's data-nodes and
	// coordinator must be configured with credentials for this bucket
	// (typically via IRSA / Workload Identity); the operator does not
	// pass credentials in the CR.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Prefix is an optional key prefix under the bucket. Final layout
	// is `<prefix>/backups/<backup_id>/...`. Empty places artifacts at
	// the bucket root.
	// +optional
	Prefix string `json:"prefix,omitempty"`
}

// LocalBackupTarget identifies a filesystem path the data-nodes can write to.
// The path must lie under the server-side allowlist configured in
// `StorageConfig.local_backup_dir`; the data-node rejects paths that escape it.
type LocalBackupTarget struct {
	// Path is the absolute filesystem path on the data-node pod (typically
	// a PVC mount such as `/var/lib/gvdb/backups`). Must lie under the
	// server-configured allowlist root.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^/.+`
	Path string `json:"path"`
}

// BackupTargetSpec selects where to write a backup. Exactly one of `s3` or
// `local` must be set. Validation is enforced at reconcile time rather than
// via CEL/oneof because kubebuilder v4 still emits oneOf as anyOf in the
// generated CRD and the runtime check is unambiguous.
type BackupTargetSpec struct {
	// +optional
	S3 *S3BackupTarget `json:"s3,omitempty"`
	// +optional
	Local *LocalBackupTarget `json:"local,omitempty"`
}

// GVDBBackupSpec is the desired state of a GVDBBackup.
type GVDBBackupSpec struct {
	// ClusterRef points at the GVDBCluster to back up. The CR must live
	// in the same namespace as the cluster.
	// +kubebuilder:validation:Required
	ClusterRef LocalObjectReference `json:"clusterRef"`

	// Collection is the name of the collection to back up.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Collection string `json:"collection"`

	// Target picks the backup destination. Exactly one of s3 / local
	// must be set; the reconciler rejects empty or doubly-set targets.
	// +kubebuilder:validation:Required
	Target BackupTargetSpec `json:"target"`

	// IncrementalFromRef optionally names a parent GVDBBackup. v1alpha1
	// accepts the field but the C++ runtime currently always performs a
	// full backup; the seam is preserved for a later incremental
	// implementation.
	// +optional
	IncrementalFromRef *LocalObjectReference `json:"incrementalFromRef,omitempty"`

	// RetentionDays caps how long the produced backup artifacts are kept
	// by the operator's reaper. v1alpha1 records the field but reaping
	// lands in a follow-up; 0 means "retain indefinitely".
	// +kubebuilder:default:=0
	// +optional
	RetentionDays int32 `json:"retentionDays,omitempty"`
}

// GVDBBackupPhase is a high-level enum of the backup's lifecycle.
type GVDBBackupPhase string

const (
	BackupPhasePending   GVDBBackupPhase = "Pending"
	BackupPhaseRunning   GVDBBackupPhase = "Running"
	BackupPhaseCompleted GVDBBackupPhase = "Completed"
	BackupPhaseFailed    GVDBBackupPhase = "Failed"
	BackupPhaseCancelled GVDBBackupPhase = "Cancelled"
)

// GVDBBackupStatus is the observed state of a GVDBBackup.
type GVDBBackupStatus struct {
	// ObservedGeneration is the metadata.generation the operator last
	// reconciled. Callers diff this against metadata.generation to
	// detect whether status is fresh.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is the high-level lifecycle stage.
	// +optional
	Phase GVDBBackupPhase `json:"phase,omitempty"`

	// BackupID is the server-allocated id. Set once the first
	// BackupCollection RPC succeeds; reused as the idempotency key on
	// subsequent reconciles so a controller restart doesn't kick off a
	// duplicate backup.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// StartedAt is set on the first successful BackupCollection call.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is set when the backup reaches a terminal state
	// (Completed, Failed, or Cancelled).
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// ShardsTotal is the number of shards in the collection the
	// coordinator is fanning out to. Reported by GetBackupStatus once
	// orchestration is under way.
	// +optional
	ShardsTotal int32 `json:"shardsTotal,omitempty"`

	// ShardsCompleted counts shards whose per-shard manifest has been
	// uploaded successfully.
	// +optional
	ShardsCompleted int32 `json:"shardsCompleted,omitempty"`

	// SizeBytes is the cumulative bytes uploaded so far. Final value is
	// approximate (a few KiB of manifest overhead).
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// ManifestURI is the canonical pointer to the top-level backup
	// manifest, e.g. `s3://bucket/prefix/backups/<id>/backup.manifest.json`.
	// Empty until the backup reaches Completed.
	// +optional
	ManifestURI string `json:"manifestURI,omitempty"`

	// Conditions surfaces the Kubernetes-standard condition set. The
	// reconciler maintains a single "Ready" condition whose status
	// mirrors Phase: True on Completed, False on Failed/Cancelled,
	// Unknown while Pending/Running.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=gvdbbkp
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Collection",type=string,JSONPath=`.spec.collection`
// +kubebuilder:printcolumn:name="Shards",type=string,JSONPath=`.status.shardsCompleted`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.sizeBytes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GVDBBackup captures a point-in-time backup of one collection in a
// GVDBCluster. The reconciler dials the cluster's proxy, calls
// VectorDBService.BackupCollection, then polls GetBackupStatus until the
// job reaches a terminal state. Idempotent across reconciles: an existing
// status.backupID is reused as the client-supplied backup_id so a
// controller restart never kicks off a duplicate run.
type GVDBBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GVDBBackupSpec   `json:"spec,omitempty"`
	Status GVDBBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GVDBBackupList is a list of GVDBBackup resources.
type GVDBBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GVDBBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GVDBBackup{}, &GVDBBackupList{})
}
