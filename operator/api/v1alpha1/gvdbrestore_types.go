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

// GVDBRestoreMode controls behavior when the target collection already exists.
// +kubebuilder:validation:Enum=NewCollection;Overwrite
type GVDBRestoreMode string

const (
	// RestoreModeNewCollection requires the target collection to NOT exist.
	// The reconciler creates it from the backup manifest's recorded shape.
	RestoreModeNewCollection GVDBRestoreMode = "NewCollection"

	// RestoreModeOverwrite drops the existing collection (if any) before
	// recreating it from the backup. The coordinator-side restore
	// orchestrator handles drop-and-recreate atomically w.r.t. proxy
	// routing — in-flight writes return NotFound between the drop and
	// re-create and clients retry.
	RestoreModeOverwrite GVDBRestoreMode = "Overwrite"
)

// GVDBRestoreSpec is the desired state of a GVDBRestore.
type GVDBRestoreSpec struct {
	// ClusterRef points at the GVDBCluster to restore into. Same-namespace.
	// +kubebuilder:validation:Required
	ClusterRef LocalObjectReference `json:"clusterRef"`

	// FromBackupRef optionally names a sibling GVDBBackup. When set, the
	// reconciler dereferences it to its Target+BackupID, so an operator
	// can restore by saying "this Backup CR" instead of pasting the
	// target+id manually. Mutually exclusive with `target` + `backupID`
	// below.
	// +optional
	FromBackupRef *LocalObjectReference `json:"fromBackupRef,omitempty"`

	// Target identifies where the backup artifacts live. Set this for
	// DR scenarios where the backup was produced by a different cluster
	// (and so there's no GVDBBackup CR to reference). Mutually exclusive
	// with FromBackupRef.
	// +optional
	Target *BackupTargetSpec `json:"target,omitempty"`

	// BackupID is the server-allocated id from the source backup. Set
	// alongside Target for DR scenarios. Mutually exclusive with
	// FromBackupRef.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// TargetCollection names the collection to restore into. Empty
	// means "use the collection name from the backup manifest".
	// +optional
	TargetCollection string `json:"targetCollection,omitempty"`

	// Mode controls drop-and-recreate behavior when the target name
	// already exists. Defaults to NewCollection (fail on conflict).
	// +kubebuilder:default:=NewCollection
	// +optional
	Mode GVDBRestoreMode `json:"mode,omitempty"`
}

// GVDBRestoreStatus is the observed state of a GVDBRestore.
type GVDBRestoreStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase reuses GVDBBackupPhase values (Pending / Running / Completed /
	// Failed). Cancelled is not produced for restores — once the
	// coordinator's StartRestoreDistributed returns a restore_id, the
	// operator polls until terminal.
	// +optional
	Phase GVDBBackupPhase `json:"phase,omitempty"`

	// RestoreID is the server-allocated id, set once StartRestore succeeds.
	// +optional
	RestoreID string `json:"restoreID,omitempty"`

	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// +optional
	ShardsTotal int32 `json:"shardsTotal,omitempty"`
	// +optional
	ShardsCompleted int32 `json:"shardsCompleted,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=gvdbrst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="BackupID",type=string,JSONPath=`.status.restoreID`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetCollection`
// +kubebuilder:printcolumn:name="Shards",type=string,JSONPath=`.status.shardsCompleted`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GVDBRestore materializes a previously-captured GVDBBackup into a
// collection in a GVDBCluster. The reconciler dereferences the source
// (a sibling GVDBBackup or an explicit target+backup_id), dials the
// cluster's proxy, calls RestoreCollection, and polls GetRestoreStatus
// until terminal.
type GVDBRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GVDBRestoreSpec   `json:"spec,omitempty"`
	Status GVDBRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GVDBRestoreList is a list of GVDBRestore resources.
type GVDBRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GVDBRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GVDBRestore{}, &GVDBRestoreList{})
}
