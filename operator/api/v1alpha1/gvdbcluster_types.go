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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImageSpec selects the container image used for every GVDB workload in the
// cluster. Mirrors the `image:` block in deploy/helm/gvdb/values.yaml.
type ImageSpec struct {
	// repository is the image repository, e.g. "ghcr.io/jonathanberhe/gvdb".
	// +kubebuilder:default:=gvdb
	// +optional
	Repository string `json:"repository,omitempty"`

	// tag overrides the image tag. When empty, the operator uses its own
	// release version (lockstep with GVDB core).
	// +optional
	Tag string `json:"tag,omitempty"`

	// pullPolicy for the workload containers.
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default:=IfNotPresent
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// StorageSpec configures a PersistentVolumeClaim template for a StatefulSet.
type StorageSpec struct {
	// size is the requested volume size (e.g. "5Gi").
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// storageClassName selects a StorageClass. Empty uses the cluster default.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
}

// AntiAffinitySpec configures pod anti-affinity for a workload.
type AntiAffinitySpec struct {
	// enabled turns on anti-affinity.
	// +kubebuilder:default:=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// type is "preferred" (soft) or "required" (hard).
	// +kubebuilder:validation:Enum=preferred;required
	// +kubebuilder:default:=preferred
	// +optional
	Type string `json:"type,omitempty"`

	// topologyKey defaults to "kubernetes.io/hostname".
	// +kubebuilder:default:="kubernetes.io/hostname"
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`
}

// PodDisruptionBudgetSpec configures an optional PDB for a workload.
type PodDisruptionBudgetSpec struct {
	// enabled turns the PDB on.
	// +kubebuilder:default:=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// minAvailable is the minimum number of pods that must remain available.
	// +kubebuilder:default:=1
	// +optional
	MinAvailable int32 `json:"minAvailable,omitempty"`
}

// ServiceAccountSpec configures a per-workload ServiceAccount, typically with
// cloud-IAM annotations (IRSA, GCP Workload Identity, Azure WI).
type ServiceAccountSpec struct {
	// create enables a per-workload ServiceAccount.
	// +kubebuilder:default:=false
	// +optional
	Create bool `json:"create,omitempty"`

	// name overrides the default "<release>-<component>" name.
	// +optional
	Name string `json:"name,omitempty"`

	// annotations attached to the ServiceAccount (e.g. IRSA role ARN).
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// WorkloadCommon groups spec fields shared by every workload (coordinator,
// data node, query node, proxy). The 0b.5 hardening primitives live here so
// each component can opt in independently.
type WorkloadCommon struct {
	// resources is the container resource requirements.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// podAntiAffinity configures pod anti-affinity.
	// +optional
	PodAntiAffinity AntiAffinitySpec `json:"podAntiAffinity,omitempty"`

	// topologySpreadConstraints are passed through to the pod spec verbatim.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// podDisruptionBudget optionally creates a PDB for the workload.
	// +optional
	PodDisruptionBudget PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// serviceAccount optionally creates a per-workload ServiceAccount.
	// +optional
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`

	// priorityClassName attaches the pod to a PriorityClass. Empty uses none.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// CoordinatorSpec defines the coordinator workload.
type CoordinatorSpec struct {
	WorkloadCommon `json:",inline"`

	// replicas is the number of coordinator pods. Use 1 for single-node, or
	// 3/5/7 for Raft HA.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	// +kubebuilder:default:=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// storage configures the coordinator's PersistentVolumeClaim template.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
}

// DataNodeSpec defines the data node workload.
type DataNodeSpec struct {
	WorkloadCommon `json:",inline"`

	// replicas is the number of data-node pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=2
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// memoryLimitGb is the in-process memory ceiling for segment caches (GiB).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=4
	// +optional
	MemoryLimitGb int32 `json:"memoryLimitGb,omitempty"`

	// terminationGracePeriodSeconds is the drain window for DRAINING heartbeat
	// + shard migration before the pod is SIGKILLed.
	// +kubebuilder:default:=60
	// +optional
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// storage configures the data-node's PersistentVolumeClaim template.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
}

// QueryNodeSpec defines the query node workload.
type QueryNodeSpec struct {
	WorkloadCommon `json:",inline"`

	// replicas is the number of query-node pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// memoryLimitGb is the in-process memory ceiling for the query cache (GiB).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=4
	// +optional
	MemoryLimitGb int32 `json:"memoryLimitGb,omitempty"`

	// terminationGracePeriodSeconds controls shutdown timing.
	// +kubebuilder:default:=30
	// +optional
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// storage configures the query-node's PersistentVolumeClaim template.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
}

// ProxyServiceSpec configures the proxy Service object.
type ProxyServiceSpec struct {
	// type is the Service type ("ClusterIP" or "NodePort").
	// +kubebuilder:validation:Enum=ClusterIP;NodePort
	// +kubebuilder:default:=ClusterIP
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// port is the Service port.
	// +kubebuilder:default:=50050
	// +optional
	Port int32 `json:"port,omitempty"`

	// nodePort pins a NodePort when type is NodePort.
	// +optional
	NodePort int32 `json:"nodePort,omitempty"`
}

// ProxySpec defines the stateless proxy workload (Deployment).
type ProxySpec struct {
	WorkloadCommon `json:",inline"`

	// replicas is the number of proxy pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// service configures the proxy Service object.
	// +optional
	Service ProxyServiceSpec `json:"service,omitempty"`
}

// SecuritySpec holds pod + container security contexts applied to every
// workload. Mirrors the opt-in primitives introduced in roadmap 0b.5.
type SecuritySpec struct {
	// podSecurityContext is applied to every pod.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// containerSecurityContext is applied to every container.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`
}

// PriorityClassLevel defines a single PriorityClass the operator manages.
type PriorityClassLevel struct {
	// value is the numeric priority (higher = more important).
	// +kubebuilder:validation:Minimum=0
	// +optional
	Value int32 `json:"value,omitempty"`
}

// PriorityClassesSpec opts into cluster-scoped PriorityClass creation for
// each workload type. Names are derived from the release + namespace to
// avoid cross-install collisions (matches the Helm chart convention).
type PriorityClassesSpec struct {
	// create turns on PriorityClass generation.
	// +kubebuilder:default:=false
	// +optional
	Create bool `json:"create,omitempty"`

	// coordinator priority level. Default 1000000.
	// +optional
	Coordinator PriorityClassLevel `json:"coordinator,omitempty"`

	// dataNode priority level. Default 900000.
	// +optional
	DataNode PriorityClassLevel `json:"dataNode,omitempty"`

	// queryNode priority level. Default 800000.
	// +optional
	QueryNode PriorityClassLevel `json:"queryNode,omitempty"`

	// proxy priority level. Default 700000.
	// +optional
	Proxy PriorityClassLevel `json:"proxy,omitempty"`
}

// ServerConfig mirrors the `server:` section of the ConfigMap.
type ServerConfig struct {
	// +kubebuilder:default:=256
	// +optional
	MaxMessageSizeMb int32 `json:"maxMessageSizeMb,omitempty"`

	// +kubebuilder:default:=1000
	// +optional
	MaxConcurrentStreams int32 `json:"maxConcurrentStreams,omitempty"`
}

// StorageConfig mirrors the `storage:` section of the ConfigMap.
type StorageConfig struct {
	// +kubebuilder:default:=512
	// +optional
	SegmentMaxSizeMb int32 `json:"segmentMaxSizeMb,omitempty"`

	// +kubebuilder:default:=64
	// +optional
	WalBufferSizeMb int32 `json:"walBufferSizeMb,omitempty"`

	// +kubebuilder:default:=true
	// +optional
	EnableCompression bool `json:"enableCompression,omitempty"`

	// +kubebuilder:default:=2
	// +optional
	CompactionThreads int32 `json:"compactionThreads,omitempty"`
}

// IndexConfig mirrors the `index:` section of the ConfigMap.
type IndexConfig struct {
	// +kubebuilder:default:="HNSW"
	// +optional
	DefaultIndexType string `json:"defaultIndexType,omitempty"`

	// +kubebuilder:default:=16
	// +optional
	HnswM int32 `json:"hnswM,omitempty"`

	// +kubebuilder:default:=200
	// +optional
	HnswEfConstruction int32 `json:"hnswEfConstruction,omitempty"`

	// +kubebuilder:default:=100
	// +optional
	HnswEfSearch int32 `json:"hnswEfSearch,omitempty"`
}

// LoggingConfig mirrors the `logging:` section of the ConfigMap.
type LoggingConfig struct {
	// +kubebuilder:validation:Enum=trace;debug;info;warn;error
	// +kubebuilder:default:="info"
	// +optional
	Level string `json:"level,omitempty"`

	// +kubebuilder:default:=true
	// +optional
	ConsoleEnabled bool `json:"consoleEnabled,omitempty"`

	// +kubebuilder:default:=false
	// +optional
	FileEnabled bool `json:"fileEnabled,omitempty"`
}

// ConfigSpec is the server-side GVDB configuration rendered into a ConfigMap
// and mounted at /etc/gvdb/config.yaml.
type ConfigSpec struct {
	// +optional
	Server ServerConfig `json:"server,omitempty"`

	// +optional
	Storage StorageConfig `json:"storage,omitempty"`

	// +optional
	Index IndexConfig `json:"index,omitempty"`

	// +optional
	Logging LoggingConfig `json:"logging,omitempty"`
}

// GVDBClusterSpec is the declarative spec for a GVDB cluster.
type GVDBClusterSpec struct {
	// image configures the container image for every workload.
	// +optional
	Image ImageSpec `json:"image,omitempty"`

	// clusterDomain is the Kubernetes cluster DNS domain used when building
	// pod FQDNs for Raft peer seeding.
	// +kubebuilder:default:="cluster.local"
	// +optional
	ClusterDomain string `json:"clusterDomain,omitempty"`

	// coordinator is the Raft-backed metadata leader workload.
	// +optional
	Coordinator CoordinatorSpec `json:"coordinator,omitempty"`

	// dataNode is the segment-storage workload.
	// +optional
	DataNode DataNodeSpec `json:"dataNode,omitempty"`

	// queryNode is the query-cache fan-out workload.
	// +optional
	QueryNode QueryNodeSpec `json:"queryNode,omitempty"`

	// proxy is the stateless gRPC gateway workload.
	// +optional
	Proxy ProxySpec `json:"proxy,omitempty"`

	// security holds the pod + container security contexts applied to every
	// workload (roadmap 0b.5).
	// +optional
	Security SecuritySpec `json:"security,omitempty"`

	// priorityClasses opts into cluster-scoped PriorityClass creation.
	// +optional
	PriorityClasses PriorityClassesSpec `json:"priorityClasses,omitempty"`

	// config is the server-side GVDB configuration file contents.
	// +optional
	Config ConfigSpec `json:"config,omitempty"`
}

// GVDBClusterPhase is a high-level summary of the cluster state. Detailed
// state lives in status.conditions[]; phase exists for quick `kubectl get`
// output. Valid values: "Pending", "Ready", "Degraded", "Failed".
// +kubebuilder:validation:Enum=Pending;Ready;Degraded;Failed
type GVDBClusterPhase string

const (
	// PhasePending: workloads are being created or converging.
	PhasePending GVDBClusterPhase = "Pending"
	// PhaseReady: all workloads report ready replica counts matching spec.
	PhaseReady GVDBClusterPhase = "Ready"
	// PhaseDegraded: at least one workload has fewer ready replicas than spec.
	PhaseDegraded GVDBClusterPhase = "Degraded"
	// PhaseFailed: reconciliation produced a terminal error (e.g. resource
	// conflict with an unmanaged Helm release).
	PhaseFailed GVDBClusterPhase = "Failed"
)

// Standard condition types surfaced on GVDBCluster.status.conditions.
const (
	// ConditionAvailable: true when the cluster is serving queries.
	ConditionAvailable = "Available"
	// ConditionProgressing: true during scale / upgrade in-flight.
	ConditionProgressing = "Progressing"
	// ConditionDegraded: true when one or more workloads are unhealthy.
	ConditionDegraded = "Degraded"
)

// WorkloadStatus reports desired vs ready replicas for one workload.
type WorkloadStatus struct {
	// desired is the spec replica count.
	Desired int32 `json:"desired"`

	// ready is the count of Ready pods (from StatefulSet / Deployment status).
	Ready int32 `json:"ready"`
}

// NodeCountStatus reports per-workload replica status.
type NodeCountStatus struct {
	// +optional
	Coordinator WorkloadStatus `json:"coordinator,omitempty"`
	// +optional
	DataNode WorkloadStatus `json:"dataNode,omitempty"`
	// +optional
	QueryNode WorkloadStatus `json:"queryNode,omitempty"`
	// +optional
	Proxy WorkloadStatus `json:"proxy,omitempty"`
}

// GVDBClusterStatus is the observed state of a GVDBCluster.
type GVDBClusterStatus struct {
	// observedGeneration is the .metadata.generation the operator most
	// recently reconciled against. Callers use this with conditions to know
	// whether status is fresh.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a high-level cluster status summary (see GVDBClusterPhase).
	// +optional
	Phase GVDBClusterPhase `json:"phase,omitempty"`

	// conditions is the Kubernetes-standard condition set. Types include
	// "Available", "Progressing", "Degraded".
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// nodeCounts reports per-workload desired vs ready replicas.
	// +optional
	NodeCounts NodeCountStatus `json:"nodeCounts,omitempty"`

	// collectionCount is the number of collections in the cluster, queried
	// via VectorDBService.ListCollections.
	// +optional
	CollectionCount int32 `json:"collectionCount,omitempty"`

	// totalVectors is the cluster-wide vector count, from VectorDBService.GetStats.
	// +optional
	TotalVectors int64 `json:"totalVectors,omitempty"`

	// coordinatorLeader names the pod currently holding the Raft leader role,
	// queried from the coordinator via GetLeaderInfo. Empty while no leader
	// is elected (e.g. during bootstrap or mid-rollout re-election). Format
	// is the pod name under the ordinal-based convention (e.g.
	// "prod-coordinator-1"); may fall back to an IP if NodeRegistry can
	// resolve it server-side.
	// +optional
	CoordinatorLeader string `json:"coordinatorLeader,omitempty"`

	// lastRebalance is the timestamp of the most recent shard rebalance on
	// this coordinator's watch. Populated from
	// GetClusterHealthResponse.last_rebalance_unix_ms; nil when no rebalance
	// has fired since coordinator startup.
	// +optional
	LastRebalance *metav1.Time `json:"lastRebalance,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=gvdb;gvdbcluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Coordinator",type=integer,JSONPath=`.status.nodeCounts.coordinator.ready`
// +kubebuilder:printcolumn:name="DataNode",type=integer,JSONPath=`.status.nodeCounts.dataNode.ready`
// +kubebuilder:printcolumn:name="QueryNode",type=integer,JSONPath=`.status.nodeCounts.queryNode.ready`
// +kubebuilder:printcolumn:name="Collections",type=integer,JSONPath=`.status.collectionCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GVDBCluster is the declarative spec for a GVDB distributed vector database
// cluster. Applying a GVDBCluster yields the same Kubernetes topology the
// deploy/helm/gvdb chart produces — StatefulSets for coordinator / data / query
// nodes, a Deployment for the stateless proxy, headless Services for peer
// discovery, a ConfigMap for server config, and the opt-in 0b.5 hardening
// primitives (PDBs, ServiceAccounts, PriorityClasses).
type GVDBCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of GVDBCluster
	// +optional
	Spec GVDBClusterSpec `json:"spec,omitempty"`

	// status defines the observed state of GVDBCluster
	// +optional
	Status GVDBClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GVDBClusterList is a list of GVDBCluster resources.
type GVDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GVDBCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GVDBCluster{}, &GVDBClusterList{})
}
