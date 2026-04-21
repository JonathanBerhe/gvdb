/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package render translates a GVDBCluster CR into the same Kubernetes objects
// deploy/helm/gvdb produces. The naming and labeling conventions here mirror
// the Helm chart's _helpers.tpl so a Helm-installed and an operator-installed
// cluster are structurally indistinguishable (snapshot-diffed in tests).
package render

import (
	"fmt"
	"strings"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
)

// Options configure how rendering produces objects. The reconciler fills this
// in once per reconcile pass so package-level mutable state isn't needed.
type Options struct {
	// DefaultImageTag is used when spec.image.tag is empty. The reconciler
	// sets this from its build-time version (lockstep with GVDB core).
	DefaultImageTag string

	// ConfigHash is a deterministic hash of the rendered ConfigMap contents.
	// It's stamped onto workload pod templates as an annotation so a change
	// to spec.config rolls pods — otherwise ConfigMap edits silently don't
	// take effect until pods happen to restart.
	ConfigHash string
}

// Component is a stable identifier for each workload type used in names,
// labels, and port lookups. String value matches the Helm `app:` label suffix.
type Component string

const (
	CoordinatorComponent Component = "coordinator"
	DataNodeComponent    Component = "data-node"
	QueryNodeComponent   Component = "query-node"
	ProxyComponent       Component = "proxy"
)

// Port numbers mirror the Helm chart (deploy/helm/gvdb/templates/*).
const (
	CoordinatorGRPCPort    = 50051
	CoordinatorRaftPort    = 8300
	CoordinatorMetricsPort = 9091
	DataNodeGRPCPort       = 50060
	QueryNodeGRPCPort      = 50070
	ProxyGRPCPort          = 50050
	ProxyMetricsPort       = 9050
)

// Annotation key set on pod templates so a ConfigMap content change triggers
// a rolling restart of the dependent workload.
const ConfigHashAnnotation = "gvdb.io/config-hash"

// Default replica counts mirror deploy/helm/gvdb/values.yaml. They kick in
// when the CR leaves the field unset (CRD Minimum=1 + default:=N guard the
// K8s API path; these guard the Go call path used by tests and the
// reconciler's status computation).
const (
	defaultCoordinatorReplicas = 1
	defaultDataNodeReplicas    = 2
	defaultQueryNodeReplicas   = 1
	defaultProxyReplicas       = 1
)

// EffectiveReplicas returns the replica count the operator will actually
// render for the workload — spec value if set, else the default. Using this
// from both the render layer and the reconciler's status computation keeps
// the two views consistent.
func EffectiveReplicas(cluster *gvdbv1alpha1.GVDBCluster, c Component) int32 {
	switch c {
	case CoordinatorComponent:
		if v := cluster.Spec.Coordinator.Replicas; v > 0 {
			return v
		}
		return defaultCoordinatorReplicas
	case DataNodeComponent:
		if v := cluster.Spec.DataNode.Replicas; v > 0 {
			return v
		}
		return defaultDataNodeReplicas
	case QueryNodeComponent:
		if v := cluster.Spec.QueryNode.Replicas; v > 0 {
			return v
		}
		return defaultQueryNodeReplicas
	case ProxyComponent:
		if v := cluster.Spec.Proxy.Replicas; v > 0 {
			return v
		}
		return defaultProxyReplicas
	}
	return 0
}

// FullName truncates the CR name to 63 chars, matching the Helm fullname helper.
func FullName(cluster *gvdbv1alpha1.GVDBCluster) string {
	n := cluster.Name
	if len(n) > 63 {
		n = n[:63]
	}
	return strings.TrimSuffix(n, "-")
}

// WorkloadName returns "<release>-<component>", matching the Helm chart.
func WorkloadName(cluster *gvdbv1alpha1.GVDBCluster, c Component) string {
	return fmt.Sprintf("%s-%s", FullName(cluster), c)
}

// ConfigMapName returns "<release>-config".
func ConfigMapName(cluster *gvdbv1alpha1.GVDBCluster) string {
	return FullName(cluster) + "-config"
}

// ImageRef builds "<repo>:<tag>". Tag falls back to opts.DefaultImageTag
// (the operator's own release version — lockstep with GVDB core).
func ImageRef(cluster *gvdbv1alpha1.GVDBCluster, opts Options) string {
	repo := cluster.Spec.Image.Repository
	if repo == "" {
		repo = "gvdb"
	}
	tag := cluster.Spec.Image.Tag
	if tag == "" {
		tag = opts.DefaultImageTag
	}
	if tag == "" {
		// Last-resort sentinel for tests that don't set opts.
		tag = "latest"
	}
	return repo + ":" + tag
}

// ClusterDomain returns the configured cluster DNS domain, defaulting to
// "cluster.local" for parity with the Helm chart.
func ClusterDomain(cluster *gvdbv1alpha1.GVDBCluster) string {
	if cluster.Spec.ClusterDomain == "" {
		return "cluster.local"
	}
	return cluster.Spec.ClusterDomain
}

// podFQDN builds "<pod>.<svc>.<ns>.svc.<domain>".
func podFQDN(cluster *gvdbv1alpha1.GVDBCluster, c Component, ordinal int32) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.%s",
		WorkloadName(cluster, c),
		ordinal,
		WorkloadName(cluster, c),
		cluster.Namespace,
		ClusterDomain(cluster),
	)
}

// CoordinatorAddress returns the pod-0 gRPC address for clients and peers.
// Mirrors the gvdb.coordinator.address helper.
func CoordinatorAddress(cluster *gvdbv1alpha1.GVDBCluster) string {
	return fmt.Sprintf("%s:%d", podFQDN(cluster, CoordinatorComponent, 0), CoordinatorGRPCPort)
}

// CoordinatorRaftPeers builds the "id:host:port" Raft peer list that seeds
// the multi-node cluster config on every coordinator pod. Matches the
// gvdb.coordinator.raftPeers Helm helper (see roadmap 0b.4).
func CoordinatorRaftPeers(cluster *gvdbv1alpha1.GVDBCluster) string {
	replicas := EffectiveReplicas(cluster, CoordinatorComponent)
	parts := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		parts = append(parts, fmt.Sprintf("%d:%s:%d",
			i+1, // NuRaft requires node_id > 0 — ordinal 0 becomes id 1.
			podFQDN(cluster, CoordinatorComponent, i),
			CoordinatorRaftPort,
		))
	}
	return strings.Join(parts, ",")
}

// DataNodeAddresses returns a comma-separated list of data-node pod addresses
// for the proxy --data-nodes flag. Mirrors gvdb.dataNode.addresses.
func DataNodeAddresses(cluster *gvdbv1alpha1.GVDBCluster) string {
	return workloadAddresses(cluster, DataNodeComponent, EffectiveReplicas(cluster, DataNodeComponent), DataNodeGRPCPort)
}

// QueryNodeAddresses returns a comma-separated list of query-node pod addresses
// for the proxy --query-nodes flag. Mirrors gvdb.queryNode.addresses.
func QueryNodeAddresses(cluster *gvdbv1alpha1.GVDBCluster) string {
	return workloadAddresses(cluster, QueryNodeComponent, EffectiveReplicas(cluster, QueryNodeComponent), QueryNodeGRPCPort)
}

func workloadAddresses(cluster *gvdbv1alpha1.GVDBCluster, c Component, replicas int32, port int) string {
	parts := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		parts = append(parts, fmt.Sprintf("%s:%d", podFQDN(cluster, c, i), port))
	}
	return strings.Join(parts, ",")
}

// Labels returns the standard label set for a workload.
// Matches gvdb.<component>.labels.
func Labels(cluster *gvdbv1alpha1.GVDBCluster, c Component) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "gvdb-operator",
		"app.kubernetes.io/part-of":    "gvdb",
		"app":                          WorkloadName(cluster, c),
	}
}

// SelectorLabels returns only the identity labels used in Selector.matchLabels.
// Keeping this narrow matches the Helm chart and keeps selectors stable across
// metadata churn.
func SelectorLabels(cluster *gvdbv1alpha1.GVDBCluster, c Component) map[string]string {
	return map[string]string{
		"app": WorkloadName(cluster, c),
	}
}
