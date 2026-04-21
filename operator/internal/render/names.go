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

// ImageRef builds "<repo>:<tag>". Tag falls back to the operator release
// version when the spec doesn't set one.
func ImageRef(cluster *gvdbv1alpha1.GVDBCluster) string {
	repo := cluster.Spec.Image.Repository
	if repo == "" {
		repo = "gvdb"
	}
	tag := cluster.Spec.Image.Tag
	if tag == "" {
		// Lockstep: tag defaults to the operator's own version when unset.
		// The operator binary receives this via an env var / build flag;
		// tests pass an explicit tag to stay deterministic.
		tag = OperatorVersion()
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
	replicas := cluster.Spec.Coordinator.Replicas
	if replicas < 1 {
		replicas = 1
	}
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
	return workloadAddresses(cluster, DataNodeComponent, cluster.Spec.DataNode.Replicas, DataNodeGRPCPort)
}

// QueryNodeAddresses returns a comma-separated list of query-node pod addresses
// for the proxy --query-nodes flag. Mirrors gvdb.queryNode.addresses.
func QueryNodeAddresses(cluster *gvdbv1alpha1.GVDBCluster) string {
	return workloadAddresses(cluster, QueryNodeComponent, cluster.Spec.QueryNode.Replicas, QueryNodeGRPCPort)
}

func workloadAddresses(cluster *gvdbv1alpha1.GVDBCluster, c Component, replicas int32, port int) string {
	if replicas < 1 {
		replicas = 1
	}
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

// operatorVersion is the fallback image tag. Set by cmd/main.go via
// SetOperatorVersion; tests override directly.
var operatorVersion = "latest"

// OperatorVersion reports the current release tag the operator defaults to.
func OperatorVersion() string {
	return operatorVersion
}

// SetOperatorVersion is used by cmd/main.go to pin the default image tag.
func SetOperatorVersion(v string) {
	if v != "" {
		operatorVersion = v
	}
}
