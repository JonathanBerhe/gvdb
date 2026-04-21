/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package render

import (
	"strings"
	"testing"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testCluster(name, ns string, coordReplicas, dataReplicas, queryReplicas int32) *gvdbv1alpha1.GVDBCluster {
	return &gvdbv1alpha1.GVDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: gvdbv1alpha1.GVDBClusterSpec{
			Image: gvdbv1alpha1.ImageSpec{Repository: "gvdb", Tag: "test"},
			Coordinator: gvdbv1alpha1.CoordinatorSpec{
				Replicas: coordReplicas,
			},
			DataNode:  gvdbv1alpha1.DataNodeSpec{Replicas: dataReplicas},
			QueryNode: gvdbv1alpha1.QueryNodeSpec{Replicas: queryReplicas},
			Proxy:     gvdbv1alpha1.ProxySpec{Replicas: 1},
		},
	}
}

func TestNames(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 2, 1)
	if got := FullName(c); got != "prod" {
		t.Fatalf("FullName: got %q, want %q", got, "prod")
	}
	if got := WorkloadName(c, CoordinatorComponent); got != "prod-coordinator" {
		t.Fatalf("WorkloadName(coordinator): got %q", got)
	}
	if got := ConfigMapName(c); got != "prod-config" {
		t.Fatalf("ConfigMapName: got %q", got)
	}
	if got := ImageRef(c); got != "gvdb:test" {
		t.Fatalf("ImageRef: got %q", got)
	}
}

func TestRaftPeers_HA(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	got := CoordinatorRaftPeers(c)
	want := "1:prod-coordinator-0.prod-coordinator.gvdb.svc.cluster.local:8300," +
		"2:prod-coordinator-1.prod-coordinator.gvdb.svc.cluster.local:8300," +
		"3:prod-coordinator-2.prod-coordinator.gvdb.svc.cluster.local:8300"
	if got != want {
		t.Fatalf("raftPeers mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestRaftPeers_SingleNode(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 2, 1)
	got := CoordinatorRaftPeers(c)
	want := "1:prod-coordinator-0.prod-coordinator.gvdb.svc.cluster.local:8300"
	if got != want {
		t.Fatalf("raftPeers mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestDataAndQueryNodeAddresses(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 3, 2)
	wantData := "prod-data-node-0.prod-data-node.gvdb.svc.cluster.local:50060," +
		"prod-data-node-1.prod-data-node.gvdb.svc.cluster.local:50060," +
		"prod-data-node-2.prod-data-node.gvdb.svc.cluster.local:50060"
	if got := DataNodeAddresses(c); got != wantData {
		t.Fatalf("dataNodeAddresses: got %q", got)
	}
	wantQuery := "prod-query-node-0.prod-query-node.gvdb.svc.cluster.local:50070," +
		"prod-query-node-1.prod-query-node.gvdb.svc.cluster.local:50070"
	if got := QueryNodeAddresses(c); got != wantQuery {
		t.Fatalf("queryNodeAddresses: got %q", got)
	}
}

func TestCoordinatorStatefulSet_ParallelAndPeersFlag(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	sts := CoordinatorStatefulSet(c)
	if sts.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Fatalf("coordinator must use Parallel pod management (roadmap 0b.4), got %q", sts.Spec.PodManagementPolicy)
	}
	if got := *sts.Spec.Replicas; got != 3 {
		t.Fatalf("replicas: got %d, want 3", got)
	}
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(args, "--raft-peers") {
		t.Fatalf("multi-node coordinator must receive --raft-peers, args=%q", args)
	}
	if !strings.Contains(args, "NODE_ID=$((ORDINAL + 1))") {
		t.Fatalf("coordinator must derive NODE_ID from ordinal, args=%q", args)
	}
}

func TestCoordinatorStatefulSet_SingleNodeFlag(t *testing.T) {
	c := testCluster("dev", "gvdb", 1, 1, 1)
	sts := CoordinatorStatefulSet(c)
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(args, "--single-node") {
		t.Fatalf("single-node coordinator must receive --single-node, args=%q", args)
	}
	if strings.Contains(args, "--raft-peers") {
		t.Fatalf("single-node coordinator must not receive --raft-peers, args=%q", args)
	}
}

func TestDataNodeStatefulSet(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 3, 1)
	sts := DataNodeStatefulSet(c)
	if got := *sts.Spec.Replicas; got != 3 {
		t.Fatalf("replicas: got %d, want 3", got)
	}
	if sts.Spec.Template.Spec.TerminationGracePeriodSeconds == nil ||
		*sts.Spec.Template.Spec.TerminationGracePeriodSeconds != 60 {
		t.Fatalf("data-node default terminationGracePeriodSeconds must be 60 (drain window), got %v",
			sts.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(args, "NODE_ID=$((101 + ORDINAL))") {
		t.Fatalf("data-node must derive NODE_ID=101+ORDINAL, args=%q", args)
	}
}

func TestQueryNodeStatefulSet(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 2, 2)
	sts := QueryNodeStatefulSet(c)
	if got := *sts.Spec.Replicas; got != 2 {
		t.Fatalf("replicas: got %d, want 2", got)
	}
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(args, "NODE_ID=$((201 + ORDINAL))") {
		t.Fatalf("query-node must derive NODE_ID=201+ORDINAL, args=%q", args)
	}
}

func TestProxyDeployment(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	dep := ProxyDeployment(c)
	if got := *dep.Spec.Replicas; got != 1 {
		t.Fatalf("replicas: got %d, want 1", got)
	}
	args := dep.Spec.Template.Spec.Containers[0].Args
	// --coordinators value lives in the arg slot immediately after the flag.
	found := false
	for i, a := range args {
		if a == "--coordinators" && i+1 < len(args) {
			if args[i+1] != CoordinatorAddress(c) {
				t.Fatalf("proxy --coordinators: got %q, want %q", args[i+1], CoordinatorAddress(c))
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("proxy args must include --coordinators, got %v", args)
	}
}

func TestCoordinatorService(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 1, 1)
	svc := CoordinatorService(c)
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("coordinator service must be headless, got clusterIP=%q", svc.Spec.ClusterIP)
	}
	wantPorts := map[string]int32{"grpc": 50051, "raft": 8300, "metrics": 9091}
	if len(svc.Spec.Ports) != len(wantPorts) {
		t.Fatalf("coordinator service ports: got %d, want %d", len(svc.Spec.Ports), len(wantPorts))
	}
	for _, p := range svc.Spec.Ports {
		if want, ok := wantPorts[p.Name]; !ok || want != p.Port {
			t.Fatalf("unexpected port %s=%d", p.Name, p.Port)
		}
	}
}

func TestAll_RendersAllCoreObjects(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 3, 2)
	objs, err := All(c)
	if err != nil {
		t.Fatalf("All() error: %v", err)
	}
	// With no hardening opt-ins: ConfigMap + 4 Services + 3 StatefulSets + 1 Deployment = 9.
	if len(objs) != 9 {
		t.Fatalf("All(): got %d objects, want 9", len(objs))
	}
}

func TestAll_WithHardeningEnabled(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 3, 2)
	// Opt into all hardening primitives.
	c.Spec.Coordinator.PodDisruptionBudget = gvdbv1alpha1.PodDisruptionBudgetSpec{Enabled: true, MinAvailable: 2}
	c.Spec.DataNode.PodDisruptionBudget = gvdbv1alpha1.PodDisruptionBudgetSpec{Enabled: true, MinAvailable: 2}
	c.Spec.Coordinator.ServiceAccount = gvdbv1alpha1.ServiceAccountSpec{Create: true}
	c.Spec.DataNode.ServiceAccount = gvdbv1alpha1.ServiceAccountSpec{Create: true, Annotations: map[string]string{
		"eks.amazonaws.com/role-arn": "arn:aws:iam::123:role/gvdb",
	}}
	c.Spec.PriorityClasses = gvdbv1alpha1.PriorityClassesSpec{Create: true}

	objs, err := All(c)
	if err != nil {
		t.Fatalf("All() error: %v", err)
	}
	// Base 9 + 2 SAs + 2 PDBs + 4 PriorityClasses = 17.
	if len(objs) != 17 {
		t.Fatalf("All() with hardening: got %d objects, want 17", len(objs))
	}
}

func TestPodDisruptionBudgets_FailOnMinAvailableTooHigh(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	c.Spec.Coordinator.PodDisruptionBudget = gvdbv1alpha1.PodDisruptionBudgetSpec{
		Enabled: true, MinAvailable: 5, // > replicas=3
	}
	if _, err := PodDisruptionBudgets(c); err == nil {
		t.Fatalf("PodDisruptionBudgets must reject minAvailable > replicas, got nil error")
	}
}

func TestPriorityClassName_EmbedsNamespace(t *testing.T) {
	c := testCluster("prod", "team-a", 1, 1, 1)
	got := PriorityClassName(c, CoordinatorComponent)
	want := "prod-team-a-coordinator"
	if got != want {
		t.Fatalf("PriorityClassName: got %q, want %q (must embed namespace so cluster-scoped names don't collide)", got, want)
	}
}

func TestAntiAffinity_PreferredAndRequired(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 3, 2)
	c.Spec.Coordinator.PodAntiAffinity = gvdbv1alpha1.AntiAffinitySpec{
		Enabled: true, Type: "required", TopologyKey: "topology.kubernetes.io/zone",
	}
	sts := CoordinatorStatefulSet(c)
	aff := sts.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected podAntiAffinity to be populated when enabled=true")
	}
	if len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("required mode must produce RequiredDuringSchedulingIgnoredDuringExecution term")
	}
	if aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != "topology.kubernetes.io/zone" {
		t.Fatalf("topologyKey not threaded through")
	}

	// Switch to preferred.
	c.Spec.Coordinator.PodAntiAffinity.Type = "preferred"
	sts = CoordinatorStatefulSet(c)
	aff = sts.Spec.Template.Spec.Affinity
	if len(aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("preferred mode must produce PreferredDuringSchedulingIgnoredDuringExecution term")
	}
}

func TestPriorityClassName_AutoWired(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	c.Spec.PriorityClasses = gvdbv1alpha1.PriorityClassesSpec{Create: true}
	sts := CoordinatorStatefulSet(c)
	if got := sts.Spec.Template.Spec.PriorityClassName; got != "prod-gvdb-coordinator" {
		t.Fatalf("priorityClassName should auto-wire to chart-managed name, got %q", got)
	}
}

func TestConfigMap_Defaults(t *testing.T) {
	c := testCluster("dev", "gvdb", 1, 1, 1)
	cm := ConfigMap(c)
	got := cm.Data["config.yaml"]
	// Defaults (matching +kubebuilder:default and Helm values.yaml).
	for _, want := range []string{
		`max_message_size_mb: 256`,
		`segment_max_size_mb: 512`,
		`default_index_type: "HNSW"`,
		`level: "info"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("ConfigMap missing default %q\nfull data:\n%s", want, got)
		}
	}
}
