/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package render

import (
	"fmt"
	"strings"
	"testing"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if got := ImageRef(c, Options{}); got != "gvdb:test" {
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

func TestQueryNodeDNSURI(t *testing.T) {
	// Post-1.7: proxy takes a single dns:/// URI for the query-node headless
	// service. gRPC resolves all Ready pod A records at request time, so
	// `kubectl scale query-node` is picked up without a helm-upgrade.
	c := testCluster("prod", "gvdb", 3, 3, 2)
	want := "dns:///prod-query-node.gvdb.svc.cluster.local:50070"
	if got := QueryNodeDNSURI(c); got != want {
		t.Fatalf("queryNodeDNSURI: got %q, want %q", got, want)
	}
}

func TestCoordinatorStatefulSet_ParallelAndPeersFlag(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	sts := CoordinatorStatefulSet(c, Options{})
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
	if !strings.Contains(args, "--coordinator-grpc-peers") {
		t.Fatalf("multi-node coordinator must receive --coordinator-grpc-peers for 1.7b auto-join, args=%q", args)
	}
	if !strings.Contains(args, "NODE_ID=$((ORDINAL + 1))") {
		t.Fatalf("coordinator must derive NODE_ID from ordinal, args=%q", args)
	}
}

func TestCoordinatorGRPCPeers_HA(t *testing.T) {
	// Post-1.7b: the binary probes these endpoints on startup for a live
	// leader and uses index (leader_id-1) to follow NOT_LEADER redirects.
	// Order must mirror CoordinatorRaftPeers exactly.
	c := testCluster("prod", "gvdb", 3, 2, 1)
	want := "prod-coordinator-0.prod-coordinator.gvdb.svc.cluster.local:50051," +
		"prod-coordinator-1.prod-coordinator.gvdb.svc.cluster.local:50051," +
		"prod-coordinator-2.prod-coordinator.gvdb.svc.cluster.local:50051"
	if got := CoordinatorGRPCPeers(c); got != want {
		t.Fatalf("coordinatorGRPCPeers mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestCoordinatorStatefulSet_SingleNodeSkipsGRPCPeers(t *testing.T) {
	// Single-node coordinator takes --single-node; no peer list and no
	// 1.7b auto-join logic applies.
	c := testCluster("prod", "gvdb", 1, 2, 1)
	sts := CoordinatorStatefulSet(c, Options{})
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	if strings.Contains(args, "--coordinator-grpc-peers") {
		t.Fatalf("single-node coordinator must not receive --coordinator-grpc-peers, args=%q", args)
	}
}

func TestCoordinatorStatefulSet_SingleNodeFlag(t *testing.T) {
	c := testCluster("dev", "gvdb", 1, 1, 1)
	sts := CoordinatorStatefulSet(c, Options{})
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
	sts := DataNodeStatefulSet(c, Options{})
	// Roadmap 1.8.c: Spec.Replicas is owned by the scale reconciler, NOT
	// this render. Render must leave it unset so the two SSA field managers
	// never fight over the value.
	if sts.Spec.Replicas != nil {
		t.Fatalf("data-node render must not set Spec.Replicas (owned by scale reconciler); got %d", *sts.Spec.Replicas)
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
	sts := QueryNodeStatefulSet(c, Options{})
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
	dep := ProxyDeployment(c, Options{})
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
	objs, err := All(c, Options{})
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

	objs, err := All(c, Options{})
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
	sts := CoordinatorStatefulSet(c, Options{})
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
	sts = CoordinatorStatefulSet(c, Options{})
	aff = sts.Spec.Template.Spec.Affinity
	if len(aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("preferred mode must produce PreferredDuringSchedulingIgnoredDuringExecution term")
	}
}

func TestPriorityClassName_AutoWired(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	c.Spec.PriorityClasses = gvdbv1alpha1.PriorityClassesSpec{Create: true}
	sts := CoordinatorStatefulSet(c, Options{})
	if got := sts.Spec.Template.Spec.PriorityClassName; got != "prod-gvdb-coordinator" {
		t.Fatalf("priorityClassName should auto-wire to chart-managed name, got %q", got)
	}
}

func TestRaftPeers_SevenReplicaHA(t *testing.T) {
	c := testCluster("prod", "gvdb", 7, 2, 1)
	got := CoordinatorRaftPeers(c)
	// Each peer must appear with id = ordinal+1 (NuRaft > 0 requirement).
	for i := 0; i < 7; i++ {
		want := fmt.Sprintf("%d:prod-coordinator-%d.prod-coordinator.gvdb.svc.cluster.local:8300", i+1, i)
		if !strings.Contains(got, want) {
			t.Fatalf("7-replica raftPeers missing %q in %q", want, got)
		}
	}
}

func TestStorageClassName_ThreadsToPVC(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 2, 1)
	c.Spec.DataNode.Storage = gvdbv1alpha1.StorageSpec{
		Size:             resource.MustParse("10Gi"),
		StorageClassName: "fast-ssd",
	}
	sts := DataNodeStatefulSet(c, Options{})
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected 1 volumeClaimTemplate, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	pvc := sts.Spec.VolumeClaimTemplates[0]
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast-ssd" {
		t.Fatalf("storageClassName not threaded to PVC, got %v", pvc.Spec.StorageClassName)
	}
	gotSize := pvc.Spec.Resources.Requests["storage"]
	if gotSize.String() != "10Gi" {
		t.Fatalf("pvc size: got %q, want 10Gi", gotSize.String())
	}
}

func TestConfigHashAnnotation_StampedOnAllWorkloads(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 2, 1)
	objs, err := All(c, Options{})
	if err != nil {
		t.Fatalf("All(): %v", err)
	}
	// Extract the hash from the coordinator and verify everyone shares it.
	var hash string
	for _, obj := range objs {
		if sts, ok := obj.(*appsv1.StatefulSet); ok && sts.Name == "prod-coordinator" {
			hash = sts.Spec.Template.Annotations[ConfigHashAnnotation]
			break
		}
	}
	if hash == "" {
		t.Fatalf("coordinator StatefulSet missing %s annotation", ConfigHashAnnotation)
	}
	for _, obj := range objs {
		switch o := obj.(type) {
		case *appsv1.StatefulSet:
			if got := o.Spec.Template.Annotations[ConfigHashAnnotation]; got != hash {
				t.Fatalf("%s config-hash mismatch: got %q want %q", o.Name, got, hash)
			}
		case *appsv1.Deployment:
			if got := o.Spec.Template.Annotations[ConfigHashAnnotation]; got != hash {
				t.Fatalf("%s config-hash mismatch: got %q want %q", o.Name, got, hash)
			}
		}
	}
}

func TestConfigHashAnnotation_ChangesWhenConfigChanges(t *testing.T) {
	c1 := testCluster("prod", "gvdb", 1, 1, 1)
	objs1, _ := All(c1, Options{})
	c2 := testCluster("prod", "gvdb", 1, 1, 1)
	c2.Spec.Config.Logging.Level = "debug" // diverge from default "info"
	objs2, _ := All(c2, Options{})

	hash := func(objs []client.Object) string {
		for _, o := range objs {
			if sts, ok := o.(*appsv1.StatefulSet); ok && sts.Name == "prod-coordinator" {
				return sts.Spec.Template.Annotations[ConfigHashAnnotation]
			}
		}
		return ""
	}
	if hash(objs1) == hash(objs2) {
		t.Fatalf("config change must produce a different config-hash so pods roll; both were %q", hash(objs1))
	}
}

func TestPriorityClasses_CarryClusterSelectorLabels(t *testing.T) {
	c := testCluster("prod", "gvdb", 3, 2, 1)
	c.Spec.PriorityClasses = gvdbv1alpha1.PriorityClassesSpec{Create: true}
	pcs := PriorityClasses(c)
	if len(pcs) != 4 {
		t.Fatalf("expected 4 PriorityClasses, got %d", len(pcs))
	}
	// All four must carry the cluster-identity labels so the reconciler's
	// finalizer can delete them via List(MatchingLabels) on CR teardown.
	for _, pc := range pcs {
		if pc.Labels[ClusterNameLabel] != "prod" {
			t.Fatalf("%s missing/wrong %s label: %q", pc.Name, ClusterNameLabel, pc.Labels[ClusterNameLabel])
		}
		if pc.Labels[ClusterNamespaceLabel] != "gvdb" {
			t.Fatalf("%s missing/wrong %s label: %q", pc.Name, ClusterNamespaceLabel, pc.Labels[ClusterNamespaceLabel])
		}
	}
}

func TestImageRef_FallbackLadder(t *testing.T) {
	c := testCluster("prod", "gvdb", 1, 1, 1)
	// spec.image.tag wins.
	if got := ImageRef(c, Options{DefaultImageTag: "0.99.0"}); got != "gvdb:test" {
		t.Fatalf("tag override: got %q", got)
	}
	// Empty spec.image.tag → opts.DefaultImageTag.
	c.Spec.Image.Tag = ""
	if got := ImageRef(c, Options{DefaultImageTag: "0.21.0"}); got != "gvdb:0.21.0" {
		t.Fatalf("opts fallback: got %q", got)
	}
	// Both empty → "latest" sentinel (test-only path).
	if got := ImageRef(c, Options{}); got != "gvdb:latest" {
		t.Fatalf("final fallback: got %q", got)
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
