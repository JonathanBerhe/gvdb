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

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	schedv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Default priority values when the CR leaves them unset, matching the Helm
// values.yaml. Coordinator has the highest value because Raft leader loss
// stalls metadata ops.
const (
	defaultCoordinatorPriority = 1000000
	defaultDataNodePriority    = 900000
	defaultQueryNodePriority   = 800000
	defaultProxyPriority       = 700000
)

// PriorityClassName returns the cluster-scoped PriorityClass name for a
// workload. Names embed the namespace so installs of the same release name
// in different namespaces don't collide on the cluster-scoped object
// (matches the gvdb.priorityClass.name.<component> helpers).
func PriorityClassName(cluster *gvdbv1alpha1.GVDBCluster, c Component) string {
	return fmt.Sprintf("%s-%s-%s", FullName(cluster), cluster.Namespace, c)
}

// PriorityClasses renders the cluster-scoped PriorityClass set when the CR
// opts in (roadmap 0b.5). Returns nil when priorityClasses.create is false.
func PriorityClasses(cluster *gvdbv1alpha1.GVDBCluster) []*schedv1.PriorityClass {
	pcs := &cluster.Spec.PriorityClasses
	if !pcs.Create {
		return nil
	}
	entries := []struct {
		component   Component
		value       int32
		description string
	}{
		{CoordinatorComponent, valueOrDefault(pcs.Coordinator.Value, defaultCoordinatorPriority),
			"GVDB coordinator — Raft leader, never preempt"},
		{DataNodeComponent, valueOrDefault(pcs.DataNode.Value, defaultDataNodePriority),
			"GVDB data-node — holds vector segments"},
		{QueryNodeComponent, valueOrDefault(pcs.QueryNode.Value, defaultQueryNodePriority),
			"GVDB query-node — search cache + fan-out"},
		{ProxyComponent, valueOrDefault(pcs.Proxy.Value, defaultProxyPriority),
			"GVDB proxy — stateless gateway"},
	}
	out := make([]*schedv1.PriorityClass, 0, len(entries))
	for _, e := range entries {
		out = append(out, &schedv1.PriorityClass{
			TypeMeta: metav1.TypeMeta{APIVersion: "scheduling.k8s.io/v1", Kind: "PriorityClass"},
			ObjectMeta: metav1.ObjectMeta{
				Name:   PriorityClassName(cluster, e.component),
				Labels: Labels(cluster, e.component),
			},
			Value:         e.value,
			GlobalDefault: false,
			Description:   e.description,
		})
	}
	return out
}

func valueOrDefault(v, def int32) int32 {
	if v == 0 {
		return def
	}
	return v
}
