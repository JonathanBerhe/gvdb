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
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodDisruptionBudgets renders one PDB per workload that opted in (roadmap
// 0b.5). Returns an empty slice if nothing is enabled.
// An error is returned when minAvailable > replicas for any workload — that
// configuration would produce a PDB that is always violated and blocks every
// voluntary disruption (matches the Helm chart's fail-fast check).
func PodDisruptionBudgets(cluster *gvdbv1alpha1.GVDBCluster) ([]*policyv1.PodDisruptionBudget, error) {
	out := make([]*policyv1.PodDisruptionBudget, 0, 4)
	entries := []struct {
		component Component
		pdb       gvdbv1alpha1.PodDisruptionBudgetSpec
		replicas  int32
	}{
		{CoordinatorComponent, cluster.Spec.Coordinator.PodDisruptionBudget, cluster.Spec.Coordinator.Replicas},
		{DataNodeComponent, cluster.Spec.DataNode.PodDisruptionBudget, cluster.Spec.DataNode.Replicas},
		{QueryNodeComponent, cluster.Spec.QueryNode.PodDisruptionBudget, cluster.Spec.QueryNode.Replicas},
		{ProxyComponent, cluster.Spec.Proxy.PodDisruptionBudget, cluster.Spec.Proxy.Replicas},
	}
	for _, e := range entries {
		if !e.pdb.Enabled {
			continue
		}
		min := e.pdb.MinAvailable
		if min == 0 {
			min = 1
		}
		if min > e.replicas {
			return nil, fmt.Errorf("%s.podDisruptionBudget.minAvailable (%d) > %s.replicas (%d)",
				e.component, min, e.component, e.replicas)
		}
		minAvailable := intstr.FromInt32(min)
		out = append(out, &policyv1.PodDisruptionBudget{
			TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      WorkloadName(cluster, e.component),
				Namespace: cluster.Namespace,
				Labels:    Labels(cluster, e.component),
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvailable,
				Selector:     &metav1.LabelSelector{MatchLabels: SelectorLabels(cluster, e.component)},
			},
		})
	}
	return out, nil
}
