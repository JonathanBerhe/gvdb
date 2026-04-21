/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package render

import (
	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceAccounts renders one SA per workload that opted in (roadmap 0b.5).
// Names default to "<release>-<component>" and accept arbitrary annotations
// for cloud-IAM binding (IRSA on EKS, Workload Identity on GKE, Azure WI on
// AKS).
func ServiceAccounts(cluster *gvdbv1alpha1.GVDBCluster) []*corev1.ServiceAccount {
	out := make([]*corev1.ServiceAccount, 0, 4)
	entries := []struct {
		component Component
		sa        gvdbv1alpha1.ServiceAccountSpec
	}{
		{CoordinatorComponent, cluster.Spec.Coordinator.ServiceAccount},
		{DataNodeComponent, cluster.Spec.DataNode.ServiceAccount},
		{QueryNodeComponent, cluster.Spec.QueryNode.ServiceAccount},
		{ProxyComponent, cluster.Spec.Proxy.ServiceAccount},
	}
	for _, e := range entries {
		if !e.sa.Create {
			continue
		}
		name := e.sa.Name
		if name == "" {
			name = WorkloadName(cluster, e.component)
		}
		out = append(out, &corev1.ServiceAccount{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   cluster.Namespace,
				Labels:      Labels(cluster, e.component),
				Annotations: e.sa.Annotations,
			},
		})
	}
	return out
}
