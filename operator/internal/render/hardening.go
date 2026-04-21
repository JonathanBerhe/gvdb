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

// buildAntiAffinity constructs a podAntiAffinity that keeps pods of the same
// workload apart. Mirrors the gvdb.antiAffinity helper: preferred mode uses a
// weight-100 preferredDuringSchedulingIgnoredDuringExecution term; required
// mode uses requiredDuringSchedulingIgnoredDuringExecution.
func buildAntiAffinity(spec gvdbv1alpha1.AntiAffinitySpec, selector map[string]string) *corev1.Affinity {
	topologyKey := spec.TopologyKey
	if topologyKey == "" {
		topologyKey = "kubernetes.io/hostname"
	}
	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
		TopologyKey:   topologyKey,
	}
	paa := &corev1.PodAntiAffinity{}
	if spec.Type == "required" {
		paa.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{term}
	} else {
		paa.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{{
			Weight:          100,
			PodAffinityTerm: term,
		}}
	}
	return &corev1.Affinity{PodAntiAffinity: paa}
}

// resolvePriorityClassName picks the PriorityClass name to attach to a pod:
//   - explicit wc.PriorityClassName wins
//   - otherwise when priorityClasses.create is true, auto-wire the chart-managed
//     PriorityClass name so operators don't duplicate the string
//   - otherwise empty (no PriorityClass)
func resolvePriorityClassName(cluster *gvdbv1alpha1.GVDBCluster, c Component, wc gvdbv1alpha1.WorkloadCommon) string {
	if wc.PriorityClassName != "" {
		return wc.PriorityClassName
	}
	if cluster.Spec.PriorityClasses.Create {
		return PriorityClassName(cluster, c)
	}
	return ""
}

// resolveServiceAccountName picks the ServiceAccount name to attach to a pod:
//   - wc.ServiceAccount.Name wins if set
//   - when wc.ServiceAccount.Create is true and no explicit name, fall back to
//     "<release>-<component>" (matches the chart's default)
//   - otherwise empty (K8s defaults to "default")
func resolveServiceAccountName(cluster *gvdbv1alpha1.GVDBCluster, c Component, wc gvdbv1alpha1.WorkloadCommon) string {
	if wc.ServiceAccount.Name != "" {
		return wc.ServiceAccount.Name
	}
	if wc.ServiceAccount.Create {
		return WorkloadName(cluster, c)
	}
	return ""
}
