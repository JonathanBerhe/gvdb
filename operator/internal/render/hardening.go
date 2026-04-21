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
)

// Stubs for the 0b.5 hardening primitives. The full implementation (PDB,
// ServiceAccount, PriorityClass renderers + anti-affinity builder + auto-wire
// priorityClassName when priorityClasses.create=true) lands in the next
// commit so the diff is reviewable as a separate concern.

// buildAntiAffinity returns nil while the hardening layer is not yet wired.
// The WorkloadCommon.PodAntiAffinity.Enabled gate in applyWorkloadCommon
// ensures we don't produce misleading affinity blocks in the meantime.
func buildAntiAffinity(_ gvdbv1alpha1.AntiAffinitySpec, _ map[string]string) *corev1.Affinity {
	return nil
}

// resolvePriorityClassName today only honors an explicit override on the
// workload spec. The auto-wire behavior (pick the chart-managed class when
// priorityClasses.create=true) arrives with the PriorityClass renderer.
func resolvePriorityClassName(_ *gvdbv1alpha1.GVDBCluster, _ Component, wc gvdbv1alpha1.WorkloadCommon) string {
	return wc.PriorityClassName
}

// resolveServiceAccountName today only honors explicit overrides. SA creation
// (and the default "<release>-<component>" fallback when create=true) ships
// with the ServiceAccount renderer.
func resolveServiceAccountName(_ *gvdbv1alpha1.GVDBCluster, _ Component, wc gvdbv1alpha1.WorkloadCommon) string {
	if wc.ServiceAccount.Create || wc.ServiceAccount.Name != "" {
		return wc.ServiceAccount.Name
	}
	return ""
}
