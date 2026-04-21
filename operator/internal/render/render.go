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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// All renders every Kubernetes object for the given GVDBCluster. The
// reconciler takes this slice, stamps OwnerReferences on each object, and
// applies them via Server-Side Apply. Returns an error only when a CR
// configuration is internally inconsistent (e.g. PDB minAvailable > replicas).
func All(cluster *gvdbv1alpha1.GVDBCluster) ([]client.Object, error) {
	objs := []client.Object{
		ConfigMap(cluster),
		CoordinatorService(cluster),
		DataNodeService(cluster),
		QueryNodeService(cluster),
		ProxyService(cluster),
		CoordinatorStatefulSet(cluster),
		DataNodeStatefulSet(cluster),
		QueryNodeStatefulSet(cluster),
		ProxyDeployment(cluster),
	}
	for _, sa := range ServiceAccounts(cluster) {
		objs = append(objs, sa)
	}
	pdbs, err := PodDisruptionBudgets(cluster)
	if err != nil {
		return nil, err
	}
	for _, p := range pdbs {
		objs = append(objs, p)
	}
	for _, pc := range PriorityClasses(cluster) {
		objs = append(objs, pc)
	}
	return objs, nil
}
