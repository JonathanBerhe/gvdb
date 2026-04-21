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
// applies them via Server-Side Apply. Hardening primitives (PDB, SA,
// PriorityClass) are not yet wired — they land in the next commit alongside
// the full anti-affinity builder.
func All(cluster *gvdbv1alpha1.GVDBCluster) []client.Object {
	return []client.Object{
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
}
