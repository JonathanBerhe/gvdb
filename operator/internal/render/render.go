/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package render

import (
	"crypto/sha256"
	"encoding/hex"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// All renders every Kubernetes object for the given GVDBCluster. The
// reconciler takes this slice, stamps OwnerReferences on each object, and
// applies them via Server-Side Apply. Returns an error only when a CR
// configuration is internally inconsistent (e.g. PDB minAvailable > replicas).
//
// opts.ConfigHash is computed here from the rendered ConfigMap so callers
// never need to pre-compute it; it's then stamped as a pod-template
// annotation on every workload so that editing spec.config triggers a
// rolling restart.
func All(cluster *gvdbv1alpha1.GVDBCluster, opts Options) ([]client.Object, error) {
	cm := ConfigMap(cluster)
	opts.ConfigHash = hashConfigData(cm.Data)

	objs := []client.Object{
		cm,
		CoordinatorService(cluster),
		DataNodeService(cluster),
		QueryNodeService(cluster),
		ProxyService(cluster),
		CoordinatorStatefulSet(cluster, opts),
		DataNodeStatefulSet(cluster, opts),
		QueryNodeStatefulSet(cluster, opts),
		ProxyDeployment(cluster, opts),
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

// hashConfigData returns a short deterministic digest of the ConfigMap data,
// suitable for stamping as a pod-template annotation.
func hashConfigData(data map[string]string) string {
	h := sha256.New()
	// ConfigMap today carries a single "config.yaml" key; hashing its value
	// directly keeps the digest stable against map-iteration order. Extend to
	// a sorted-key fold if we ever add more keys.
	h.Write([]byte(data["config.yaml"]))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
