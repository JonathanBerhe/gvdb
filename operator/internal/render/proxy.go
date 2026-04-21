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
	"strconv"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProxyDeployment renders the stateless proxy Deployment.
func ProxyDeployment(cluster *gvdbv1alpha1.GVDBCluster) *appsv1.Deployment {
	spec := &cluster.Spec.Proxy
	replicas := spec.Replicas
	if replicas == 0 {
		replicas = 1
	}

	podSpec := corev1.PodSpec{
		SecurityContext: cluster.Spec.Security.PodSecurityContext,
		InitContainers: []corev1.Container{
			waitForCoordinator(cluster),
			waitForDataNode(cluster),
		},
		Containers: []corev1.Container{{
			Name:            "proxy",
			Image:           ImageRef(cluster),
			ImagePullPolicy: imagePullPolicy(cluster),
			SecurityContext: cluster.Spec.Security.ContainerSecurityContext,
			Command:         []string{"/usr/local/bin/gvdb-proxy"},
			Args: []string{
				"--node-id", "1",
				"--bind-address", "0.0.0.0:" + strconv.Itoa(ProxyGRPCPort),
				"--data-dir", "/tmp/gvdb/proxy",
				"--coordinators", CoordinatorAddress(cluster),
				"--data-nodes", DataNodeAddresses(cluster),
				"--query-nodes", QueryNodeAddresses(cluster),
			},
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: ProxyGRPCPort},
				{Name: "metrics", ContainerPort: ProxyMetricsPort},
			},
			ReadinessProbe: tcpProbe(ProxyGRPCPort, 5, 10),
			LivenessProbe:  tcpProbe(ProxyGRPCPort, 10, 20),
			Resources:      spec.Resources,
		}},
	}
	applyWorkloadCommon(&podSpec, spec.WorkloadCommon, SelectorLabels(cluster, ProxyComponent), cluster, ProxyComponent)

	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, ProxyComponent),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, ProxyComponent),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: SelectorLabels(cluster, ProxyComponent)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: SelectorLabels(cluster, ProxyComponent)},
				Spec:       podSpec,
			},
		},
	}
}

// waitForDataNode blocks proxy startup until data-node-0 is reachable, matching
// the Helm chart's proxy init container.
func waitForDataNode(cluster *gvdbv1alpha1.GVDBCluster) corev1.Container {
	svcName := WorkloadName(cluster, DataNodeComponent)
	host := fmt.Sprintf("%s-0.%s.%s.svc.%s",
		WorkloadName(cluster, DataNodeComponent), svcName, cluster.Namespace, ClusterDomain(cluster))
	return corev1.Container{
		Name:            "wait-for-data-node",
		Image:           ImageRef(cluster),
		ImagePullPolicy: imagePullPolicy(cluster),
		Command:         []string{"sh", "-c"},
		Args: []string{fmt.Sprintf(`echo "Waiting for data node..."
until nc -z %s %d; do
  sleep 2
done
echo "Data node is ready"
`, host, DataNodeGRPCPort)},
	}
}
