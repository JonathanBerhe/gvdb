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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func queryNodeStartupScript(cluster *gvdbv1alpha1.GVDBCluster) string {
	svcName := WorkloadName(cluster, QueryNodeComponent)
	domain := ClusterDomain(cluster)
	memGb := cluster.Spec.QueryNode.MemoryLimitGb
	if memGb == 0 {
		memGb = 4
	}
	return fmt.Sprintf(`ORDINAL=${HOSTNAME##*-}
NODE_ID=$((201 + ORDINAL))
ADVERTISE="${HOSTNAME}.%s.%s.svc.%s:%d"
exec /usr/local/bin/gvdb-query-node \
  --node-id $NODE_ID \
  --bind-address 0.0.0.0:%d \
  --advertise-address $ADVERTISE \
  --data-dir /data/gvdb/query_node \
  --coordinator %s \
  --memory-limit-gb %d
`,
		svcName, cluster.Namespace, domain, QueryNodeGRPCPort,
		QueryNodeGRPCPort,
		CoordinatorAddress(cluster),
		memGb,
	)
}

// QueryNodeStatefulSet renders the query-node StatefulSet.
func QueryNodeStatefulSet(cluster *gvdbv1alpha1.GVDBCluster, opts Options) *appsv1.StatefulSet {
	spec := &cluster.Spec.QueryNode
	replicas := EffectiveReplicas(cluster, QueryNodeComponent)
	grace := spec.TerminationGracePeriodSeconds
	if grace == 0 {
		grace = 30
	}

	podSpec := corev1.PodSpec{
		TerminationGracePeriodSeconds: &grace,
		SecurityContext:               cluster.Spec.Security.PodSecurityContext,
		InitContainers:                []corev1.Container{waitForCoordinator(cluster, opts)},
		Containers: []corev1.Container{{
			Name:            "query-node",
			Image:           ImageRef(cluster, opts),
			ImagePullPolicy: imagePullPolicy(cluster),
			SecurityContext: cluster.Spec.Security.ContainerSecurityContext,
			Command:         []string{"sh", "-c"},
			Args:            []string{queryNodeStartupScript(cluster)},
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: QueryNodeGRPCPort},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/data/gvdb/query_node"},
				{Name: "config", MountPath: "/etc/gvdb"},
			},
			ReadinessProbe: tcpProbe(QueryNodeGRPCPort, 10, 10),
			LivenessProbe:  tcpProbe(QueryNodeGRPCPort, 20, 20),
			Resources:      spec.Resources,
		}},
		Volumes: []corev1.Volume{configVolume(cluster)},
	}
	applyWorkloadCommon(&podSpec, spec.WorkloadCommon, SelectorLabels(cluster, QueryNodeComponent), cluster, QueryNodeComponent)

	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, QueryNodeComponent),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, QueryNodeComponent),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: WorkloadName(cluster, QueryNodeComponent),
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: SelectorLabels(cluster, QueryNodeComponent)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      SelectorLabels(cluster, QueryNodeComponent),
					Annotations: podTemplateAnnotations(opts),
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{dataPVC(spec.Storage, "2Gi")},
		},
	}
}
