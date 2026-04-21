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

// dataNodeStartupScript mirrors the data-node shell logic in the Helm chart:
// derive node_id = 101 + ordinal, build pod FQDN, exec the binary with the
// coordinator-0 address.
func dataNodeStartupScript(cluster *gvdbv1alpha1.GVDBCluster) string {
	svcName := WorkloadName(cluster, DataNodeComponent)
	domain := ClusterDomain(cluster)
	memGb := cluster.Spec.DataNode.MemoryLimitGb
	if memGb == 0 {
		memGb = 4
	}
	return fmt.Sprintf(`ORDINAL=${HOSTNAME##*-}
NODE_ID=$((101 + ORDINAL))
ADVERTISE="${HOSTNAME}.%s.%s.svc.%s:%d"
exec /usr/local/bin/gvdb-data-node \
  --node-id $NODE_ID \
  --bind-address 0.0.0.0:%d \
  --advertise-address $ADVERTISE \
  --data-dir /data/gvdb/data_node \
  --coordinator %s \
  --memory-limit-gb %d
`,
		svcName, cluster.Namespace, domain, DataNodeGRPCPort,
		DataNodeGRPCPort,
		CoordinatorAddress(cluster),
		memGb,
	)
}

// DataNodeStatefulSet renders the data-node StatefulSet.
func DataNodeStatefulSet(cluster *gvdbv1alpha1.GVDBCluster, opts Options) *appsv1.StatefulSet {
	spec := &cluster.Spec.DataNode
	replicas := EffectiveReplicas(cluster, DataNodeComponent)
	grace := spec.TerminationGracePeriodSeconds
	if grace == 0 {
		grace = 60
	}

	podSpec := corev1.PodSpec{
		TerminationGracePeriodSeconds: &grace,
		SecurityContext:               cluster.Spec.Security.PodSecurityContext,
		InitContainers: []corev1.Container{
			waitForCoordinator(cluster, opts),
		},
		Containers: []corev1.Container{{
			Name:            "data-node",
			Image:           ImageRef(cluster, opts),
			ImagePullPolicy: imagePullPolicy(cluster),
			SecurityContext: cluster.Spec.Security.ContainerSecurityContext,
			Command:         []string{"sh", "-c"},
			Args:            []string{dataNodeStartupScript(cluster)},
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: DataNodeGRPCPort},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/data/gvdb/data_node"},
				{Name: "config", MountPath: "/etc/gvdb"},
			},
			ReadinessProbe: tcpProbe(DataNodeGRPCPort, 10, 10),
			LivenessProbe:  tcpProbe(DataNodeGRPCPort, 20, 20),
			Resources:      spec.Resources,
		}},
		Volumes: []corev1.Volume{configVolume(cluster)},
	}
	applyWorkloadCommon(&podSpec, spec.WorkloadCommon, SelectorLabels(cluster, DataNodeComponent), cluster, DataNodeComponent)

	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, DataNodeComponent),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, DataNodeComponent),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: WorkloadName(cluster, DataNodeComponent),
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: SelectorLabels(cluster, DataNodeComponent)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      SelectorLabels(cluster, DataNodeComponent),
					Annotations: podTemplateAnnotations(opts),
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{dataPVC(spec.Storage, "5Gi")},
		},
	}
}

// waitForCoordinator is the init container used by data-node, query-node, and
// proxy pods. Matches the Helm chart init container.
func waitForCoordinator(cluster *gvdbv1alpha1.GVDBCluster, opts Options) corev1.Container {
	svcName := WorkloadName(cluster, CoordinatorComponent)
	host := fmt.Sprintf("%s-0.%s.%s.svc.%s",
		WorkloadName(cluster, CoordinatorComponent), svcName, cluster.Namespace, ClusterDomain(cluster))
	return corev1.Container{
		Name:            "wait-for-coordinator",
		Image:           ImageRef(cluster, opts),
		ImagePullPolicy: imagePullPolicy(cluster),
		Command:         []string{"sh", "-c"},
		Args: []string{fmt.Sprintf(`echo "Waiting for coordinator..."
until nc -z %s %d; do
  sleep 2
done
echo "Coordinator is ready"
`, host, CoordinatorGRPCPort)},
	}
}

// configVolume is the shared "config" volume backed by the server ConfigMap.
func configVolume(cluster *gvdbv1alpha1.GVDBCluster) corev1.Volume {
	return corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName(cluster)},
			},
		},
	}
}
