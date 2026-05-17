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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// coordinatorStartupScript builds the sh -c args for the coordinator. Mirrors
// the POSIX-portable ordinal-parsing + FQDN-building shell logic added in
// PR #70 (see deploy/helm/gvdb/templates/coordinator-statefulset.yaml:55-89).
// Keeping this as one expression matches the Helm chart verbatim so a snapshot
// diff between the two is empty.
func coordinatorStartupScript(cluster *gvdbv1alpha1.GVDBCluster) string {
	singleNode := cluster.Spec.Coordinator.Replicas <= 1
	svcName := WorkloadName(cluster, CoordinatorComponent)
	domain := ClusterDomain(cluster)

	peerFlag := "--single-node"
	if !singleNode {
		// Two flags required for roadmap 1.7b Raft auto-join:
		//   --raft-peers              seeds NuRaft's initial cluster_config
		//                             on first boot (legacy + cold-start)
		//   --coordinator-grpc-peers  used by the startup peer-probe to
		//                             detect an existing leader, and by
		//                             the SIGTERM self-remove RPC
		// Order of entries is significant — the binary uses (node_id - 1)
		// as the index into coordinator_grpc_peers when resolving a leader
		// redirect, so both flags must iterate ordinals in the same order.
		peerFlag = fmt.Sprintf("--raft-peers %q --coordinator-grpc-peers %q",
			CoordinatorRaftPeers(cluster),
			CoordinatorGRPCPeers(cluster))
	}
	return fmt.Sprintf(`set -eu
ORDINAL=${HOSTNAME##*-}
case "$ORDINAL" in
  ''|*[!0-9]*)
    echo "ERROR: could not parse ordinal from HOSTNAME=$HOSTNAME" >&2
    exit 1
    ;;
esac
NODE_ID=$((ORDINAL + 1))
ADVERTISE="${HOSTNAME}.%s.%s.svc.%s:%d"
RAFT_ADVERTISE="${HOSTNAME}.%s.%s.svc.%s:%d"
exec /usr/local/bin/gvdb-coordinator \
  --node-id $NODE_ID \
  --bind-address 0.0.0.0:%d \
  --advertise-address $ADVERTISE \
  --raft-address 0.0.0.0:%d \
  --raft-advertise-address $RAFT_ADVERTISE \
  --data-dir /data/gvdb/coordinator \
  --config /etc/gvdb/config.yaml \
  %s
`,
		svcName, cluster.Namespace, domain, CoordinatorGRPCPort,
		svcName, cluster.Namespace, domain, CoordinatorRaftPort,
		CoordinatorGRPCPort,
		CoordinatorRaftPort,
		peerFlag,
	)
}

// CoordinatorStatefulSet renders the coordinator StatefulSet. Matches
// deploy/helm/gvdb/templates/coordinator-statefulset.yaml. Uses
// podManagementPolicy: Parallel for HA bootstrap (roadmap 0b.4).
func CoordinatorStatefulSet(cluster *gvdbv1alpha1.GVDBCluster, opts Options) *appsv1.StatefulSet {
	spec := &cluster.Spec.Coordinator
	replicas := EffectiveReplicas(cluster, CoordinatorComponent)

	podSpec := corev1.PodSpec{
		SecurityContext: cluster.Spec.Security.PodSecurityContext,
		Containers: []corev1.Container{{
			Name:            "coordinator",
			Image:           ImageRef(cluster, opts),
			ImagePullPolicy: imagePullPolicy(cluster),
			SecurityContext: cluster.Spec.Security.ContainerSecurityContext,
			Command:         []string{"sh", "-c"},
			Args:            []string{coordinatorStartupScript(cluster)},
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: CoordinatorGRPCPort},
				{Name: "raft", ContainerPort: CoordinatorRaftPort},
				{Name: "metrics", ContainerPort: CoordinatorMetricsPort},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/data/gvdb/coordinator"},
				{Name: "config", MountPath: "/etc/gvdb"},
			},
			ReadinessProbe: tcpProbe(CoordinatorGRPCPort, 5, 10),
			// Liveness has a 90s initialDelay to give Raft election runway
			// during parallel HA bootstrap (roadmap 0b.4).
			LivenessProbe: tcpProbe(CoordinatorGRPCPort, 90, 20),
			Resources:     spec.Resources,
		}},
		Volumes: []corev1.Volume{{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName(cluster)},
				},
			},
		}},
	}
	applyWorkloadCommon(&podSpec, spec.WorkloadCommon, SelectorLabels(cluster, CoordinatorComponent), cluster, CoordinatorComponent)

	// Partition is intentionally NOT set here. The reconciler's rollout
	// state machine (internal/controller/rollout.go) is the sole owner of
	// spec.updateStrategy.rollingUpdate.partition — it pins to replicas-1
	// the moment it detects a new revision and decrements pod-by-pod as
	// leader election confirms quorum (roadmap 0b.6.C). Setting partition
	// from both render + state machine creates a write loop via SSA.
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, CoordinatorComponent),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, CoordinatorComponent),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         WorkloadName(cluster, CoordinatorComponent),
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: SelectorLabels(cluster, CoordinatorComponent)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      SelectorLabels(cluster, CoordinatorComponent),
					Annotations: podTemplateAnnotations(opts),
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{dataPVC(spec.Storage, "1Gi")},
		},
	}
}

// podTemplateAnnotations returns the stable annotation map stamped onto every
// workload pod template. Today only the config-hash lives here; callers pass
// an empty map when opts.ConfigHash is unset (tests).
func podTemplateAnnotations(opts Options) map[string]string {
	if opts.ConfigHash == "" {
		return nil
	}
	return map[string]string{ConfigHashAnnotation: opts.ConfigHash}
}

// imagePullPolicy returns the spec's pull policy, defaulting to IfNotPresent.
func imagePullPolicy(cluster *gvdbv1alpha1.GVDBCluster) corev1.PullPolicy {
	if p := cluster.Spec.Image.PullPolicy; p != "" {
		return p
	}
	return corev1.PullIfNotPresent
}

// tcpProbe builds a TCP Readiness/Liveness probe on the given port.
func tcpProbe(port int, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(port)},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
	}
}

// dataPVC builds the "data" PersistentVolumeClaim template for a StatefulSet.
// Defaults to 1Gi when the spec doesn't set a size.
func dataPVC(spec gvdbv1alpha1.StorageSpec, defaultSize string) corev1.PersistentVolumeClaim {
	size := spec.Size
	if size.IsZero() {
		size = resource.MustParse(defaultSize)
	}
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &spec.StorageClassName
	}
	return pvc
}

// applyWorkloadCommon threads the fields shared by every workload onto the
// pod spec. Anti-affinity is applied via a dedicated helper that only takes
// effect when enabled; PriorityClass name + ServiceAccount name are read
// here but their full behavior (auto-wire when priorityClasses.create=true,
// create SA object) is wired up in the hardening renderer pass.
func applyWorkloadCommon(
	pod *corev1.PodSpec,
	wc gvdbv1alpha1.WorkloadCommon,
	selector map[string]string,
	cluster *gvdbv1alpha1.GVDBCluster,
	c Component,
) {
	if wc.PodAntiAffinity.Enabled {
		pod.Affinity = buildAntiAffinity(wc.PodAntiAffinity, selector)
	}
	if len(wc.TopologySpreadConstraints) > 0 {
		pod.TopologySpreadConstraints = wc.TopologySpreadConstraints
	}
	if name := resolvePriorityClassName(cluster, c, wc); name != "" {
		pod.PriorityClassName = name
	}
	if saName := resolveServiceAccountName(cluster, c, wc); saName != "" {
		pod.ServiceAccountName = saName
	}
}
