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
	"k8s.io/apimachinery/pkg/util/intstr"
)

// CoordinatorService renders the headless service fronting the coordinator
// StatefulSet. Matches deploy/helm/gvdb/templates/coordinator-service.yaml.
func CoordinatorService(cluster *gvdbv1alpha1.GVDBCluster) *corev1.Service {
	return headlessService(cluster, CoordinatorComponent, []corev1.ServicePort{
		port("grpc", CoordinatorGRPCPort),
		port("raft", CoordinatorRaftPort),
		port("metrics", CoordinatorMetricsPort),
	})
}

// DataNodeService renders the headless service fronting the data-node
// StatefulSet. Matches deploy/helm/gvdb/templates/data-node-service.yaml.
func DataNodeService(cluster *gvdbv1alpha1.GVDBCluster) *corev1.Service {
	return headlessService(cluster, DataNodeComponent, []corev1.ServicePort{
		port("grpc", DataNodeGRPCPort),
	})
}

// QueryNodeService renders the headless service fronting the query-node
// StatefulSet. Matches deploy/helm/gvdb/templates/query-node-service.yaml.
func QueryNodeService(cluster *gvdbv1alpha1.GVDBCluster) *corev1.Service {
	return headlessService(cluster, QueryNodeComponent, []corev1.ServicePort{
		port("grpc", QueryNodeGRPCPort),
	})
}

// ProxyService renders the (optionally NodePort) ClusterIP service in front of
// the proxy Deployment. Matches deploy/helm/gvdb/templates/proxy-service.yaml.
func ProxyService(cluster *gvdbv1alpha1.GVDBCluster) *corev1.Service {
	svcType := cluster.Spec.Proxy.Service.Type
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	port := cluster.Spec.Proxy.Service.Port
	if port == 0 {
		port = ProxyGRPCPort
	}
	grpc := corev1.ServicePort{
		Name:       "grpc",
		Port:       port,
		TargetPort: intstr.FromInt(ProxyGRPCPort),
	}
	if svcType == corev1.ServiceTypeNodePort && cluster.Spec.Proxy.Service.NodePort != 0 {
		grpc.NodePort = cluster.Spec.Proxy.Service.NodePort
	}
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, ProxyComponent),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, ProxyComponent),
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: SelectorLabels(cluster, ProxyComponent),
			Ports: []corev1.ServicePort{
				grpc,
				namedMetricsPort("metrics", ProxyMetricsPort),
			},
		},
	}
}

func headlessService(cluster *gvdbv1alpha1.GVDBCluster, c Component, ports []corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkloadName(cluster, c),
			Namespace: cluster.Namespace,
			Labels:    Labels(cluster, c),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  SelectorLabels(cluster, c),
			Ports:     ports,
		},
	}
}

func port(name string, p int) corev1.ServicePort {
	portInt := int32(p)
	return corev1.ServicePort{
		Name:       name,
		Port:       portInt,
		TargetPort: intstr.FromInt32(portInt),
	}
}

func namedMetricsPort(name string, p int) corev1.ServicePort {
	return corev1.ServicePort{
		Name:       name,
		Port:       int32(p),
		TargetPort: intstr.FromInt(p),
	}
}
