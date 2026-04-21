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
	"strings"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConfigMap builds the server config ConfigMap mounted at /etc/gvdb on every
// workload. Mirrors deploy/helm/gvdb/templates/configmap.yaml; defaults match
// the Helm values.
func ConfigMap(cluster *gvdbv1alpha1.GVDBCluster) *corev1.ConfigMap {
	cfg := cluster.Spec.Config
	// Defaults mirror values.yaml + CRD +kubebuilder:default markers.
	server := cfg.Server
	server = withServerDefaults(server)
	storage := withStorageDefaults(cfg.Storage)
	index := withIndexDefaults(cfg.Index)
	logging := withLoggingDefaults(cfg.Logging)

	var b strings.Builder
	fmt.Fprintf(&b, "server:\n")
	fmt.Fprintf(&b, "  bind_address: \"0.0.0.0\"\n")
	fmt.Fprintf(&b, "  grpc_port: 50051\n")
	fmt.Fprintf(&b, "  metrics_port: 9090\n")
	fmt.Fprintf(&b, "  max_message_size_mb: %d\n", server.MaxMessageSizeMb)
	fmt.Fprintf(&b, "  max_concurrent_streams: %d\n", server.MaxConcurrentStreams)
	fmt.Fprintf(&b, "storage:\n")
	fmt.Fprintf(&b, "  data_dir: \"/data/gvdb\"\n")
	fmt.Fprintf(&b, "  segment_max_size_mb: %d\n", storage.SegmentMaxSizeMb)
	fmt.Fprintf(&b, "  wal_buffer_size_mb: %d\n", storage.WalBufferSizeMb)
	fmt.Fprintf(&b, "  enable_compression: %t\n", storage.EnableCompression)
	fmt.Fprintf(&b, "  compaction_threads: %d\n", storage.CompactionThreads)
	fmt.Fprintf(&b, "index:\n")
	fmt.Fprintf(&b, "  default_index_type: %q\n", index.DefaultIndexType)
	fmt.Fprintf(&b, "  hnsw_m: %d\n", index.HnswM)
	fmt.Fprintf(&b, "  hnsw_ef_construction: %d\n", index.HnswEfConstruction)
	fmt.Fprintf(&b, "  hnsw_ef_search: %d\n", index.HnswEfSearch)
	fmt.Fprintf(&b, "  use_gpu: false\n")
	fmt.Fprintf(&b, "logging:\n")
	fmt.Fprintf(&b, "  level: %q\n", logging.Level)
	fmt.Fprintf(&b, "  console_enabled: %t\n", logging.ConsoleEnabled)
	fmt.Fprintf(&b, "  file_enabled: %t\n", logging.FileEnabled)
	fmt.Fprintf(&b, "consensus:\n")
	fmt.Fprintf(&b, "  node_id: 1\n")
	fmt.Fprintf(&b, "  single_node_mode: true\n")
	fmt.Fprintf(&b, "  election_timeout_ms: 5000\n")
	fmt.Fprintf(&b, "  heartbeat_interval_ms: 1000\n")
	fmt.Fprintf(&b, "  peers: []\n")

	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapName(cluster),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "gvdb-operator",
				"app.kubernetes.io/part-of":    "gvdb",
			},
		},
		Data: map[string]string{"config.yaml": b.String()},
	}
}

func withServerDefaults(s gvdbv1alpha1.ServerConfig) gvdbv1alpha1.ServerConfig {
	if s.MaxMessageSizeMb == 0 {
		s.MaxMessageSizeMb = 256
	}
	if s.MaxConcurrentStreams == 0 {
		s.MaxConcurrentStreams = 1000
	}
	return s
}

func withStorageDefaults(s gvdbv1alpha1.StorageConfig) gvdbv1alpha1.StorageConfig {
	if s.SegmentMaxSizeMb == 0 {
		s.SegmentMaxSizeMb = 512
	}
	if s.WalBufferSizeMb == 0 {
		s.WalBufferSizeMb = 64
	}
	if s.CompactionThreads == 0 {
		s.CompactionThreads = 2
	}
	return s
}

func withIndexDefaults(i gvdbv1alpha1.IndexConfig) gvdbv1alpha1.IndexConfig {
	if i.DefaultIndexType == "" {
		i.DefaultIndexType = "HNSW"
	}
	if i.HnswM == 0 {
		i.HnswM = 16
	}
	if i.HnswEfConstruction == 0 {
		i.HnswEfConstruction = 200
	}
	if i.HnswEfSearch == 0 {
		i.HnswEfSearch = 100
	}
	return i
}

func withLoggingDefaults(l gvdbv1alpha1.LoggingConfig) gvdbv1alpha1.LoggingConfig {
	if l.Level == "" {
		l.Level = "info"
	}
	return l
}
