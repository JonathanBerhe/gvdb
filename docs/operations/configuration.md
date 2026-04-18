# Configuration

Every GVDB binary reads a YAML config file. For the authoritative schema, see [`include/utils/config.h`](https://github.com/JonathanBerhe/gvdb/blob/main/include/utils/config.h); the struct field names map directly to YAML keys.

## Minimal YAML

```yaml
server:
  bind_address: "0.0.0.0"
  grpc_port: 50051

storage:
  data_dir: "/var/lib/gvdb"

logging:
  level: "info"
```

Run with `--config /etc/gvdb/config.yaml`.

## Full schema

```yaml
server:
  bind_address: "0.0.0.0"
  grpc_port: 50051
  metrics_port: 9090
  max_message_size_mb: 256
  max_concurrent_streams: 1000

  tls:
    enabled: false
    cert_path: ""
    key_path: ""
    ca_cert_path: ""          # for mutual TLS
    mutual_tls: false

  auth:
    enabled: false
    api_keys: []              # legacy: flat list, each key treated as admin

    roles:                    # RBAC — preferred over api_keys
      - key: "admin-key"
        role: admin           # admin | readwrite | readonly | collection_admin
        collections: ["*"]    # "*" = all collections

      - key: "reader-key"
        role: readonly
        collections: ["products", "reviews"]

storage:
  data_dir: "/tmp/gvdb"
  segment_max_size_mb: 512
  wal_buffer_size_mb: 64
  enable_compression: true
  compaction_threads: 4

  # Object store (S3/MinIO). Empty `object_store_type` disables tiered storage.
  object_store_type: ""            # "s3" or "minio"
  object_store_endpoint: ""        # e.g. "http://minio:9000"
  object_store_access_key: ""
  object_store_secret_key: ""
  object_store_bucket: ""
  object_store_region: ""
  object_store_prefix: ""
  object_store_use_ssl: true
  object_store_cache_size_mb: 256
  object_store_upload_threads: 2

index:
  default_index_type: "HNSW"       # FLAT | HNSW | IVF_FLAT | IVF_PQ | IVF_SQ
                                    # | TURBOQUANT | IVF_TURBOQUANT | AUTO
  hnsw_m: 16
  hnsw_ef_construction: 200
  hnsw_ef_search: 100
  ivf_nlist: 100
  use_gpu: false

logging:
  level: "info"                     # trace | debug | info | warn | error
  console_enabled: true
  file_enabled: true
  file_path: "/tmp/gvdb/logs/gvdb.log"
  max_file_size_mb: 100
  max_files: 10

  audit:
    enabled: false
    file_path: "/tmp/gvdb/logs/audit.jsonl"
    max_file_size_mb: 100
    max_files: 10

consensus:
  node_id: 1
  single_node_mode: true
  peers: []                         # list of peer addresses for coordinator role
  election_timeout_ms: 5000
  heartbeat_interval_ms: 1000
```

## Default config shipped with the Helm chart

The Helm chart renders a ConfigMap with a subset of these keys — see [Deploy with Helm](deploy-helm.md). The starting-point ConfigMap lives at [`deploy/k8s/configmap.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/k8s/configmap.yaml).

## Overriding in Helm deployments

The Helm chart doesn't expose `server.auth`, `server.tls`, or `storage.object_store_*` as values. Override by mounting a custom ConfigMap / Secret over the chart-rendered one, or by post-rendering Helm output.

Example — custom auth + TLS layered on top of the chart:

```yaml
# gvdb-config.yaml (mounted as ConfigMap, replaces chart-rendered config)
server:
  bind_address: "0.0.0.0"
  grpc_port: 50051
  tls:
    enabled: true
    cert_path: "/etc/gvdb/tls/server.crt"
    key_path: "/etc/gvdb/tls/server.key"
    mutual_tls: true
    ca_cert_path: "/etc/gvdb/tls/ca.crt"
  auth:
    enabled: true
    roles:
      - key: "admin-key"
        role: admin
        collections: ["*"]

storage:
  data_dir: "/data/gvdb"
  segment_max_size_mb: 512

index:
  default_index_type: "AUTO"

logging:
  level: "info"
  audit:
    enabled: true
```

## See also

- [Deploy with Helm](deploy-helm.md) — what the chart renders
- [Security](security.md) — TLS + auth + audit
- [CLI reference](../reference/cli.md) — flags that override config
- [Config reference](../reference/config.md) — condensed schema view
