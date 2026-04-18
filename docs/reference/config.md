# Config reference

Condensed YAML schema for the GVDB server. For the per-field narrative and production examples, see [Configuration](../operations/configuration.md). **Authoritative source**: [`include/utils/config.h`](https://github.com/JonathanBerhe/gvdb/blob/main/include/utils/config.h).

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
    api_keys: []              # legacy: flat list, each treated as admin
    roles: []                 # RBAC — preferred
      # - key: "admin-key"
      #   role: admin         # admin | readwrite | readonly | collection_admin
      #   collections: ["*"]

storage:
  data_dir: "/tmp/gvdb"
  segment_max_size_mb: 512
  wal_buffer_size_mb: 64
  enable_compression: true
  compaction_threads: 4

  object_store_type: ""       # "s3" or "minio"; empty = disabled
  object_store_endpoint: ""
  object_store_access_key: ""
  object_store_secret_key: ""
  object_store_bucket: ""
  object_store_region: ""
  object_store_prefix: ""
  object_store_use_ssl: true
  object_store_cache_size_mb: 256
  object_store_upload_threads: 2

index:
  default_index_type: "HNSW"  # FLAT | HNSW | IVF_FLAT | IVF_PQ | IVF_SQ
                               # | TURBOQUANT | IVF_TURBOQUANT | AUTO
  hnsw_m: 16
  hnsw_ef_construction: 200
  hnsw_ef_search: 100
  ivf_nlist: 100
  use_gpu: false

logging:
  level: "info"               # trace | debug | info | warn | error
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
  peers: []
  election_timeout_ms: 5000
  heartbeat_interval_ms: 1000
```

## See also

- [Configuration](../operations/configuration.md) — per-field narrative
- [CLI reference](cli.md) — flags that override config
- [Helm chart](../operations/deploy-helm.md) — `config.*` subset surfaced as Helm values
