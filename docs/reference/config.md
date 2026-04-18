# Config reference

Full YAML schema for every GVDB binary. Values shown are defaults.

See [operations/configuration](../operations/configuration.md) for how precedence and env vars work.

```yaml
server:
  grpc_port: 50051
  http_port: 8080

  tls:
    enabled: false
    cert_path: ""
    key_path: ""
    client_ca_path: ""          # mutual TLS

  auth:
    enabled: false

    api_keys: []                # legacy: all keys treated as admin

    rbac:
      users: []
      # - api_key: "admin-key"
      #   role: admin            # admin | readwrite | readonly | collection_admin
      #   collections: ["*"]     # "*" = all collections

storage:
  data_dir: "./gvdb-data"
  ttl_sweep_interval_seconds: 60

  segment:
    seal_size_bytes: 1073741824 # 1 GiB
    compaction_idle_seconds: 300
    flush_on_seal: true

  wal:
    buffer_size_bytes: 16777216 # 16 MiB
    sync_interval_ms: 1000

  object_store:
    enabled: false
    endpoint: ""                 # e.g. https://s3.amazonaws.com or http://minio:9000
    region: "us-east-1"
    bucket: ""
    prefix: "segments/"
    access_key_id: ""
    secret_access_key: ""
    upload_threads: 4
    cache_size_gb: 10

index:
  memory_budget_gb: 4

  hnsw:
    m: 16
    ef_construction: 200
    ef_search: 64

  ivf:
    nlist: 16384
    nprobe: 32

  pq:
    m: 96
    nbits: 8

  turboquant:
    bits: 4                     # 1 | 2 | 4 | 8

cluster:
  node_id: 1
  role: "single-node"           # single-node | coordinator | data-node | query-node | proxy
  bind_address: "0.0.0.0:50051"
  advertise_address: ""         # defaults to bind_address
  coordinators: []              # required for non-coordinator roles
  peers: []                     # required for coordinator role
  heartbeat_interval_ms: 1000

consensus:
  data_dir: ""                  # defaults to <storage.data_dir>/raft
  election_timeout_ms: 1000
  heartbeat_interval_ms: 100
  snapshot_distance: 10000
  snapshot_creation_ms: 60000

query_cache:
  enabled: true
  max_entries: 10000
  ttl_seconds: 300

metrics:
  enabled: true
  prometheus_port: 9090

logging:
  level: "info"                 # trace | debug | info | warn | error
  format: "json"                # json | text

  audit:
    enabled: false
    path: "/var/log/gvdb/audit.jsonl"
    rotation_mb: 100
    max_files: 10
```

## Per-role required fields

### Single-node

No `cluster.peers` or `cluster.coordinators` required.

### Coordinator

```yaml
cluster:
  role: coordinator
  node_id: 1
  peers:
    - coord-2:50051
    - coord-3:50051
consensus:
  data_dir: /var/lib/gvdb/raft
```

### Data node

```yaml
cluster:
  role: data-node
  node_id: 101
  coordinators:
    - coord-1:50051
    - coord-2:50051
    - coord-3:50051
```

### Query node

```yaml
cluster:
  role: query-node
  node_id: 201
  coordinators:
    - coord-1:50051
    - coord-2:50051
    - coord-3:50051
```

### Proxy

```yaml
cluster:
  role: proxy
  coordinators:
    - coord-1:50051
    - coord-2:50051
    - coord-3:50051
```

## See also

- [Configuration (operations)](../operations/configuration.md)
- [CLI reference](cli.md)
- [Helm chart](../operations/deploy-helm.md)
