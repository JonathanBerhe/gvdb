# Configuration

Every GVDB binary reads a YAML config file and overrides from CLI flags and environment variables.

Precedence: **CLI flag > env var > YAML > default**.

## Minimal YAML

```yaml
server:
  grpc_port: 50051

storage:
  data_dir: /var/lib/gvdb

logging:
  level: info
```

Run with `--config /etc/gvdb/config.yaml` or `GVDB_CONFIG=/etc/gvdb/config.yaml`.

## Full schema

```yaml
server:
  grpc_port: 50051
  http_port: 8080                  # health endpoint, prometheus (:9090 by default)

  tls:
    enabled: false
    cert_path: /etc/gvdb/server.crt
    key_path: /etc/gvdb/server.key
    client_ca_path: /etc/gvdb/ca.crt   # for mutual TLS

  auth:
    enabled: false
    api_keys:                       # legacy — treated as admin
      - "legacy-key-1"
    rbac:
      users:
        - api_key: "admin-key"
          role: admin
          collections: ["*"]
        - api_key: "reader-key"
          role: readonly
          collections: ["products", "reviews"]

storage:
  data_dir: /var/lib/gvdb
  ttl_sweep_interval_seconds: 60

  segment:
    seal_size_bytes: 1073741824    # 1 GiB auto-seal threshold
    compaction_idle_seconds: 300

  wal:
    buffer_size_bytes: 16777216    # 16 MiB
    sync_interval_ms: 1000

  object_store:                     # tiered storage (S3 / MinIO)
    enabled: false
    endpoint: https://s3.amazonaws.com
    region: us-east-1
    bucket: gvdb-cold
    prefix: segments/
    access_key_id: ""
    secret_access_key: ""
    upload_threads: 4
    cache_size_gb: 50

index:
  memory_budget_gb: 4
  hnsw:
    m: 16
    ef_construction: 200
    ef_search: 64
  ivf:
    nlist: 16384
    nprobe: 32

cluster:
  node_id: 1
  role: single-node                 # coordinator | data-node | query-node | proxy | single-node
  bind_address: 0.0.0.0:50051
  advertise_address: ""             # defaults to bind_address
  coordinators:                     # required for data-node/query-node/proxy
    - localhost:50051

consensus:
  data_dir: /var/lib/gvdb/raft
  election_timeout_ms: 1000
  heartbeat_interval_ms: 100
  snapshot_distance: 10000

logging:
  level: info                       # trace | debug | info | warn | error
  format: json                      # json | text
  audit:
    enabled: false
    path: /var/log/gvdb/audit.jsonl
    rotation_mb: 100
    max_files: 10

metrics:
  prometheus_port: 9090
  enabled: true
```

## Environment variables

Common ones that map to the above:

| Env | YAML path |
|-----|-----------|
| `GVDB_CONFIG` | Path to YAML file |
| `GVDB_BIND_ADDRESS` | `cluster.bind_address` |
| `GVDB_ADVERTISE_ADDRESS` | `cluster.advertise_address` |
| `GVDB_DATA_DIR` | `storage.data_dir` |
| `GVDB_NODE_ID` | `cluster.node_id` |
| `GVDB_LOG_LEVEL` | `logging.level` |

## Per-node role

In a distributed deployment, each binary reads the same YAML but picks up role-specific fields:

- **Coordinator**: `cluster.role: coordinator`, `consensus.*`, `cluster.coordinators` (peers)
- **Data node**: `cluster.role: data-node`, `cluster.coordinators` (address list), `storage.*`
- **Query node**: `cluster.role: query-node`, `cluster.coordinators`
- **Proxy**: `cluster.role: proxy`, `cluster.coordinators`

The [Helm chart](deploy-helm.md) renders per-role ConfigMaps automatically.

## See also

- [Helm deploy](deploy-helm.md) — how values translate to config
- [Security](security.md) — TLS + RBAC production setup
- [Reference — CLI](../reference/cli.md) — every flag
