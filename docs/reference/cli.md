# CLI reference

Common flags for every GVDB binary. All flags have environment-variable equivalents via `GVDB_<UPPER_CASE_NAME>`.

## Shared flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config <path>` | — | Path to YAML config file |
| `--bind-address <host:port>` | `0.0.0.0:50051` | gRPC bind address |
| `--advertise-address <host:port>` | `<bind-address>` | Address advertised to other nodes |
| `--data-dir <path>` | `./gvdb-data` | Local data directory |
| `--node-id <int>` | `1` | Unique node ID (required for clustered binaries) |
| `--log-level <level>` | `info` | `trace` / `debug` / `info` / `warn` / `error` |
| `--help` | | Print help |
| `--version` | | Print version |

## `gvdb-single-node`

All-in-one server for dev and small workloads.

```bash
gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
```

| Flag | Description |
|------|-------------|
| `--port <int>` | gRPC port (alias for `--bind-address :PORT`) |
| `--http-port <int>` | Health + metrics port (default `8080`) |

## `gvdb-coordinator`

Cluster metadata via Raft consensus.

```bash
gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --peers 10.0.0.2:50051,10.0.0.3:50051
```

| Flag | Description |
|------|-------------|
| `--peers <list>` | Comma-separated list of peer coordinators |
| `--raft-dir <path>` | Raft log directory (default `<data-dir>/raft`) |

## `gvdb-data-node`

Sharded vector storage.

```bash
gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --coordinator localhost:50051
```

| Flag | Description |
|------|-------------|
| `--coordinator <host:port>` | Coordinator address (repeatable) |
| `--memory-limit-gb <int>` | Memory budget for vector storage |

## `gvdb-query-node`

Search fan-out and result merging.

```bash
gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --coordinator localhost:50051
```

## `gvdb-proxy`

Client entry point.

```bash
gvdb-proxy \
  --bind-address 0.0.0.0:50050 \
  --coordinators localhost:50051 \
  --data-nodes localhost:50060 \
  --query-nodes localhost:50070
```

| Flag | Description |
|------|-------------|
| `--coordinators <list>` | Coordinator addresses |
| `--data-nodes <list>` | Data node addresses (seed list) |
| `--query-nodes <list>` | Query node addresses (seed list) |

## Environment variables

Equivalent env vars override the same settings:

- `GVDB_CONFIG`
- `GVDB_BIND_ADDRESS`
- `GVDB_ADVERTISE_ADDRESS`
- `GVDB_DATA_DIR`
- `GVDB_NODE_ID`
- `GVDB_LOG_LEVEL`

Precedence: **flag > env var > YAML > default**.

## See also

- [Configuration](../operations/configuration.md) — full YAML schema
- [Deploy with Docker](../operations/deploy-docker.md)
- [Deploy with Helm](../operations/deploy-helm.md)
