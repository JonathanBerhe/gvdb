# CLI reference

Flag lists for each GVDB binary. **Authoritative source**: [`src/main/*.cpp`](https://github.com/JonathanBerhe/gvdb/tree/main/src/main) — run `<binary> --help` for the version you have installed.

Flag vs. env-var vs. YAML: a CLI flag overrides the environment variable, which overrides the YAML config, which overrides the built-in default.

## `gvdb-single-node`

```bash
gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
```

| Flag | Description |
|------|-------------|
| `--config FILE` | YAML config file (optional) |
| `--port PORT` | gRPC server port. Default `50051`. |
| `--data-dir PATH` | Data directory. Default `/tmp/gvdb`. |
| `--node-id ID` | Node ID. Default `1`. |
| `--help`, `-h` | Show help |

## `gvdb-coordinator`

```bash
gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:50052 \
  --raft-peers 2@host-b:50052,3@host-c:50052
```

| Flag | Description |
|------|-------------|
| `--config FILE` | YAML config file |
| `--node-id ID` | Node ID (unique per coordinator) |
| `--bind-address HOST:PORT` | gRPC bind address |
| `--advertise-address HOST:PORT` | Address advertised to peers (defaults to bind) |
| `--raft-address HOST:PORT` | Raft transport bind address |
| `--raft-peers ID@HOST:PORT,...` | Peer coordinators |
| `--data-dir PATH` | Data directory |
| `--single-node` | Run as a single-node Raft (no peers required) |
| `--help`, `-h` | Show help |

## `gvdb-data-node`

```bash
gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --coordinator localhost:50051
```

| Flag | Description |
|------|-------------|
| `--config FILE` | YAML config file |
| `--node-id ID` | Node ID |
| `--bind-address HOST:PORT` | gRPC bind address |
| `--advertise-address HOST:PORT` | Address advertised to cluster |
| `--coordinator HOST:PORT` | Coordinator address (repeatable for HA) |
| `--shards N` | Number of shards this node owns |
| `--memory-limit-gb N` | Memory budget for vector data |
| `--data-dir PATH` | Data directory |
| `--help`, `-h` | Show help |

## `gvdb-query-node`

```bash
gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --coordinator localhost:50051
```

| Flag | Description |
|------|-------------|
| `--node-id ID` | Node ID |
| `--bind-address HOST:PORT` | gRPC bind address |
| `--advertise-address HOST:PORT` | Address advertised to cluster |
| `--coordinator HOST:PORT` | Coordinator address |
| `--memory-limit-gb N` | Memory budget |
| `--data-dir PATH` | Data directory |
| `--help`, `-h` | Show help |

## `gvdb-proxy`

```bash
gvdb-proxy \
  --bind-address 0.0.0.0:50050 \
  --coordinators localhost:50051 \
  --data-nodes localhost:50060 \
  --query-nodes localhost:50070
```

| Flag | Description |
|------|-------------|
| `--config FILE` | YAML config file |
| `--node-id ID` | Node ID |
| `--bind-address HOST:PORT` | gRPC bind address |
| `--coordinators LIST` | Comma-separated coordinator addresses |
| `--data-nodes LIST` | Comma-separated data-node addresses (seed list) |
| `--query-nodes LIST` | Comma-separated query-node addresses (seed list) |
| `--data-dir PATH` | Data directory |
| `--help`, `-h` | Show help |

## Environment variables

Partial env-var override is supported for cloud-native deployments:

| Var | Honored by | Replaces |
|-----|------------|----------|
| `GVDB_BIND_ADDRESS` | all clustered binaries | `--bind-address` |
| `GVDB_ADVERTISE_ADDRESS` | coordinator, data-node, query-node | `--advertise-address` |
| `GVDB_DATA_DIR` | all | `--data-dir` |
| `GVDB_RAFT_ADDRESS` | coordinator | `--raft-address` |

Precedence: **flag > env var > YAML > default**.

## See also

- [Configuration](../operations/configuration.md) — full YAML schema
- [Deploy with Docker](../operations/deploy-docker.md)
- [Deploy with Helm](../operations/deploy-helm.md)
