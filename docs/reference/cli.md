# CLI reference

This page lists the flags of each GVDB program. The flag tables in [`src/main`](https://github.com/JonathanBerhe/gvdb/tree/main/src/main) are the source of this page. Run `<program> --help` to see the flags of the version that you have.

Each program exits with 0 after `--help` or `-h`. An unknown flag, or a flag without its value, prints the usage text and exits with 1. A list flag takes comma-separated values, and each use of the flag adds its values to the list.

## `gvdb-single-node`

All roles in one process.

```bash
gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
```

| Flag | Description |
|------|-------------|
| `--config FILE` | YAML configuration file. Optional. |
| `--port PORT` | gRPC port. Replaces `server.grpc_port`. Default `50051`. |
| `--data-dir PATH` | Data folder. Replaces `storage.data_dir`. Default `/tmp/gvdb`. |
| `--node-id ID` | Node ID. Replaces `consensus.node_id`. Default `1`. |
| `--help`, `-h` | Show the usage text. |

A flag replaces the value from the YAML file, and the YAML file replaces the built-in default. `gvdb-single-node` reads no environment variable.

## `gvdb-coordinator`

```bash
gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:8300 \
  --raft-peers 1:host-a:8300,2:host-b:8300,3:host-c:8300 \
  --coordinator-grpc-peers host-a:50051,host-b:50051,host-c:50051
```

| Flag | Description |
|------|-------------|
| `--node-id ID` | Node ID. Unique per coordinator. Default `1`. |
| `--bind-address ADDR` | gRPC bind address. Default `0.0.0.0:50051`. |
| `--advertise-address ADDR` | The coordinator reads this flag but does not use it. |
| `--raft-address ADDR` | Raft bind address. Only the port is used, because NuRaft binds `0.0.0.0`. Default `0.0.0.0:8300`. |
| `--raft-advertise-address ADDR` | The Raft address that peers dial. In Kubernetes, set it to the pod FQDN. Default: `--raft-address`. |
| `--raft-peers LIST` | Raft peers as `id:host:port`. The flag turns on multi-node mode. |
| `--coordinator-grpc-peers LIST` | Coordinator gRPC addresses for the startup peer probe and for the self-remove at shutdown. List them in the same order as `--raft-peers`: node ID 1 is the first. |
| `--data-dir PATH` | Data folder. Default `/tmp/gvdb/coordinator`. |
| `--config FILE` | YAML configuration file for the object store, auth, TLS and the audit log. |
| `--single-node` | Run Raft in single-node mode. This is the default when `--raft-peers` is not given. |
| `--help`, `-h` | Show the usage text. |

## `gvdb-data-node`

```bash
gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --coordinator localhost:50051
```

| Flag | Description |
|------|-------------|
| `--node-id ID` | Node ID. Default `101`. |
| `--bind-address ADDR` | gRPC bind address. Default `0.0.0.0:50060`. |
| `--advertise-address ADDR` | Address advertised to the coordinator. Default: the bind address. |
| `--data-dir PATH` | Data folder. Default `/tmp/gvdb/data_node`. |
| `--coordinator LIST` | Coordinator addresses. The node sends its heartbeat to the first one. Without this flag, the node runs alone. |
| `--config FILE` | YAML configuration file for the object store and the audit log. |
| `--memory-limit-gb SIZE` | Shown in the startup banner. The data node does not apply this limit. Default `8`. |
| `--drain-wait-seconds SEC` | At shutdown, the longest time to wait for the coordinator to move the shards off the node. Default `15`. |
| `--help`, `-h` | Show the usage text. |

## `gvdb-query-node`

```bash
gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --coordinator localhost:50051
```

| Flag | Description |
|------|-------------|
| `--node-id ID` | Node ID. Default `201`. |
| `--bind-address ADDR` | gRPC bind address. Default `0.0.0.0:50070`. |
| `--advertise-address ADDR` | Address advertised to the coordinator. Default: the bind address. |
| `--data-dir PATH` | Cache folder. Default `/tmp/gvdb/query_node`. |
| `--coordinator LIST` | Coordinator addresses. The node sends its heartbeat to the first one. |
| `--memory-limit-gb SIZE` | Shown in the startup banner. The query node does not apply this limit. Default `16`. |
| `--help`, `-h` | Show the usage text. |

## `gvdb-proxy`

```bash
gvdb-proxy \
  --bind-address 0.0.0.0:50050 \
  --coordinators localhost:50051 \
  --query-nodes dns:///gvdb-query-node.gvdb.svc.cluster.local:50070
```

| Flag | Description |
|------|-------------|
| `--node-id ID` | Node ID. Default `1`. |
| `--bind-address ADDR` | gRPC bind address. Default `0.0.0.0:50050`. |
| `--data-dir PATH` | Data folder for the log. Default `/tmp/gvdb/proxy`. |
| `--config FILE` | YAML configuration file for auth, TLS and the audit log. |
| `--coordinators LIST` | Coordinator addresses. Required. |
| `--query-nodes URI` | One gRPC target for all query nodes, for example `dns:///service:port` or `host:port`. The proxy sends reads to it. With a `dns:///` URI, gRPC balances the calls across all pods of the service. |
| `--help`, `-h` | Show the usage text. |

## Environment variables

The cluster programs read these environment variables. When a variable is set and not empty, its value replaces the flag value.

| Variable | Read by | Replaces |
|----------|---------|----------|
| `GVDB_BIND_ADDRESS` | coordinator, data node, query node, proxy | `--bind-address` |
| `GVDB_ADVERTISE_ADDRESS` | coordinator, data node, query node | `--advertise-address` |
| `GVDB_DATA_DIR` | coordinator, data node, query node, proxy | `--data-dir` |
| `GVDB_RAFT_ADDRESS` | coordinator | `--raft-address` |
| `GVDB_RAFT_ADVERTISE_ADDRESS` | coordinator | `--raft-advertise-address` |
| `GVDB_COORDINATOR_GRPC_PEERS` | coordinator | `--coordinator-grpc-peers` (the whole list) |

The order is: environment variable, then flag, then built-in default.

## See also

- [Configuration](../operations/configuration.md): the YAML schema
- [Deploy with Docker](../operations/deploy-docker.md)
- [Deploy with Helm](../operations/deploy-helm.md)
