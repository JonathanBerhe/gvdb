# Distributed cluster

Run GVDB as a full distributed topology for production workloads.

## Topology

A distributed GVDB cluster has four roles:

| Role | Purpose |
|------|---------|
| **Coordinator** | Cluster metadata via Raft consensus. Typically 3 replicas for quorum. |
| **Data node** | Sharded vector storage and indexing. Scale horizontally for capacity. |
| **Query node** | Distributed search with fan-out and result merging. Scale for QPS. |
| **Proxy** | Client entry point, request routing, load balancing. |

Clients always talk to the proxy. See the [architecture overview](../architecture/overview.md) for the full diagram.

## Deploy with Helm

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  --set dataNode.replicas=3 \
  --set queryNode.replicas=2 \
  --set proxy.service.type=LoadBalancer
```

Wait for pods:

```bash
kubectl wait --for=condition=ready pod --all -n gvdb --timeout=120s
kubectl get pods -n gvdb
```

You should see 3 coordinators, 3 data nodes, 2 query nodes, and a proxy.

## Connect

```bash
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050
```

Then from your app:

```python
from gvdb import GVDBClient
client = GVDBClient("localhost:50050")
```

In a `LoadBalancer` setup, point the client at the external IP of the `gvdb-proxy` service.

## Scale out

Add more data nodes when storage or insert throughput is the bottleneck:

```bash
helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb \
  --set dataNode.replicas=5
```

Add more query nodes when search QPS is the bottleneck:

```bash
helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb \
  --set queryNode.replicas=4
```

## Sharding

Collections are distributed across data nodes using consistent hashing (150 virtual nodes). No manual shard configuration — the coordinator assigns shards on collection creation and rebalances when nodes join or leave.

See [Architecture — storage](../architecture/storage.md) for details on the sharding strategy.

## Replication and fault tolerance

- Data nodes replicate segments for durability; replica count is configurable per collection.
- The coordinator auto-detects node failures via heartbeat and promotes replicas.
- See [Architecture — consensus](../architecture/consensus.md) for the Raft implementation.

## Persistence and tiered storage

- Sealed segments flush to local disk for durability.
- Enable [tiered storage](../features/tiered-storage.md) to offload cold segments to S3 or MinIO automatically.

## Monitoring

- Every node exposes a Prometheus endpoint.
- The [Web UI](../operations/monitoring.md) provides a browser-based collection browser and metrics dashboard.

## Next steps

- [Helm values reference](../operations/deploy-helm.md) — every configurable parameter
- [Configuration](../operations/configuration.md) — per-node YAML config
- [Security](../operations/security.md) — TLS, API keys, RBAC
