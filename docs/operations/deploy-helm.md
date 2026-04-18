# Deploy with Helm

The official Helm chart is published to GitHub Container Registry as an OCI artifact.

## Install

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace
```

Connect from outside the cluster:

```bash
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050
```

## Upgrade

```bash
helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb \
  --set image.tag=v1.2.0
```

## Values reference

The chart is intentionally minimal. Exposed keys mirror [`deploy/helm/gvdb/values.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/helm/gvdb/values.yaml) — follow that file for the source of truth.

### `image`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `image.repository` | `gvdb` | Container image name |
| `image.tag` | `""` (falls back to `Chart.appVersion`) | Image tag |
| `image.pullPolicy` | `IfNotPresent` | |

### `coordinator`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `coordinator.replicas` | `1` | Coordinators (use 3 for Raft quorum in production) |
| `coordinator.singleNode` | `true` | Run as single-node embedded coordinator |
| `coordinator.resources` | see values.yaml | CPU/memory requests and limits |
| `coordinator.storage.size` | `1Gi` | PVC size for Raft log |
| `coordinator.storage.storageClass` | `""` (cluster default) | StorageClass override |

### `dataNode`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dataNode.replicas` | `2` | Data nodes — scale for storage + insert throughput |
| `dataNode.memoryLimitGb` | `4` | Memory budget for vector storage |
| `dataNode.resources` | see values.yaml | CPU/memory requests and limits |
| `dataNode.storage.size` | `5Gi` | PVC size per data node |
| `dataNode.storage.storageClass` | `""` | StorageClass override |

### `queryNode`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `queryNode.replicas` | `1` | Query nodes — scale for QPS |
| `queryNode.memoryLimitGb` | `4` | Memory budget |
| `queryNode.resources` | see values.yaml | CPU/memory requests and limits |
| `queryNode.storage.size` | `2Gi` | PVC size |
| `queryNode.storage.storageClass` | `""` | StorageClass override |

### `proxy`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proxy.replicas` | `1` | Proxy replicas (horizontally scalable) |
| `proxy.resources` | see values.yaml | CPU/memory requests and limits |
| `proxy.service.type` | `ClusterIP` | `ClusterIP`, `NodePort`, or `LoadBalancer` |
| `proxy.service.port` | `50050` | gRPC port |
| `proxy.service.nodePort` | `""` | Explicit NodePort when `type: NodePort` |

### `config` (server-side config ConfigMap)

| Parameter | Default |
|-----------|---------|
| `config.server.maxMessageSizeMb` | `256` |
| `config.server.maxConcurrentStreams` | `1000` |
| `config.storage.segmentMaxSizeMb` | `512` |
| `config.storage.walBufferSizeMb` | `64` |
| `config.storage.enableCompression` | `true` |
| `config.storage.compactionThreads` | `2` |
| `config.index.defaultIndexType` | `"HNSW"` |
| `config.index.hnswM` | `16` |
| `config.index.hnswEfConstruction` | `200` |
| `config.index.hnswEfSearch` | `100` |
| `config.logging.level` | `"info"` |
| `config.logging.consoleEnabled` | `true` |
| `config.logging.fileEnabled` | `false` |

### `ui`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ui.enabled` | `false` | Deploy the GVDB Web UI alongside the cluster |
| `ui.image.repository` | `ghcr.io/jonathanberhe/gvdb-ui` | UI image |
| `ui.image.tag` | `latest` | UI tag |
| `ui.port` | `8080` | Container port |
| `ui.service.type` | `ClusterIP` | |
| `ui.service.port` | `8080` | |

## What the chart does **not** surface (yet)

The following are **not Helm-parameterized**. Configure them by mounting a custom `gvdb-config.yaml` ConfigMap / Secret that overrides the values the chart renders, or patch the StatefulSet directly:

- **Authentication** / **RBAC** — API keys, RBAC users (see [Security](security.md))
- **TLS** — mutual TLS material (certificates, keys)
- **Audit logging**
- **Prometheus `ServiceMonitor`**
- **Object storage** (S3 / MinIO) for [tiered storage](../features/tiered-storage.md) — the server supports it, but the chart doesn't expose the knobs

Contributions to expose these in the Helm chart are welcome.

## Example: small production setup

```yaml title="values.prod.yaml"
image:
  tag: v1.2.0

coordinator:
  replicas: 3
  singleNode: false

dataNode:
  replicas: 5
  memoryLimitGb: 16
  storage:
    size: 200Gi
    storageClass: gp3

queryNode:
  replicas: 3

proxy:
  replicas: 2
  service:
    type: LoadBalancer

config:
  index:
    defaultIndexType: "AUTO"
  logging:
    level: "info"

ui:
  enabled: true
```

```bash
helm upgrade --install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  -f values.prod.yaml
```

## See also

- [Distributed cluster](../getting-started/distributed-cluster.md) — walkthrough
- [Configuration](configuration.md) — the server-side YAML the chart renders
- [Security](security.md) — how to layer auth/TLS on top of the chart
