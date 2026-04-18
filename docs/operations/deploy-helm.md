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

All values can be set via `--set key=value` or a custom `values.yaml`.

### Replicas

| Parameter | Default | Description |
|-----------|---------|-------------|
| `coordinator.replicas` | `3` | Coordinators (use 3 for quorum) |
| `dataNode.replicas` | `2` | Data nodes — scale for storage + insert throughput |
| `queryNode.replicas` | `1` | Query nodes — scale for QPS |

### Service exposure

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proxy.service.type` | `ClusterIP` | `ClusterIP`, `NodePort`, or `LoadBalancer` |
| `proxy.service.port` | `50050` | gRPC port |

### Image

| Parameter | Default | Description |
|-----------|---------|-------------|
| `image.repository` | `ghcr.io/jonathanberhe/gvdb` | Container image |
| `image.tag` | `latest` | Image tag — pin to a version in production |
| `image.pullPolicy` | `IfNotPresent` | |

### Storage

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dataNode.storage.size` | `5Gi` | PVC size per data node |
| `dataNode.storage.storageClassName` | — | StorageClass (cluster default if unset) |
| `dataNode.memoryLimitGb` | `4` | Memory budget for vector data |

### Auth

| Parameter | Default | Description |
|-----------|---------|-------------|
| `auth.enabled` | `false` | Enable API key / RBAC |
| `auth.apiKeys` | `[]` | Legacy list of admin API keys |
| `auth.rbac` | `{}` | Full RBAC config (see [RBAC](../features/rbac.md)) |

### Tiered storage (S3 / MinIO)

```yaml
storage:
  objectStore:
    enabled: true
    endpoint: https://s3.amazonaws.com
    region: us-east-1
    bucket: gvdb-cold
    prefix: segments/
    cacheSizeGb: 50
    uploadThreads: 4
    accessKeyId: "<from Secret>"
    secretAccessKey: "<from Secret>"
```

See [tiered storage](../features/tiered-storage.md) for the full picture.

### Observability

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metrics.enabled` | `true` | Expose Prometheus metrics on `:9090` |
| `metrics.serviceMonitor.enabled` | `false` | Create a ServiceMonitor (requires Prometheus Operator) |

### Web UI

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ui.enabled` | `false` | Deploy the GVDB web UI alongside the cluster |
| `ui.service.type` | `ClusterIP` | |

See the full [`deploy/helm/gvdb/values.yaml`](https://github.com/JonathanBerhe/gvdb/blob/main/deploy/helm/gvdb/values.yaml) in the repo for every parameter.

## Example: production setup

```yaml title="values.prod.yaml"
coordinator:
  replicas: 3

dataNode:
  replicas: 5
  memoryLimitGb: 16
  storage:
    size: 200Gi
    storageClassName: gp3

queryNode:
  replicas: 3

proxy:
  service:
    type: LoadBalancer
  replicas: 2

auth:
  enabled: true
  rbac:
    users:
      - apiKey: "${ADMIN_KEY}"
        role: admin
        collections: ["*"]

storage:
  objectStore:
    enabled: true
    endpoint: https://s3.us-east-1.amazonaws.com
    region: us-east-1
    bucket: gvdb-cold
    cacheSizeGb: 100

metrics:
  serviceMonitor:
    enabled: true
```

```bash
helm upgrade --install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace \
  -f values.prod.yaml
```

## See also

- [Distributed cluster](../getting-started/distributed-cluster.md) — walkthrough
- [Configuration](configuration.md) — per-node YAML
- [Monitoring](monitoring.md)
- [Security](security.md)
