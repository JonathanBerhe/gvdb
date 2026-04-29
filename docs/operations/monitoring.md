# Monitoring

Every GVDB binary exposes Prometheus metrics and a health endpoint. Dashboards for Grafana are pre-provisioned, and a Web UI ships alongside for interactive investigation.

## Prometheus metrics

Each binary exposes a `/metrics` endpoint on a workload-specific port:

| Workload | Port | Notes |
|----------|------|-------|
| coordinator | `9090 + node_id` (e.g. `9091` for `node_id=1`) | Per-replica; HA Raft cluster has one port per coordinator |
| proxy | `9050` | Fixed |
| data-node | `9100 + (node_id - 100)` (e.g. `9101` for `node_id=101`) | Per-replica |
| query-node | `9200 + (node_id - 200)` (e.g. `9201` for `node_id=201`) | Per-replica |

```bash
curl http://<node>:9091/metrics      # coordinator-0
curl http://<node>:9050/metrics      # proxy
```

Exposed metric families cover RPCs (rate, latency, errors), segments (counts by state, sizes), index builds (queue depth), the query result cache, replication, and Raft. The exact set evolves across versions — scrape the endpoint to see what's live on your deployment.

## Prometheus Operator (PodMonitor / ServiceMonitor)

The Helm chart renders Prometheus Operator resources for every workload — opt-in via `metrics.serviceMonitor.enabled=true`:

```yaml
metrics:
  serviceMonitor:
    enabled: true
    additionalLabels:
      release: kube-prometheus-stack    # match your Prometheus's selector
    interval: 30s
```

Because each StatefulSet replica binds metrics on an ordinal-derived port (see the table above), the chart uses **PodMonitor** for coordinator, data-node, and query-node, with relabel rules that rewrite `__address__` to `<pod_ip>:<derived_port>`. The proxy uses **ServiceMonitor** (fixed port). All four resources land in the release namespace by default; override via `metrics.serviceMonitor.namespace`.

See [Deploy with Helm — `metrics`](deploy-helm.md#metrics-prometheus-operator) for the full values reference, and [Configuration](configuration.md) for how to override the metrics port server-side if needed.

## Grafana dashboards

GVDB ships Grafana dashboards following the RED method — **R**ate, **E**rrors, **D**uration. Auto-provisioned via docker-compose for local development; for K8s, import the JSON from `grafana/dashboards/` in the repo.

Key panels:

- **Requests per second** per RPC
- **Error rate** (% of non-`OK` responses)
- **Latency p50 / p95 / p99** per RPC
- **Index build queue depth**
- **Segment counts by state**
- **Replication lag**

## Health check

Every node implements the `HealthCheck` gRPC method on its main port. From a client:

```python
from gvdb import GVDBClient
GVDBClient("localhost:50051").health_check()
```

The Helm chart wires Kubernetes readiness/liveness probes to this RPC.

## Web UI

The `gvdb-ui` binary provides a browser-based collection browser, search playground, and metrics dashboard.

=== "Docker"

    ```bash
    docker run -p 8080:8080 \
      ghcr.io/jonathanberhe/gvdb-ui \
      --gvdb-addr host.docker.internal:50051
    ```

=== "Helm"

    ```bash
    helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
      --set ui.enabled=true
    kubectl port-forward -n gvdb svc/gvdb-ui 8080:8080
    ```

=== "From source"

    ```bash
    make build-ui
    ./ui/gateway/gvdb-ui --gvdb-addr localhost:50051
    ```

Open `http://localhost:8080`.

## Audit logging

Structured JSON audit logs for every non-public RPC. Enable in config:

```yaml
logging:
  audit:
    enabled: true
    file_path: /var/log/gvdb/audit.jsonl
    max_file_size_mb: 100
    max_files: 10
```

Each line records `timestamp`, `api_key_id`, `operation`, `collection`, `status`, `grpc_code`, `latency_ms`, `item_count`. See [RBAC](../features/rbac.md#audit-logging).

## Tracing

OpenTelemetry distributed tracing is planned.

## See also

- [Security](security.md) — audit logging details
- [Configuration](configuration.md) — metrics and logging options
