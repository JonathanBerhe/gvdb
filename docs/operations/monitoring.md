# Monitoring

Every GVDB binary exposes Prometheus metrics and a health endpoint. Dashboards for Grafana are pre-provisioned, and a Web UI ships alongside for interactive investigation.

## Prometheus metrics

Each node exposes metrics on `server.metrics_port` (default `9090`):

```bash
curl http://<node>:9090/metrics
```

Exposed metric families cover RPCs (rate, latency, errors), segments (counts by state, sizes), index builds (queue depth), the query result cache, replication, and Raft. The exact set evolves across versions — scrape the endpoint to see what's live on your deployment.

## ServiceMonitor

The Helm chart does not render a `ServiceMonitor` yet. To add one, create a Kubernetes manifest that selects the GVDB pods' metrics port:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: gvdb
  namespace: gvdb
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: gvdb
  endpoints:
    - port: metrics
      interval: 30s
```

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

OpenTelemetry support is on the roadmap — see the repo's `ROADMAP.md`.

## See also

- [Security](security.md) — audit logging details
- [Configuration](configuration.md) — metrics and logging options
