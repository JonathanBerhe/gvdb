# Monitoring

Every GVDB binary exposes Prometheus metrics and a health endpoint. Dashboards for Grafana are pre-provisioned, and a Web UI ships alongside for interactive investigation.

## Prometheus metrics

Each node exposes metrics on port `9090` by default:

```bash
curl http://<node>:9090/metrics
```

Key metric families:

| Metric | Description |
|--------|-------------|
| `gvdb_rpc_requests_total` | Counter per RPC, labelled by method + status |
| `gvdb_rpc_duration_seconds` | Latency histogram per RPC |
| `gvdb_collection_vectors` | Current vector count per collection |
| `gvdb_segment_count` | Segments by state (growing/sealed/flushed) |
| `gvdb_index_build_queue_size` | Pending sealed segments awaiting index build |
| `gvdb_cache_hits_total` / `gvdb_cache_misses_total` | Query result cache |
| `gvdb_replication_lag_bytes` | Per-replica replication lag |
| `gvdb_raft_*` | Coordinator consensus metrics |

## ServiceMonitor (Prometheus Operator)

Enable via Helm:

```yaml
metrics:
  serviceMonitor:
    enabled: true
```

This creates a `ServiceMonitor` resource that Prometheus Operator picks up automatically.

## Grafana dashboards

GVDB ships Grafana dashboards following the RED method — **R**ate, **E**rrors, **D**uration. Auto-provisioned via docker-compose for local development; for K8s, import the JSON from `grafana/dashboards/` in the repo.

Key panels:

- **Requests per second** per RPC
- **Error rate** (% of non-`OK` responses)
- **Latency p50 / p95 / p99** per RPC
- **Index build queue depth**
- **Segment counts by state**
- **Replication lag**

## Health endpoint

```bash
curl http://<node>:8080/health
# {"status":"ok","version":"0.16.0","uptime_seconds":3600}
```

Kubernetes readiness and liveness probes point here in the [Helm chart](deploy-helm.md).

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
    path: /var/log/gvdb/audit.jsonl
```

Each line records `timestamp`, `api_key_id`, `operation`, `collection`, `status`, `grpc_code`, `latency_ms`, `item_count`. See [RBAC](../features/rbac.md#audit-logging).

## Tracing

OpenTelemetry support is on the roadmap — see the repo's `ROADMAP.md`.

## See also

- [Security](security.md) — audit logging details
- [Configuration](configuration.md) — metrics and logging options
