# GVDB Grafana Dashboard

Production-ready Grafana dashboard for monitoring GVDB performance and health.

## Dashboard Overview

The dashboard provides real-time monitoring across 4 key areas:

### 1. Request Rates
- **Insert Rate**: Inserts per second by collection and status
- **Search Rate**: Searches per second by collection and status
- **Total QPS**: Combined query throughput

### 2. Latency
- **Insert Latency**: p50, p95, p99 percentiles by collection
- **Search Latency**: p50, p95, p99 percentiles by collection

### 3. Success & Errors
- **Insert Success Rate**: Percentage of successful inserts
- **Search Success Rate**: Percentage of successful searches
- **Error Rate**: Errors per second by operation and collection

### 4. Data Volume
- **Total Vectors**: Sum across all collections
- **Collections**: Total number of collections
- **Vectors by Collection**: Time series breakdown
- **Insert Batch Size**: Average vectors per batch
- **Vectors Inserted**: Insert rate over time

## Quick Start with Docker Compose

This folder holds a Docker Compose stack: Prometheus (`prometheus.yml`) and Grafana with the data source and this dashboard provisioned (`provisioning/`, `gvdb-dashboard.json`). Run these commands from the repository root:

```bash
make run             # gvdb-single-node on localhost:50051, metrics on :9090
make monitoring-up   # Prometheus on localhost:9091, Grafana on localhost:3000
```

Open Grafana at http://localhost:3000 and log in with `admin` / `admin`. The dashboard is in the GVDB folder. Prometheus scrapes `host.docker.internal:9090`, which is the server that `make run` started on this host.

To stop everything:

```bash
make stop
make monitoring-down
```

## Manual Setup (Without Docker)

### Install Prometheus

**macOS:**
```bash
brew install prometheus
```

**Linux:**
```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xvf prometheus-2.45.0.linux-amd64.tar.gz
cd prometheus-2.45.0.linux-amd64
```

**Configure Prometheus** (`prometheus.yml`):
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'gvdb'
    static_configs:
      - targets: ['localhost:9090']
```

**Start Prometheus:**
```bash
prometheus --config.file=prometheus.yml
```

### Install Grafana

**macOS:**
```bash
brew install grafana
brew services start grafana
```

**Linux:**
```bash
sudo apt-get install -y software-properties-common
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
sudo apt-get update
sudo apt-get install grafana
sudo systemctl start grafana-server
```

**Access Grafana**: http://localhost:3000

## Key Metrics Reference

### Counters
- `gvdb_insert_requests_total{collection, status}` - Total insert requests
- `gvdb_insert_vectors_total{collection}` - Total vectors inserted
- `gvdb_search_requests_total{collection, status}` - Total search requests

### Histograms
- `gvdb_insert_duration_seconds{collection}` - Insert latency distribution
- `gvdb_search_duration_seconds{collection}` - Search latency distribution
- `gvdb_insert_batch_size` - Batch size distribution

### Gauges
- `gvdb_vector_count{collection}` - Current vector count per collection
- `gvdb_collection_count` - Current number of collections

## Dashboard Customization

### Add New Panel

1. Click "Add Panel" → "Add new panel"
2. Write PromQL query
3. Configure visualization
4. Save dashboard

### Example Queries

**Average insert latency:**
```promql
rate(gvdb_insert_duration_seconds_sum[5m]) / rate(gvdb_insert_duration_seconds_count[5m])
```

**Total vectors across all collections:**
```promql
sum(gvdb_vector_count)
```

**Error rate percentage:**
```promql
sum(rate(gvdb_insert_requests_total{status="error"}[5m])) / sum(rate(gvdb_insert_requests_total[5m])) * 100
```

## Alerting (Optional)

Add alerts for critical conditions:

### High Error Rate Alert

```yaml
groups:
  - name: gvdb_alerts
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: |
          sum(rate(gvdb_insert_requests_total{status="error"}[5m])) /
          sum(rate(gvdb_insert_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value | humanizePercentage }}"

      - alert: HighLatency
        expr: |
          histogram_quantile(0.99,
            sum(rate(gvdb_search_duration_seconds_bucket[5m])) by (le)
          ) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High search latency detected"
          description: "p99 latency is {{ $value }}s"
```

## Troubleshooting

### Dashboard Shows No Data

1. **Check GVDB is running**: `curl http://localhost:9090/metrics`
2. **Check Prometheus targets**: http://localhost:9091/targets
   - Should show `gvdb` target as "UP"
3. **Check Prometheus scraping**: http://localhost:9091/graph
   - Query: `gvdb_insert_requests_total`

### Metrics Not Updating

- Ensure GVDB metrics server is running on `:9090`
- Check Prometheus scrape interval (default: 15s)
- Verify firewall allows connections to `:9090`

### High Memory Usage in Prometheus

- Reduce retention period: `--storage.tsdb.retention.time=7d`
- Reduce scrape frequency in `prometheus.yml`

## Production Recommendations

1. **Set proper retention**: 15-30 days for Prometheus data
2. **Enable authentication**: Configure Grafana auth (LDAP, OAuth)
3. **Set up alerting**: Configure Alertmanager for critical alerts
4. **Use remote storage**: For long-term metrics (Thanos, Cortex)
5. **Dashboard permissions**: Read-only access for most users
6. **Regular backups**: Export dashboards periodically

## References

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [PromQL Basics](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Histogram Queries](https://prometheus.io/docs/practices/histograms/)
