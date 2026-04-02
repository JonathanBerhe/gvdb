# GVDB - Distributed Vector Database

A high-performance distributed vector database written in C++ for similarity search at scale.

Store, index, and search high-dimensional vectors (embeddings from OpenAI, Cohere, HuggingFace, etc.) with sub-millisecond latency. Use it to power semantic search, recommendation engines, RAG pipelines, image retrieval, and anomaly detection.

## Features

- **Vector Search**: FLAT, HNSW, IVF_FLAT, IVF_PQ, IVF_SQ index types via Faiss
- **Distributed Mode**: Coordinator, data nodes, query nodes, proxy with full sharding and replication
- **Multi-Shard Collections**: Data distributed across nodes with consistent hashing (150 virtual nodes)
- **Fault Tolerance**: Automatic failure detection, replica promotion, auto-replication
- **Metadata Filtering**: SQL-like filters (`age > 18 AND city = 'NYC'`, `LIKE`, `IN`)
- **Persistence**: Vectors flushed to disk, index rebuilt on startup recovery
- **gRPC API**: Protobuf-based client/server with TLS and API key authentication
- **Raft Consensus**: Metadata operations replicated via NuRaft

## Architecture

```mermaid
graph TB
    Client([Client])

    subgraph Proxy Layer
        Proxy[gvdb-proxy<br/>Load Balancing]
    end

    subgraph Control Plane
        C1[gvdb-coordinator]
        C2[gvdb-coordinator]
        C3[gvdb-coordinator]
        C1 <--> C2
        C2 <--> C3
        C1 <--> C3
    end

    subgraph Data Plane
        DN1[gvdb-data-node<br/>Shards 1-4]
        DN2[gvdb-data-node<br/>Shards 5-8]
    end

    subgraph Query Plane
        QN1[gvdb-query-node]
        QN2[gvdb-query-node]
    end

    Client --> Proxy
    Proxy -- "metadata ops" --> C1
    Proxy -- "search" --> QN1 & QN2
    Proxy -- "insert/get/delete" --> DN1 & DN2
    QN1 & QN2 -- "ExecuteShardQuery" --> DN1 & DN2
    DN1 & DN2 -- "heartbeat" --> C1
    QN1 & QN2 -- "heartbeat" --> C1
    C1 -- "CreateSegment<br/>ReplicateSegment" --> DN1 & DN2

    style Proxy fill:#4a9eff,color:#fff
    style C1 fill:#ff6b6b,color:#fff
    style C2 fill:#ff6b6b,color:#fff
    style C3 fill:#ff6b6b,color:#fff
    style DN1 fill:#51cf66,color:#fff
    style DN2 fill:#51cf66,color:#fff
    style QN1 fill:#ffd43b,color:#333
    style QN2 fill:#ffd43b,color:#333
```

| Binary | Role |
|--------|------|
| `gvdb-single-node` | All-in-one for development and small deployments |
| `gvdb-coordinator` | Cluster metadata via Raft consensus |
| `gvdb-data-node` | Sharded vector storage and indexing |
| `gvdb-query-node` | Distributed search with fan-out and result merging |
| `gvdb-proxy` | Client entry point with load balancing |

## Quick Start

### Deploy on Kubernetes (Helm)

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace

# Scale data nodes
helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --set dataNode.replicas=5

# Connect
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050
```

### Deploy locally with Kind

```bash
make deploy   # builds image, creates kind cluster, installs via Helm
make status   # check pods
```

### Build from source

```bash
make build          # Debug build
make build-release  # Release build
make test           # Run all C++ tests (37 suites)
```

### Run (single-node)

```bash
./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb
```

### Run (distributed, bare metal)

```bash
# Coordinator
./build/bin/gvdb-coordinator --node-id 1 --bind-address 0.0.0.0:50051

# Data node (use --advertise-address in containers)
./build/bin/gvdb-data-node --node-id 101 --bind-address 0.0.0.0:50060 \
  --coordinator localhost:50051

# Proxy
./build/bin/gvdb-proxy --coordinators localhost:50051 \
  --data-nodes localhost:50060
```

## Helm Chart Configuration

All values are configurable via `--set` or a custom `values.yaml`:

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --set dataNode.replicas=3 \
  --set queryNode.replicas=2 \
  --set proxy.service.type=LoadBalancer \
  --set image.tag=v1.2.0
```

Key values:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dataNode.replicas` | `2` | Number of data nodes |
| `queryNode.replicas` | `1` | Number of query nodes |
| `proxy.service.type` | `ClusterIP` | `ClusterIP`, `NodePort`, or `LoadBalancer` |
| `image.repository` | `gvdb` | Container image |
| `image.tag` | `latest` | Image tag |
| `dataNode.storage.size` | `5Gi` | PVC size per data node |
| `dataNode.memoryLimitGb` | `4` | Memory limit for vector storage |

See [`deploy/helm/gvdb/values.yaml`](deploy/helm/gvdb/values.yaml) for all options.

## Configuration

```yaml
server:
  grpc_port: 50051
  tls:
    enabled: true
    cert_path: /etc/gvdb/server.crt
    key_path: /etc/gvdb/server.key
  auth:
    enabled: true
    api_keys:
      - "your-api-key"

storage:
  data_dir: /var/lib/gvdb

logging:
  level: info
```

All binaries support environment variables (`GVDB_BIND_ADDRESS`, `GVDB_ADVERTISE_ADDRESS`, `GVDB_DATA_DIR`) for cloud-native deployments.

## Build Requirements

- C++20 compiler (GCC 11+, Clang 14+)
- CMake 3.15+
- Dependencies fetched automatically via CMake FetchContent

## Tests

```bash
make test             # C++ unit + integration tests (37 suites)
make test-e2e         # Go e2e tests against local server
make test-e2e-kind    # Go e2e tests against kind cluster
```

See [test/README.md](test/README.md) for details on the test structure.

## License

Apache License 2.0 - see [LICENSE](LICENSE)
