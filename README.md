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

```
Client --> Proxy (load balancing)
             |
     +-------+-------+
     |               |
Coordinator      Data Nodes (sharded storage)
  (Raft)              |
                 Query Nodes (distributed search)
```

**5 binaries**: `gvdb-single-node`, `gvdb-coordinator`, `gvdb-data-node`, `gvdb-query-node`, `gvdb-proxy`

## Quick Start

### Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Run (single-node)

```bash
./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb
```

### Run (distributed)

```bash
# Coordinator
./build/bin/gvdb-coordinator --node-id 1 --bind-address 0.0.0.0:50051

# Data node
./build/bin/gvdb-data-node --node-id 101 --bind-address 0.0.0.0:50060 \
  --coordinator localhost:50051

# Proxy
./build/bin/gvdb-proxy --coordinators localhost:50051 \
  --data-nodes localhost:50060
```

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

## Build Requirements

- C++20 compiler (GCC 11+, Clang 14+)
- CMake 3.15+
- Dependencies fetched automatically via CMake FetchContent

## Tests

```bash
ctest --test-dir build --output-on-failure
```

## License

Apache License 2.0 - see [LICENSE](LICENSE)
