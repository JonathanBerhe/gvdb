# Installation

Choose a deployment option depending on how you plan to use GVDB.

## Docker (single-node)

The fastest way to get a GVDB server running locally.

```bash
docker run -d \
  --name gvdb \
  -p 50051:50051 \
  -v "$PWD/gvdb-data:/var/lib/gvdb" \
  ghcr.io/jonathanberhe/gvdb:latest \
  gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
```

Check the server is up:

```bash
docker logs gvdb
```

The server listens on `localhost:50051`. Connect with the [Python SDK](../python-sdk/index.md) or any gRPC client.

## Helm (Kubernetes)

For distributed production deployments. Installs coordinator, data nodes, query nodes, and proxy as a single release.

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb \
  --namespace gvdb --create-namespace

kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050
```

See [Deploy with Helm](../operations/deploy-helm.md) for the full values reference and [Distributed cluster](distributed-cluster.md) for a walkthrough.

## Local kind cluster

For testing the distributed topology on your laptop:

```bash
git clone https://github.com/JonathanBerhe/gvdb.git
cd gvdb
make deploy   # builds image, creates kind cluster, installs via Helm
make status   # check pods
```

Requires Docker Desktop and [kind](https://kind.sigs.k8s.io/) on `PATH`.

## Build from source

For development or to target a platform without a prebuilt image.

**Prerequisites:**

- C++20 compiler (GCC 11+, Clang 14+)
- CMake 3.15+
- Python 3.8+ (for proto generation)

```bash
git clone https://github.com/JonathanBerhe/gvdb.git
cd gvdb
make build          # Debug build
make build-release  # Optimized build
make test           # Run the C++ test suite
```

Binaries land in `build/bin/`:

```bash
./build/bin/gvdb-single-node --port 50051 --data-dir /tmp/gvdb
```

Optional build flags:

| Flag | Enables |
|------|---------|
| `-DGVDB_WITH_S3=ON` | S3/MinIO tiered storage (requires libssl-dev, libcurl) |
| `-DGVDB_WITH_METAL=ON` | Apple Metal GPU acceleration (macOS only) |

Example:

```bash
make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"
```

## Python SDK

```bash
pip install gvdb                # base client
pip install gvdb[import]        # with Parquet/NumPy/Pandas bulk import
pip install gvdb[import-all]    # everything including h5ad for scRNA-seq
```

See [Python SDK overview](../python-sdk/index.md).

## Java connectors

Published to GitHub Packages. See the [Spark connector](../connectors/spark.md) and [Flink connector](../connectors/flink.md) pages for the Gradle/Maven coordinates and integration examples.

## Next steps

- [Quickstart](quickstart.md) — insert and search your first vectors
- [Distributed cluster](distributed-cluster.md) — multi-node Helm walkthrough
- [Configuration](../operations/configuration.md) — YAML schema and env vars
