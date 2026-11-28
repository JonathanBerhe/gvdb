# GVDB

GVDB is a distributed vector database written in C++. It stores embeddings and finds the vectors that are nearest to a query vector. Applications use it for semantic search, recommendations and retrieval-augmented generation (RAG). Clients connect over gRPC, and the [Python SDK](clients/python) is the supported client.

Documentation: [jonathanberhe.github.io/gvdb](https://jonathanberhe.github.io/gvdb/)

## Features

- Vector indexes: FLAT, HNSW, IVF_FLAT, IVF_PQ, IVF_SQ, TurboQuant and IVF_TURBOQUANT, or `AUTO` to let GVDB choose.
- Hybrid search: dense vectors, sparse vectors and BM25 text in one query, merged with Reciprocal Rank Fusion.
- Metadata filters with an SQL-like syntax, for example `age > 18 AND city = 'NYC'`.
- A time-to-live (TTL) per vector.
- A cluster mode with shards, replicas, failure detection and Raft consensus for the metadata.
- Tiered storage: sealed segments go to S3, GCS or MinIO, and a local cache keeps the hot ones.
- Backup and restore of a collection, also through the Kubernetes operator.
- TLS, API keys and role-based access control per collection.
- GPU search on Apple Metal and, as an option, on NVIDIA CUDA. See [GPU acceleration](docs/features/gpu-acceleration.md).

## Programs

| Program | Role |
|---------|------|
| `gvdb-single-node` | The whole database in one process, for development and small deployments |
| `gvdb-coordinator` | Keeps the cluster metadata, replicated with Raft |
| `gvdb-data-node` | Stores the shards of the collections and builds their indexes |
| `gvdb-query-node` | Sends a search to the data nodes and merges the results |
| `gvdb-proxy` | The entry point of a cluster for clients |

## Quick start

This starts `gvdb-single-node` on `localhost:50051` from a source build. [Build from source](#build-from-source) lists what the build needs.

```bash
make run
```

Install the SDK, then run a first search from Python:

```bash
pip install gvdb
```

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("demo", dimension=3, metric="l2", index_type="flat")
client.insert("demo", [1, 2], [[0.1, 0.2, 0.3], [0.9, 0.8, 0.7]])
for result in client.search("demo", [0.1, 0.2, 0.3], top_k=1):
    print(result.id, result.distance)
```

`make stop` stops the server. The [quickstart](docs/getting-started/quickstart.md) shows the same steps with Docker.

## Deploy

Install a cluster on Kubernetes with Helm:

```bash
helm install gvdb oci://ghcr.io/jonathanberhe/charts/gvdb --namespace gvdb --create-namespace
```

To try the chart on your machine, `make deploy` builds the image, creates a kind cluster and installs the chart. These pages give the details:

- [Deploy with Helm](docs/operations/deploy-helm.md): the chart values and the production settings.
- [Deploy with the operator](docs/operations/deploy-operator.md): the `GVDBCluster`, `GVDBBackup` and `GVDBRestore` resources.
- [Configuration](docs/operations/configuration.md) and the [CLI reference](docs/reference/cli.md): the YAML file, the flags and the environment variables.

## Build from source

You need:

- CMake 3.28 or later and Ninja.
- On Linux: Clang 22 with libc++ from apt.llvm.org. `tools/ci/install-llvm.sh` installs it on Ubuntu or Debian. The `dev` preset also builds with GCC 13.
- On macOS: Apple clang from Xcode, and `libomp` and `zlib` from Homebrew.

CMake downloads the other dependencies. The Makefile calls the presets in `CMakePresets.json`:

```bash
make build           # debug build with tests, in build/dev
make test            # all C++ tests of the dev build
make build-release   # release build, in build/release
```

## Repository layout

| Folder | Content |
|--------|---------|
| `src/` | The C++ source. One folder per module, with each header next to its source file |
| `proto/` | The gRPC API (`vectordb.proto`) and the internal cluster API (`internal.proto`) |
| `test/` | Unit tests per module, integration tests and benchmarks. See [test/README.md](test/README.md) |
| `clients/python/` | The Python SDK |
| `operator/` | The Kubernetes operator, in Go |
| `deploy/` | The Dockerfile, the Helm charts, the kind configuration and the monitoring stack |
| `docs/` | The documentation site |
| `tools/` | Scripts for CI, coverage, protobuf generation and the Modal GPU tests |

## Contributing

Before you open a pull request, run these commands from the repository root:

```bash
make test           # C++ tests
make format         # clang-format
make tidy           # clang-tidy
make check-layers   # module dependency order
make test-sdk       # Python SDK tests, when you change the SDK or the API
```

[docs/contributing.md](docs/contributing.md) describes the code conventions, the module layers and the C++23 features that the code uses. Commit messages follow Conventional Commits, for example `fix(storage): ...`. If you change the API, run `make proto` to regenerate the Python and operator stubs.

## License

Apache License 2.0. See [LICENSE](LICENSE).
