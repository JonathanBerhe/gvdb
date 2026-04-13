# Distributed Vector Database - Project Configuration for Claude Code

## Critical Rules - NEVER VIOLATE THESE

### Build System Constraints
- **NEVER** hand-edit generated files in `build/` directory
- **NEVER** modify Makefiles directly - only edit CMakeLists.txt files
- **NEVER** commit binary files or build artifacts
- **NEVER** modify files in `/third_party/` or `/external/` directories
- **ALWAYS** use out-of-source builds: `build/` directory is gitignored
- **ALWAYS** run `cmake -S . -B build` from project root, never from subdirectories

### C++ Safety Requirements
- **NEVER** use raw `new`/`delete` - use smart pointers (`std::unique_ptr`, `std::shared_ptr`)
- **NEVER** use raw pointers for ownership - only for non-owning references
- **NEVER** ignore compiler warnings - treat warnings as errors with `-Wall -Wextra -Werror`
- **ALWAYS** run tests after code changes: `ctest --test-dir build --output-on-failure`
- **ALWAYS** check for memory leaks: `valgrind --leak-check=full ./build/tests/<test_name>`
- **ALWAYS** follow RAII pattern for resource management

### Python SDK / Proto Constraints
- **ALWAYS** after regenerating Python protobuf stubs (`grpc_tools.protoc`), fix the import in `clients/python/gvdb/pb/vectordb_pb2_grpc.py`: change `import vectordb_pb2 as vectordb__pb2` → `from gvdb.pb import vectordb_pb2 as vectordb__pb2`. The generator emits absolute imports but the SDK uses package-relative imports.
- **ALWAYS** run the Python SDK tests after proto changes: `make test-sdk`
- **ALWAYS** use `uv` (not `pip`) for the Python SDK — `uv sync` to install, `uv run` to execute
- **ALWAYS** run `make lint-sdk` before committing Python SDK changes (ruff check + format)
- **NEVER** add pyarrow, numpy, pandas, or anndata to the base `dependencies` — they are optional extras (`pip install gvdb[parquet]`, `gvdb[import-all]`, etc.). Use lazy imports in `client.py` wrappers and `_require()` in `importers.py`.
- **ALWAYS** use `pytest.importorskip()` in tests that depend on optional extras so they skip gracefully

### Module Dependency Rules
- **NEVER** introduce circular dependencies between modules
- **NEVER** include implementation files (.cpp) - only headers
- **NEVER** expose internal implementation details through public headers
- Core module has NO internal dependencies (foundation layer)
- All other modules may depend on core
- See "Module Architecture" section for complete dependency graph

## Build Commands and Environment

### Prerequisites
- **C++ Standard**: C++17 (minimum), C++20 (preferred)
- **Compiler**: GCC 11+, Clang 14+, or MSVC 2019+
- **CMake**: Version 3.15 or higher
- **Python**: 3.8+ (for tooling and code generation)

### Essential Build Commands
```bash
# Build (Debug by default)
make build

# Build with S3/MinIO support (requires libssl-dev, libcurl4-openssl-dev)
make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"

# Build with Apple Metal GPU acceleration (macOS only)
make build CMAKE_EXTRA="-DGVDB_WITH_METAL=ON"

# Run Metal GPU benchmark (macOS only, builds automatically)
make bench-metal

# Build Release
make build-release

# Run all C++ tests
make test

# Run specific test module
./build/test/unit/core_tests
./build/test/unit/index_tests

# Run Go e2e tests against local server
make test-e2e

# Run Go e2e tests against kind cluster
make test-e2e-kind

# Run S3 integration tests (starts MinIO via docker-compose)
make test-s3

# Run Python SDK tests (starts server automatically)
make test-sdk

# Run Python SDK tests against kind cluster
make test-sdk-kind

# Lint Python SDK (ruff check + format)
make lint-sdk

# Regenerate Python protobuf stubs (and fix imports)
make generate-python-stubs

# Clean
make clean

# Docker
make docker-build

# Deploy to kind (build + create cluster + helm install)
make deploy

# Helm
make helm-install
make helm-upgrade
make helm-uninstall

# Generate compile_commands.json for tooling
cmake -S . -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
```

### Build Configurations
- **Debug**: Full debugging symbols, no optimization, assertions enabled
- **Release**: Full optimization (-O3), no debug symbols, assertions disabled
- **RelWithDebInfo**: Optimized with debug symbols for profiling
- **MinSizeRel**: Optimize for binary size

## Project Structure
```
distributed-vector-db/
├── src/
│   ├── core/           # Foundation layer - no dependencies
│   ├── auth/           # RBAC and authentication (depends on: core, utils)
│   ├── consensus/      # Raft consensus (depends on: core)
│   ├── index/          # Vector indexing (depends on: core)
│   ├── storage/        # Persistence (depends on: core, index)
│   ├── compute/        # Query execution (depends on: core, index)
│   ├── cluster/        # Distributed coordination (depends on: all above)
│   ├── network/        # gRPC communication (depends on: core, auth)
│   ├── utils/          # Utilities (depends on: core)
│   └── main/           # Binary entry points
│       ├── coordinator_main.cpp
│       ├── query_node_main.cpp
│       ├── data_node_main.cpp
│       ├── proxy_main.cpp
│       └── single_node_main.cpp
├── include/            # Public headers
├── clients/
│   └── python/         # Python SDK (gvdb package)
│       ├── gvdb/       # SDK source + protobuf stubs
│       │   ├── client.py      # GVDBClient (CRUD, search, import_*)
│       │   ├── importers.py   # Bulk import: Parquet, NumPy, DataFrame, CSV, h5ad
│       │   └── pb/            # Generated protobuf stubs
│       ├── tests/      # pytest integration + RBAC + importer tests
│       └── examples/   # quickstart.py
├── test/
│   ├── unit/           # C++ unit tests (doctest)
│   ├── integration/    # C++ integration tests
│   └── e2e/            # Go end-to-end tests
├── proto/              # Protobuf service definitions
├── deploy/             # Helm charts + k8s manifests
├── cmake/              # CMake modules
├── build/              # Build output (GITIGNORED - DO NOT COMMIT)
└── CMakeLists.txt      # Root build configuration
```

## Module Architecture and Dependencies

### Dependency Hierarchy
```
Layer 0 (Foundation):
  core/ → No dependencies

Layer 1 (Infrastructure):
  utils/ → core
  consensus/ → core
  auth/ → core, utils
  index/ → core
  network/ → core, auth

Layer 2 (Storage/Compute):
  storage/ → core, index
  compute/ → core, index

Layer 3 (Orchestration):
  cluster/ → core, consensus, storage, compute, network
```

### Module Interfaces
- Each module exposes public API through headers in `include/<module>/`
- Internal implementation details in `src/<module>/detail/`
- When working on a module, only include public headers from dependencies
- Use forward declarations where possible to reduce compilation time

### Core Module (Foundation Layer)
- **Purpose**: Fundamental types, interfaces, and abstractions
- **Public API**: 
  - `core/types.h` - System-wide types (VectorId, CollectionId, SegmentId, ShardId)
  - `core/status.h` - Error handling using absl::Status and StatusOr<T>
  - `core/vector.h` - Vector data structures with SIMD alignment
- **Key Interfaces**:
  - `IVectorIndex` - Abstract interface for all index implementations
  - `IStorage` - Abstract interface for storage backends
- **No external dependencies except abseil**

### Index Module
- **Purpose**: Vector index implementations wrapping Faiss
- **Implements**: `core::IVectorIndex` interface
- **Index Types**: FLAT, HNSW, IVF, IVF_PQ, IVF_SQ
- **Key Classes**:
  - `IndexFactory` - Creates appropriate index based on configuration. Transparently returns `MetalFlatIndex` when Metal GPU is available and index type is FLAT.
  - `IndexManager` - Manages index lifecycle and memory
- **SIMD Optimization**: Runtime CPU capability detection (SSE, AVX2, AVX512)
- **Metal GPU** (`-DGVDB_WITH_METAL=ON`, macOS only): `MetalFlatIndex` in `src/index/metal/` — custom MSL distance kernels dispatched via metal-cpp. ObjC++ (`.mm`) strictly isolated to `src/index/metal/`. 16-24x speedup over CPU on Apple Silicon.

### Storage Module
- **Purpose**: Data persistence and segment management
- **Key Interfaces**:
  - `ISegmentStore` - Abstract interface for all segment storage backends (22 methods). All consumers use this.
  - `IObjectStore` - Abstract interface for object storage backends (S3, MinIO, GCS, Azure)
- **Key Classes**:
  - `SegmentManager` implements `ISegmentStore` — local disk only
  - `TieredSegmentManager` implements `ISegmentStore` — composes SegmentManager + IObjectStore + SegmentCache. Used when S3/MinIO is configured.
  - `S3ObjectStore` implements `IObjectStore` — AWS SDK (behind `-DGVDB_WITH_S3=ON`)
  - `SegmentCache` - LRU disk cache for segments downloaded from S3
  - `SegmentManifest` - JSON manifest tracking segments in object storage
- **Key Components**:
  - Segment management (GROWING → SEALING → SEALED → FLUSHED states)
  - Tiered storage: local disk (hot) → S3/MinIO (cold) with async upload and lazy download
  - Scalar metadata indexes (bitmap + sorted numeric)

### Auth Module
- **Purpose**: RBAC and authentication
- **Key Components**:
  - `RbacStore` - Thread-safe API key → role mapping (validated via `Create()` factory)
  - `AuthContext` - Thread-local storage for current request's API key
  - `HasPermission` / `HasCollectionAccess` - Permission matrix checks
- **Roles**: admin, readwrite, readonly, collection_admin
- **Wired in**: gRPC interceptor (`AuthProcessor`) sets thread-local, service methods call `CheckPermission`
- **Audit logging**: `AuditInterceptor` (separate from auth) emits structured JSON per RPC. `AuditContext` thread-local enriched by service handlers (collection, item_count). API key read directly from gRPC metadata. Config: `logging.audit.enabled`. Files: `include/network/audit_interceptor.h`, `src/network/audit_interceptor.cpp`, `include/network/audit_context.h`, `src/network/audit_context.cpp`, `include/utils/audit_logger.h`, `src/utils/audit_logger.cpp`

### Consensus Module
- **Purpose**: Raft consensus for metadata operations
- **Uses**: NuRaft (eBay's Raft implementation)
- **Persistent storage**: RocksDB for Raft log entries
- **Responsibilities**:
  - Leader election
  - Metadata state machine
  - Timestamp oracle (TSO)

## Code Style and Patterns

### Naming Conventions
```cpp
class PascalCase {};           // Classes and structs
void camelCase();              // Functions and methods
int snake_case;                // Variables and parameters
int m_memberVariable;          // Private member variables (alternative: memberVariable_)
constexpr int kConstantValue; // Compile-time constants
enum class EnumClass {};       // Enum classes (not plain enums)
```

### Header Organization
```cpp
// Order of includes (separated by blank lines)
// 1. Corresponding header (for .cpp files)
#include "module/file.h"

// 2. Project headers
#include "core/types.h"
#include "utils/logging.h"

// 3. Third-party headers
#include <grpcpp/grpcpp.h>
#include "absl/status/status.h"

// 4. Standard library headers
#include <memory>
#include <vector>
```

### RAII and Smart Pointers
```cpp
// Always use smart pointers for ownership
std::unique_ptr<Index> index = std::make_unique<HNSWIndex>();
std::shared_ptr<Resource> shared = std::make_shared<Resource>();

// Raw pointers only for non-owning references
void ProcessIndex(const Index* index);  // Observer, doesn't own

// RAII for all resources
class SegmentReader {
    std::unique_ptr<MmapFile> file_;     // Automatic cleanup
    std::shared_mutex mutex_;            // Automatic unlock
public:
    ~SegmentReader() = default;          // Destructor handles cleanup
};
```

### Error Handling
```cpp
// Use absl::Status for operations that can fail
absl::Status LoadSegment(SegmentId id);

// Use absl::StatusOr<T> for operations that return values
absl::StatusOr<std::unique_ptr<Index>> CreateIndex(const Config& config);

// Check status before using value
auto result = CreateIndex(config);
if (!result.ok()) {
    LOG(ERROR) << "Failed to create index: " << result.status();
    return result.status();
}
auto index = std::move(result.value());
```

## Dependencies and Libraries

### Core Dependencies (via CMake FetchContent)
- **gRPC**: Network communication and service definitions
- **Protobuf**: Message serialization
- **abseil-cpp**: Status, StatusOr, flags, containers (flat_hash_map), strings
- **faiss**: Vector indexing algorithms
- **NuRaft**: Raft consensus implementation
- **RocksDB**: Persistent storage for Raft log
- **spdlog**: Structured logging
- **yaml-cpp**: Configuration file parsing
- **prometheus-cpp**: Metrics collection

### Testing Dependencies
- **doctest**: Lightweight C++ testing framework (v2.4.11)

### Important: No Boost Dependency
We use abseil-cpp for everything Boost would provide:
- `absl::Status` instead of `boost::outcome`
- `absl::flat_hash_map` instead of `boost::unordered_map`
- `absl::flags` instead of `boost::program_options`

## Testing Requirements

### Test Organization
- Unit tests in `test/unit/<module>_test.cpp`
- Integration tests in `test/integration/`
- Each module has corresponding test file
- Use doctest framework with CHECK_* and REQUIRE_* macros

### Test Execution Workflow
1. **Write tests first** (TDD approach)
2. **Verify tests fail**: `./build/tests/<module>_tests` should fail initially
3. **Implement until tests pass**
4. **Check memory safety**: `valgrind --leak-check=full ./build/tests/<module>_tests`
5. **Run all tests**: `ctest --test-dir build --output-on-failure`

### Test Coverage Requirements
- Minimum 80% line coverage for new code
- 100% coverage for public API functions
- Edge cases and error paths must be tested
- Use parameterized tests for multiple input scenarios

### Memory Safety Validation
```bash
# Run with AddressSanitizer (compile-time flag)
cmake -S . -B build -DCMAKE_CXX_FLAGS="-fsanitize=address"
./build/tests/module_tests

# Run with Valgrind (runtime check)
valgrind --leak-check=full --show-leak-kinds=all ./build/tests/module_tests
```

### Go End-to-End Tests

Go e2e tests are in `test/e2e/` and test the complete system against a running server. The test runner (`run_all_tests.sh`) starts a server automatically.

**Running E2E Tests**:
```bash
# Against local server (auto-started)
make test-e2e

# Against kind cluster (requires port-forward to proxy)
make test-e2e-kind
```

**Available Tests** (8 total, run by `run_all_tests.sh`):
- `E2E` - Complete workflow: create collection, insert, search, get, update, delete
- `Metadata` - Metadata filtering with SQL-like expressions
- `Load` - 100K+ vectors, concurrent queries, performance benchmarks
- `Fuzzy` - Edge cases, invalid inputs, error handling
- `Sparse_Search` - Sparse vector search
- `TTL` - Time-to-live expiration
- `Auto_Index` - Automatic index type selection
- `RBAC` - Role-based access control (starts its own auth-enabled server)

### Python SDK Tests

Python SDK tests are in `clients/python/tests/` and use pytest. The test fixtures start a server automatically.

**Running SDK Tests**:
```bash
# Against local server (auto-started)
make test-sdk

# Against kind cluster
make test-sdk-kind

# Lint only
make lint-sdk
```

**Test Files**:
- `test_sdk.py` - 16 integration tests covering all SDK methods
- `test_rbac.py` - 21 RBAC tests (starts its own auth-enabled server)
- `test_importers.py` - 31 unit tests for bulk importers (mocked client, no server)
- `test_import_integration.py` - 7 integration tests for bulk importers (real server)

### Local Testing Workflow

All `make test-*` commands must be run from the **project root** (`/gvdb`), not from subdirectories.

#### Local server tests (fastest feedback loop)
```bash
# C++ unit tests — no server needed
make test

# Python SDK tests — auto-starts a local single-node server
make test-sdk

# Go e2e tests — auto-starts a local server
make test-e2e
```

These are self-contained: they build/start a server, run tests, and tear down. Use these first.

#### Kind cluster tests

Kind tests run against a full distributed topology (coordinator + data nodes + query node + proxy). They require more setup and are sensitive to Docker state.

**Prerequisites**: Docker Desktop must be running before any kind commands.

**First-time setup**:
```bash
# Builds image, creates kind cluster, loads image, helm installs
make deploy
```

**Running tests**:
```bash
# 1. Verify pods are ready
kubectl get pods -n gvdb

# 2. Ensure port-forward is active (do NOT kill existing processes on port 50050)
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050 &

# 3. Run tests
make test-sdk-kind
make test-e2e-kind
```

**After code changes** (rebuild + redeploy):
```bash
# Rebuild Docker image and load into kind
make docker-build
kind load docker-image gvdb:latest --name gvdb

# Restart pods to pick up new image
kubectl rollout restart statefulset -n gvdb
kubectl rollout restart deployment -n gvdb
kubectl wait --for=condition=ready pod --all -n gvdb --timeout=120s

# Re-establish port-forward (previous one dies during rollout)
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050 &

# Run tests
make test-sdk-kind
```

**After Docker Desktop restart** (pods go Unknown):
```bash
# Delete stale pods so controllers recreate them
kubectl delete pods -n gvdb --all --force --grace-period=0

# Wait for fresh pods
kubectl wait --for=condition=ready pod --all -n gvdb --timeout=120s

# Re-establish port-forward
kubectl port-forward -n gvdb svc/gvdb-proxy 50050:50050 &
```

**Common pitfalls**:
- `make deploy` fails with "release name check failed" → release already exists, use `make helm-upgrade` instead
- All tests fail with "Server not ready" → port-forward is dead, re-establish it
- Writes fail with "Segment not found" → pods are stale from a Docker restart, delete and recreate them
- `kubectl` commands fail with "connection refused" → Docker Desktop is not running or still starting up
- **NEVER** blindly kill processes on port 50050 — the user may have an active port-forward or other services; just start a new port-forward

## Performance Considerations

### Compilation Time
- Core module uses heavy templates: ~30 seconds full rebuild
- Incremental builds should be <5 seconds
- Use forward declarations to minimize header dependencies
- Prefer Pimpl idiom for frequently-changed classes

### SIMD Optimizations
- The index module handles all SIMD optimizations
- Runtime CPU detection selects optimal implementation
- Never write SIMD intrinsics directly - use index module abstractions

### Memory Management
- Pre-allocate memory pools for hot paths
- Use memory-mapped files for large datasets
- Implement custom allocators where standard allocation is a bottleneck
- Profile with gperftools for memory hotspots

## Git Workflow

### Branch Protection
- `main` is protected: **all changes go through PRs**
- PRs must pass the `build-and-test` CI check before merging
- No direct pushes, no force pushes, no branch deletion
- **ALWAYS** create a feature branch and open a PR — never commit directly to main

### Branch Strategy
- `main`: Stable, production-ready code (protected)
- `feature/*`: Individual feature branches
- `fix/*`: Bug fix branches
- `chore/*`: Non-functional changes (docs, CI, config)

### PR Workflow
1. Create branch: `git checkout -b feature/your-feature`
2. Make changes and commit with conventional commit messages
3. Push and create PR: `gh pr create`
4. CI runs `make build && make test` (must pass)
5. Review and merge

### Commit Message Format

**User Requirements**:
- **Subject line only** - no body or description
- **No Co-Authored-By footer** - no co-sign lines
- **Exclude *.md files** - except README.md (no docs/, no .md documentation files)
- Keep commits focused and concise

**Format** (conventional commits — used by release-please for semver):
```
<type>(<scope>): <subject>
```

**Types**: feat, fix, docs, style, refactor, test, chore
**Scope**: core, index, storage, cluster, network, deploy, ci, etc.

**Version impact** (release-please reads these):
- `feat` → minor version bump (0.1.0 → 0.2.0)
- `fix` → patch version bump (0.1.0 → 0.1.1)
- `feat!` or `fix!` (bang suffix) → major version bump (0.1.0 → 1.0.0)

**Examples**:
```
feat(cluster): add NodeRegistry with heartbeat protocol
fix(storage): correct segment state transitions
refactor(network): simplify gRPC service initialization
```

### Release Process
1. Merge PRs with conventional commits to `main`
2. release-please automatically opens a Release PR with version bump + changelog
3. Merge the Release PR → git tag created → Docker image + Helm chart published
4. Users upgrade: `helm upgrade gvdb oci://ghcr.io/jonathanberhe/charts/gvdb`

### Pre-commit Checks
1. Code compiles without warnings
2. All tests pass (`make test`)
3. clang-format applied
4. No memory leaks (Valgrind clean)

## Common Pitfalls and Solutions

### Faiss Integration
- Faiss indexes are not thread-safe for writes - use mutex protection
- Some Faiss operations require specific memory alignment - use aligned allocators
- GPU indexes require different initialization - check CUDA availability first

### Template Instantiation
- Explicit instantiation in .cpp files for common types reduces compilation time
- Template errors are verbose - focus on "required from here" in error messages
- Use concepts (C++20) or SFINAE for better error messages

### Distributed System Issues
- Network operations can fail - always handle gRPC status codes
- Clock skew affects consensus - use TSO for total ordering
- Partial failures are common - implement proper retry logic with backoff

## Development Workflow Commands

### Starting New Feature
```bash
git checkout -b feature/your-feature-name main
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
```

### Running Specific Module Tests
```bash
# Build and run only index module tests
cmake --build build --target index_tests
./build/tests/index_tests -tc="*IndexFactory*"
```

### Profiling Performance
```bash
# CPU profiling with perf
perf record -g ./build/bin/vectordb-query
perf report

# Memory profiling with gperftools
CPUPROFILE=cpu.prof ./build/bin/vectordb-query
google-pprof --pdf ./build/bin/vectordb-query cpu.prof > profile.pdf
```

### Debugging
```bash
# Run with gdb
gdb ./build/bin/vectordb-coordinator
(gdb) run --node-id 1 --bind-address localhost:50051

# Run with AddressSanitizer
ASAN_OPTIONS=symbolize=1 ./build/bin/vectordb-query
```

## Module-Specific Guidance

When working on specific modules, refer to their CLAUDE.md files:
- `src/core/CLAUDE.md` - Core module patterns and type system (foundation layer)
- `src/auth/CLAUDE.md` - RBAC store, permission matrix, auth context
- `src/consensus/CLAUDE.md` - NuRaft integration, persistent log store, timestamp oracle
- `src/index/CLAUDE.md` - Faiss integration and SIMD optimization
- `src/storage/CLAUDE.md` - Segment lifecycle and LSM implementation
- `src/compute/CLAUDE.md` - Query execution and result merging
- `src/cluster/CLAUDE.md` - Distributed coordination patterns
- `src/network/CLAUDE.md` - gRPC service definitions
- `src/utils/CLAUDE.md` - Thread pool, logging, and utilities

## Future Requirements and Enhancements

### Web Management UI (Similar to Attu for Milvus)

**Status**: Not yet implemented (pending Network module completion)
**Priority**: High for production deployments and proper system investigation
**Reference**: https://github.com/zilliztech/attu (Attu - Web UI for Milvus)

We need a **lightweight web-based management interface** for GVDB to properly investigate and manage the system. This should be simpler than Attu but provide essential functionality:

**Core Features Needed**:

1. **Collection Browser**
   - List all collections with metadata (dimension, metric, vector count)
   - View collection details (segments, indexes, storage size)
   - Create/drop collections
   - Collection health status

2. **Vector Data Explorer**
   - Browse vectors in a collection (paginated)
   - View vector IDs and metadata
   - Insert test vectors
   - Delete vectors

3. **Search Testing**
   - Execute similarity searches via UI
   - Visualize search results with distances
   - Test different top-k values
   - Compare results across index types

4. **Cluster Monitor**
   - Node status (alive/dead)
   - Segment distribution map
   - Memory usage per node
   - Real-time metrics (QPS, latency)

5. **System Health**
   - Query performance graphs (p50, p95, p99)
   - Slow query log
   - Index build progress
   - Storage usage trends

**Technology Stack**:
- **Frontend**: React or Vue.js (lightweight SPA)
- **Backend**: REST API over gRPC (thin translation layer)
- **Real-time**: WebSockets for metrics streaming
- **Metrics**: Prometheus endpoint (already have prometheus-cpp)
- **Optional**: Grafana integration for advanced monitoring

**Implementation Phases**:
- **Phase 1**: Basic REST API endpoints (after Network module)
- **Phase 2**: Simple React UI for collections and search
- **Phase 3**: Real-time monitoring dashboard
- **Phase 4**: Advanced features (query profiling, tracing)

**Why This is Important**:
- Command-line tools are insufficient for investigating performance issues
- Web UI allows non-technical users to interact with the system
- Visual feedback helps debug distributed system behavior
- Essential for production deployments and demos

**Note**: This will be implemented as a separate module/service after Network module is complete.

---

## Roadmap

See `ROADMAP.md` in the project root for the full feature roadmap, implementation order, and architecture changes. Update it when completing or planning features.

---

## Questions or Uncertainties

If you encounter situations not covered here:
1. First, check module-specific CLAUDE.md files
2. Look for existing patterns in similar code
3. Ask for clarification before implementing
4. State explicitly: "I don't have enough information about X"

Remember: It's better to ask for clarification than to make incorrect assumptions that could introduce bugs or architectural violations.
