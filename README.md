# GVDB - Distributed Vector Database

A high-performance distributed vector database built with C++20, designed for similarity search at scale.

## Features

- **Multiple Index Types**: FLAT, HNSW, IVF-based indexes powered by Faiss
- **Distributed Architecture**: Coordinator, Query Node, Data Node, and Proxy components
- **Strong Consistency**: Raft consensus for metadata operations
- **SIMD Optimized**: Runtime CPU detection for optimal performance (SSE, AVX2, AVX512)
- **Production Ready**: Comprehensive testing, memory safety validation, type-safe APIs

## Prerequisites

### System Requirements
- **C++ Compiler**: GCC 11+, Clang 14+, or MSVC 2019+
- **CMake**: Version 3.15 or higher
- **Git**: For fetching dependencies
- **Internet connection**: For first build (downloads dependencies)

### Dependencies

**Most dependencies are automatically fetched!** The build system uses CMake FetchContent:
- **Abseil C++** (v20240116.2): Status handling, strings, containers - *auto-fetched*
- **Google Test** (v1.14.0): Unit testing framework - *auto-fetched*
- **Faiss** (v1.8.0): Vector similarity search library - *auto-fetched*
- **OpenMP**: Parallel processing (required by Faiss) - **needs manual install**

#### Installing OpenMP

**macOS:**
```bash
brew install libomp
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libomp-dev
```

**Container (Dockerfile):**
```dockerfile
RUN apt-get update && apt-get install -y libomp-dev
```

#### Optional: Use System Dependencies

If you prefer to use system-installed dependencies:

```bash
# Ubuntu/Debian
sudo apt-get install libabsl-dev libgtest-dev

# macOS
brew install abseil googletest

# Then build with system dependencies
cmake -S . -B build -DUSE_SYSTEM_DEPENDENCIES=ON
```

## Building

### Quick Start

```bash
# Install OpenMP first (macOS)
brew install libomp

# Configure with OpenMP
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DOpenMP_C_FLAGS="-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include" \
  -DOpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include" \
  -DOpenMP_omp_LIBRARY="/opt/homebrew/opt/libomp/lib/libomp.dylib"

# Build
cmake --build build -j8

# Run tests
ctest --test-dir build --output-on-failure
```

**Note**: On Linux, OpenMP detection usually works automatically without extra flags.

### Development Build

For development with full debugging symbols and assertions:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(nproc)
```

### Build Configurations

- **Debug**: Full debugging symbols, no optimization, assertions enabled
- **Release**: Full optimization (-O3), no debug symbols, assertions disabled
- **RelWithDebInfo**: Optimized with debug symbols for profiling
- **MinSizeRel**: Optimize for binary size

## Running Tests

### All Tests
```bash
ctest --test-dir build --output-on-failure
```

### Specific Module Tests
```bash
./build/tests/core_tests
```

### With Valgrind (Memory Leak Detection)
```bash
valgrind --leak-check=full --show-leak-kinds=all ./build/tests/core_tests
```

### With AddressSanitizer
```bash
cmake -S . -B build -DCMAKE_CXX_FLAGS="-fsanitize=address"
cmake --build build
./build/tests/core_tests
```

## Project Structure

```
gvdb/
├── include/          # Public headers
│   └── core/         # Core module headers
├── src/              # Implementation
│   ├── core/         # Foundation layer (no dependencies)
│   ├── consensus/    # Raft consensus
│   ├── index/        # Vector indexing (Faiss wrapper)
│   ├── storage/      # Persistence layer
│   ├── compute/      # Query execution
│   ├── cluster/      # Distributed coordination
│   └── network/      # gRPC services
├── test/             # Tests
│   └── unit/         # Unit tests
├── cmake/            # CMake modules
└── build/            # Build output (gitignored)
```

## Architecture

### Module Dependency Hierarchy

```
Layer 0 (Foundation):
  core/ → No dependencies

Layer 1 (Infrastructure):
  utils/ → core
  consensus/ → core
  index/ → core
  network/ → core

Layer 2 (Storage/Compute):
  storage/ → core, index
  compute/ → core, index

Layer 3 (Orchestration):
  cluster/ → core, consensus, storage, compute, network
```

### Core Module

The core module is the foundation layer that provides:
- **Strong-typed IDs**: VectorId, CollectionId, SegmentId, etc.
- **Error Handling**: Status and StatusOr<T> based on abseil
- **Vector Operations**: SIMD-aligned vector class with distance calculations
- **Interfaces**: IVectorIndex, IStorage for polymorphic implementations
- **Configuration**: Comprehensive config structures for all components

## Development Workflow

### Code Style

- Follow the C++ Core Guidelines
- Use clang-format for formatting
- All code must pass `-Wall -Wextra -Werror`
- Use smart pointers (never raw new/delete)
- RAII for all resource management

### Testing Requirements

- Minimum 80% line coverage for new code
- 100% coverage for public API functions
- All tests must pass before committing
- Memory leak checks with Valgrind

### Making Changes

1. Create a feature branch
2. Implement changes following CLAUDE.md guidelines
3. Write/update tests
4. Ensure all tests pass
5. Check for memory leaks
6. Submit pull request

## Documentation

- Root CLAUDE.md: Overall project guidelines
- Module-specific CLAUDE.md files in each src/ subdirectory
- API documentation in header files

## Current Status

**Phase**: Core Infrastructure Complete ✅

### Completed Modules

- [x] **Core module** - Foundation layer
  - [x] Type system with strong typing
  - [x] Status/StatusOr error handling
  - [x] SIMD-aligned Vector class
  - [x] Pure virtual interfaces (IVectorIndex, IStorage)
  - [x] Comprehensive configuration structures
  - [x] 38/38 tests passing

- [x] **Index module** - Vector indexing with Faiss
  - [x] FLAT index (brute-force exact search)
  - [x] HNSW index (fast approximate search)
  - [x] IVF index (memory-efficient with PQ/SQ)
  - [x] Thread-safe wrapper with OpenMP parallelization
  - [x] IndexFactory and IndexManager
  - [x] 20/20 tests passing

- [x] **Storage module** - Persistence and segment management
  - [x] Segment lifecycle (GROWING → SEALED → FLUSHED)
  - [x] SegmentManager for multi-segment coordination
  - [x] LocalStorage with metadata support
  - [x] StorageFactory pattern
  - [x] Compaction strategy (threshold-based)
  - [x] Thread-safe operations
  - [x] 19/19 tests passing

### Next Priority
- [ ] Utils module - Logging and threading utilities
- [ ] Network module - gRPC services
- [ ] Consensus module - Raft for metadata
- [ ] Compute module - Query execution engine
- [ ] Cluster module - Distributed coordination
- [ ] Integration tests

## License

[To be determined]

## Contributing

Contributions are welcome! Please read CLAUDE.md for detailed development guidelines.
