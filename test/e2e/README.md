# GVDB Integration Tests (Go)

Comprehensive integration test suite for GVDB written in Go. No external dependencies beyond the Go standard library and gRPC.

## Why Go?

- ✅ **Self-contained**: No Python virtual environments or pip issues
- ✅ **Fast**: Compiled binaries run quickly
- ✅ **Simple**: Easy dependency management with `go mod`
- ✅ **Cross-platform**: Works on macOS, Linux, Windows
- ✅ **Great gRPC support**: First-class protobuf/gRPC integration

## Test Suite Overview

| Test | Description | Duration | Lines of Code |
|------|-------------|----------|---------------|
| **E2E Test** | End-to-end workflow testing (13 steps) | ~30s | ~500 |
| **Load Test** | Concurrent operation testing | ~60s | ~400 |
| **Fuzzy Test** | Edge case and invalid input testing | ~20s | ~450 |
| **Metadata Test** | Metadata filtering & update testing | ~15s | ~400 |

## Quick Start

### Prerequisites

- **Go 1.21+** ([install](https://golang.org/dl/))
- **protoc** (Protocol Buffers compiler)
- **GVDB** built successfully (`build/bin/gvdb-all-in-one`)
- **Port 50051** available

###Run All Tests (Automated)

```bash
# From project root
cd test/integration

# Run everything (setup + build + test)
./run_all_tests.sh
```

This script will:
1. Check dependencies (Go, protoc, server binary)
2. Generate Go protobuf stubs
3. Download Go dependencies
4. Build test binaries
5. Start GVDB server
6. Run all tests
7. Generate summary report
8. Cleanup automatically

## Manual Testing

### Setup (one-time)

```bash
cd test/integration

# Generate protobuf stubs
chmod +x generate_proto.sh
./generate_proto.sh

# Download Go dependencies
go mod download
```

### Build Tests

```bash
# Build all test binaries
go build -o e2e_test e2e_test.go
go build -o load_test load_test.go
go build -o fuzzy_test fuzzy_test.go
```

### Start Server

```bash
# From project root
./build/bin/gvdb-all-in-one --port 50051 --data-dir /tmp/gvdb-test
```

### Run Individual Tests

```bash
# E2E test
./e2e_test

# Load test
./load_test

# Fuzzy test
./fuzzy_test
```

## Test Details

### E2E Test (`e2e_test.go`)

**What it tests:**
1. Server connection and health check
2. Collection creation
3. Vector insertion (1000 vectors, 128D)
4. Vector search (similarity search)
5. Database statistics retrieval
6. Collection listing
7. **Get vectors by ID** (retrieval with partial results)
8. **Update metadata** (merge mode with verification)
9. **Delete vectors** (batch deletion with verification)
10. Multiple search queries (performance)
11. Collection deletion
12. Cleanup verification

**Expected output:**
```
✅ E2E Test PASSED
  - All 13 steps completed
  - Search latency: <50ms avg
  - Get/Delete/Update operations verified
  - All operations functional
```

---

### Load Test (`load_test.go`)

**What it tests:**
- **Concurrent inserts**: 10 threads × 50 ops × 10 vectors/batch = 5000 vectors
- **Concurrent searches**: 10 threads × 50 searches = 500 queries
- **System throughput** under concurrent load
- **Latency distribution** (avg, P50, P95, P99)

**Expected output:**
```
✅ Load Test PASSED
  Insert throughput: >100 ops/sec
  Search throughput: >500 ops/sec
  P95 latency: <100ms
```

**Metrics tracked:**
- Total operations
- Success/failure counts
- Success rate percentage
- Average latency
- Percentile latencies (P50, P95, P99)
- Throughput (operations/second)

---

### Fuzzy Test (`fuzzy_test.go`)

**What it tests:**
1. **Invalid collection names**:
   - Empty strings
   - Whitespace only
   - Very long names (1000 chars)
   - Special characters (null bytes, newlines)
   - Path traversal attempts
   - SQL injection attempts

2. **Invalid dimensions**:
   - Zero dimension
   - Very large dimensions (1M)

3. **Dimension mismatches**:
   - Inserting 64D vector into 128D collection
   - Inserting 256D vector into 128D collection
   - Empty vectors

4. **Special float values**:
   - NaN (Not a Number)
   - Positive/negative infinity
   - All zeros
   - Very large values (1e38)
   - Very small values (1e-38)

5. **Empty operations**:
   - Inserting empty vector list
   - Searching in empty collection

6. **Non-existent collections**:
   - Insert to missing collection
   - Search in missing collection

7. **Large batch inserts**:
   - 1, 10, 100, 1000, 10000 vectors per batch

**Expected output:**
```
✅ Fuzzy Test PASSED
  - Invalid inputs correctly rejected
  - Edge cases handled gracefully
  - System remains stable
  - Success rate: >90%
```

---

## File Structure

```
test/integration/
├── README.md                  # This file
├── go.mod                     # Go module definition
├── generate_proto.sh          # Generate Go protobuf stubs
├── run_all_tests.sh          # Master test runner
├── e2e_test.go               # End-to-end test
├── load_test.go              # Load/stress test
├── fuzzy_test.go             # Fuzzy/edge case test
└── pb/                       # Generated protobuf stubs (auto-created)
    ├── vectordb.pb.go
    └── vectordb_grpc.pb.go
```

---

## Troubleshooting

### Go Not Installed

**Problem**: `go: command not found`

**Solution**:
```bash
# macOS (Homebrew)
brew install go

# Linux (Ubuntu/Debian)
sudo apt-get install golang-go

# Or download from: https://golang.org/dl/
```

### protoc Not Installed

**Problem**: `protoc: command not found`

**Solution**:
```bash
# macOS (Homebrew)
brew install protobuf

# Linux (Ubuntu/Debian)
sudo apt-get install protobuf-compiler

# Or download from: https://github.com/protocolbuffers/protobuf/releases
```

### Server Won't Start

**Problem**: `Cannot connect to server`

**Solutions**:
```bash
# Check if port is in use
lsof -i :50051

# Kill existing server
pkill gvdb-all-in-one

# Verify binary exists
ls -la ../../build/bin/gvdb-all-in-one

# Check server logs
tail -f /tmp/gvdb-integration-test/server.log
```

### Protobuf Generation Fails

**Problem**: `protoc-gen-go: command not found`

**Solution**:
```bash
# Install Go protobuf plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Add to PATH
export PATH="$PATH:$(go env GOPATH)/bin"
```

### Build Failures

**Problem**: `cannot find package`

**Solution**:
```bash
# Clean and re-download dependencies
go clean -modcache
go mod download

# Regenerate protobuf stubs
./generate_proto.sh
```

---

## Performance Expectations

### E2E Test

| Metric | Expected | Excellent |
|--------|----------|-----------|
| Total duration | <60s | <30s |
| Search latency | <50ms | <10ms |
| Insert latency | <100ms | <50ms |

### Load Test

| Metric | Expected | Excellent |
|--------|----------|-----------|
| Insert throughput | >100 ops/s | >500 ops/s |
| Search throughput | >500 ops/s | >2000 ops/s |
| P95 insert latency | <200ms | <100ms |
| P95 search latency | <100ms | <50ms |
| Success rate | >95% | >99% |

### Fuzzy Test

| Metric | Expected |
|--------|----------|
| Invalid input rejection | 100% |
| Special value handling | >90% |
| Large batch handling | >80% |
| Overall stability | 100% |

---

## CI/CD Integration

### GitHub Actions

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Install protoc
        run: |
          sudo apt-get update
          sudo apt-get install -y protobuf-compiler
      - name: Build GVDB
        run: |
          cmake -S . -B build
          cmake --build build --target gvdb-all-in-one -j$(nproc)
      - name: Run Integration Tests
        run: |
          cd test/integration
          ./run_all_tests.sh
```

### Docker

```dockerfile
FROM golang:1.21

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential cmake protobuf-compiler lsof

# Copy project
COPY . /gvdb
WORKDIR /gvdb

# Build GVDB
RUN cmake -S . -B build && \
    cmake --build build --target gvdb-all-in-one

# Run tests
CMD ["test/integration/run_all_tests.sh"]
```

---

## Adding New Tests

### Create New Test File

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    pb "gvdb/integration-tests/pb"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func RunMyTest() bool {
    conn, err := grpc.Dial("localhost:50051",
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Printf("Failed to connect: %v", err)
        return false
    }
    defer conn.Close()

    client := pb.NewVectorDBServiceClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // Your test logic here
    resp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
    if err != nil {
        log.Printf("Test failed: %v", err)
        return false
    }

    fmt.Printf("✅ Test PASSED: %s\n", resp.Message)
    return true
}

func main() {
    success := RunMyTest()
    if !success {
        log.Fatal("Test failed")
    }
}
```

### Build and Run

```bash
# Build
go build -o my_test my_test.go

# Run (server must be running)
./my_test
```

### Add to Test Runner

Edit `run_all_tests.sh` and add your test to the TESTS array:

```bash
TESTS=(
    "E2E Test:e2e_test_test"
    "Load Test:load_test_test"
    "Fuzzy Test:fuzzy_test_test"
    "My Test:my_test_test"  # Add your test here
)
```

Then update the build section:

```bash
build_test "My Test" "my_test.go" || exit 1
```

---

## Best Practices

1. **Always use `run_all_tests.sh`** for pre-commit testing
2. **Run load tests** before merging to main
3. **Add fuzzy tests** for new features with user input
4. **Monitor performance** - track latency trends over time
5. **Clean binaries** before committing (`git clean -fdx`)

---

## Advantages Over Python Tests

| Feature | Go | Python |
|---------|-----|--------|
| Setup time | Instant | Slow (venv, pip install) |
| Dependencies | Built-in | External (grpcio, numpy) |
| Build | Fast compiled binaries | Interpreted |
| Deployment | Single binary | Requires runtime |
| Cross-platform | Excellent | Good |
| Type safety | Compile-time | Runtime |
| Performance | Fast | Slower |

---

## Support

For issues or questions:
- Check server logs: `/tmp/gvdb-integration-test/server.log`
- Run unit tests: `ctest --test-dir build --output-on-failure`
- Verify protoc version: `protoc --version` (need 3.0+)
- Check Go version: `go version` (need 1.21+)
- Review build output for specific error messages

---

**Last Updated**: 2025-11-16
**Test Suite Version**: 2.0.0 (Go)
**Compatible with**: GVDB v1.0.0+
**Language**: Go 1.21+
