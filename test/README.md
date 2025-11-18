# GVDB Test Suite

Comprehensive testing strategy for GVDB with three distinct test layers.

## Test Organization

```
test/
├── unit/           # Unit tests - single component testing
├── integration/    # Integration tests - multi-component testing (C++)
└── e2e/            # End-to-end tests - full system testing (Go via gRPC)
```

## Test Types

### 1. Unit Tests (`test/unit/`)

**Purpose**: Test individual components in isolation

**Characteristics**:
- Fast execution (<1s per test)
- No external dependencies
- Mock/stub external components
- High code coverage (80%+ target)

**Example**: Testing `GvdbLogStore::append()` without Raft server

**Technology**: C++ with Google Test

**Run**:
```bash
ctest --test-dir build -R Tests$
./build/test/unit/core_tests
./build/test/unit/consensus_tests
```

---

### 2. Integration Tests (`test/integration/`)

**Purpose**: Test interactions between multiple GVDB components

**Characteristics**:
- Medium execution time (1-10s per test)
- Tests component integration in-process
- No external services (no network, no containers)
- Verifies interfaces between modules

**Example**: Raft cluster with GvdbLogStore + GvdbStateManager + persistence

**Technology**: C++ with Google Test

**Run**:
```bash
ctest --test-dir build -R Integration
./build/test/integration/consensus_integration_tests
```

---

### 3. End-to-End Tests (`test/e2e/`)

**Purpose**: Black-box testing of the full GVDB system

**Characteristics**:
- Slow execution (30-60s per test suite)
- Tests via public gRPC API only
- Requires running GVDB server
- Simulates real client usage

**Example**: Go client → gRPC API → GVDB server → verify results

**Technology**: Go with gRPC client

**Run**:
```bash
cd test/e2e
./run_all_tests.sh
```

---

## Comparison Matrix

| Aspect | Unit | Integration | E2E |
|--------|------|-------------|-----|
| **Scope** | Single component | Multiple components | Full system |
| **Speed** | <1s | 1-10s | 30-60s |
| **Language** | C++ | C++ | Go |
| **Dependencies** | None | In-process only | External server |
| **Isolation** | High | Medium | Low |
| **Coverage** | High (80%+) | Medium (60%+) | Low (critical paths) |
| **When to run** | Every commit | Before merge | Before release |

---

## Testing Workflow

### Development (Fast Feedback)
```bash
# Run only affected unit tests
ctest --test-dir build -R CoreTests
```

### Pre-Commit (Medium Confidence)
```bash
# Run all unit tests
ctest --test-dir build --output-on-failure
```

### Pre-Merge (High Confidence)
```bash
# Run unit + integration tests
ctest --test-dir build --output-on-failure
ctest --test-dir build -R Integration
```

### Pre-Release (Full Validation)
```bash
# Run everything
ctest --test-dir build --output-on-failure
cd test/e2e && ./run_all_tests.sh
```

---

## Test Coverage by Module

| Module | Unit Tests | Integration Tests | E2E Tests |
|--------|------------|-------------------|-----------|
| Core | ✅ core_tests | - | ✅ All e2e tests use core |
| Index | ✅ index_tests | - | ✅ Search operations |
| Storage | ✅ storage_tests | - | ✅ Insert/Delete operations |
| Consensus | ✅ consensus_tests<br>✅ consensus_persistence_tests | ✅ consensus_integration_tests | - |
| Cluster | ✅ cluster_tests | - | - |
| Compute | ✅ compute_tests | - | ✅ Search operations |
| Network | ✅ network_tests | - | ✅ All gRPC operations |
| Utils | ✅ utils_tests | - | - |

---

## Adding New Tests

### Unit Test
```bash
# 1. Create test file
touch test/unit/mymodule_test.cpp

# 2. Add to test/unit/CMakeLists.txt
add_executable(mymodule_tests mymodule_test.cpp)
target_link_libraries(mymodule_tests PRIVATE gvdb_mymodule GTest::gtest GTest::gtest_main)
add_test(NAME MyModuleTests COMMAND mymodule_tests)

# 3. Build and run
cmake --build build --target mymodule_tests
./build/test/unit/mymodule_tests
```

### Integration Test
```bash
# 1. Create test file in test/integration/
# 2. Add to test/integration/CMakeLists.txt
# 3. Link multiple GVDB modules
# 4. Run with: ctest --test-dir build -R YourIntegrationTest
```

### E2E Test
```bash
# 1. Create test file in test/e2e/ (Go)
# 2. Add to test/e2e/run_all_tests.sh
# 3. Run with: cd test/e2e && ./run_all_tests.sh
```

---

## CI/CD Integration

### GitHub Actions Example
```yaml
- name: Unit Tests
  run: ctest --test-dir build --output-on-failure

- name: Integration Tests
  run: ctest --test-dir build -R Integration

- name: E2E Tests
  run: cd test/e2e && ./run_all_tests.sh
```

---

## Troubleshooting

### Tests Failing After Code Change
1. Run only affected module: `./build/test/unit/module_tests`
2. Check test output for specific failures
3. Use `--gtest_filter` to run specific test: `./build/test/unit/core_tests --gtest_filter="Vector*"`

### Integration Tests Timing Out
- Increase timeout in CMakeLists.txt: `set_tests_properties(... PROPERTIES TIMEOUT 300)`
- Check for deadlocks or infinite loops
- Verify test cleanup is happening

### E2E Tests Can't Connect
```bash
# Check if server is running
lsof -i :50051

# Start server manually
./build/bin/gvdb-all-in-one --port 50051 --data-dir /tmp/gvdb-test

# Check server logs
tail -f /tmp/gvdb-test/server.log
```

---

## Performance Benchmarks

### Expected Test Times (on typical dev machine)

| Test Suite | Duration | Count |
|------------|----------|-------|
| Unit tests (all) | 5-10s | ~50 tests |
| Integration tests | 10-30s | ~10 tests |
| E2E tests (all) | 60-120s | 4 suites |
| **Total** | **75-160s** | **60+ tests** |

---

## Directory Structure

```
test/
├── README.md                          # This file (overview)
├── unit/                              # Unit tests
│   ├── CMakeLists.txt
│   ├── core_test.cpp
│   ├── consensus_test.cpp
│   ├── consensus_persistence_test.cpp
│   └── ...
├── integration/                       # Integration tests
│   ├── README.md                      # Integration test details
│   ├── CMakeLists.txt
│   └── consensus_integration_test.cpp
└── e2e/                               # End-to-end tests
    ├── README.md                      # E2E test details
    ├── run_all_tests.sh
    ├── e2e.go
    ├── load.go
    ├── fuzzy.go
    └── metadata.go
```

---

## Best Practices

1. **Write tests first** (TDD) - helps clarify requirements
2. **Keep unit tests fast** - use mocks for expensive operations
3. **Integration tests verify contracts** - test module boundaries
4. **E2E tests cover critical paths** - don't test every edge case
5. **Run tests before committing** - catch issues early
6. **Monitor test flakiness** - fix or remove flaky tests
7. **Clean up test data** - use RAII and TearDown methods

---

**Last Updated**: 2025-11-18
**Test Framework**: Google Test (C++), Go testing (E2E)
**Coverage Target**: 80% for new code
