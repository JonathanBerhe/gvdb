# Integration Tests

C++ integration tests for interactions between GVDB components.

See [test/README.md](../README.md) for an overview of all test types.

## What Are Integration Tests?

Integration tests run several GVDB components in one process, usually as real gRPC servers on ephemeral ports. Unit tests cover one component. The Python SDK tests in `clients/python/tests` start the real `gvdb-single-node` binary.

## Current Test Suites

| Executable | Covers |
|------------|--------|
| `raft_auto_join_tests` | Multi-node Raft: bootstrap, join, scale down, leadership transfer, ghost peer cleanup |
| `distributed_data_node_tests` | Coordinator to data node: create, insert, search, replicate, failover |
| `segment_replication_integration_tests` | Segment pull on search, segment cache reuse, read repair |
| `proxy_integration_tests` | Full flow through the proxy |
| `backup_restore_integration_tests` | Backup and restore over gRPC |
| `drain_read_fallback_tests` | Reads through the proxy while a data node drains |
| `primary_swap_integration_tests` | Upsert retries across a primary swap |

## Running Integration Tests

```bash
# Build the dev preset (tests included)
cmake --build --preset dev

# Run all integration tests
ctest --preset dev -L integration

# Run one suite
./build/dev/test/integration/raft_auto_join_tests

# Run one case
./build/dev/test/integration/raft_auto_join_tests --test-case="*ScaleUp*"
```

## Adding New Integration Tests

1. Create `<module>_integration_test.cpp`
2. Register it in `CMakeLists.txt` with `gvdb_add_test(... LABELS integration)`
3. List the GVDB modules it needs in `DEPS`
4. Write the cases with doctest

Example:
```cpp
#include "module_a/component_a.h"
#include "module_b/component_b.h"
#include <doctest/doctest.h>

TEST_CASE("ModuleIntegration - ComponentInteraction") {
  ComponentA comp_a;
  ComponentB comp_b;

  // Test interaction between components
  comp_b.SetDataSource(&comp_a);
  CHECK(comp_b.ProcessData());
}
```

## When to Use Integration Tests

Use an integration test to:
- Test interactions between two or more modules
- Test the interface between components over a real gRPC connection
- Test scenarios that need real persistence on disk
- Test state transitions across components

Use a unit test instead to:
- Test one component
- Test a behavior that mocks can cover
