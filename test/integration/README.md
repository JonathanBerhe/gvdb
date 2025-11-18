# Integration Tests

C++ integration tests for multi-component interactions within GVDB.

See [test/README.md](../README.md) for an overview of all test types.

## What Are Integration Tests?

Integration tests verify that multiple GVDB components work correctly together when integrated in-process. Unlike unit tests (single component) or e2e tests (full system via gRPC), integration tests focus on **component boundaries and interactions**.

## Current Test Suites

### Consensus Integration Tests

Tests Raft consensus persistence and recovery scenarios.

**Phase 1: Single-Node Persistence**
- `BasicRestartRecovery` - Append entries, restart, verify recovery
- `CrashRecovery` - Simulate crash, verify data persists
- `LogCompactionRecovery` - Compact log, restart, verify compaction persisted
- `ConfigurationPersistence` - Multi-server config persistence

**Phase 2: Multi-Node Cluster**
- `ClusterFormationAndRestart` - 3-node cluster formation and restart
- `LeaderCrashRecovery` - Leader crash and recovery
- `MajorityCrashRecovery` - Majority of nodes crash and recover

## Running Integration Tests

```bash
# Build integration tests
cmake --build build

# Run all integration tests
ctest --test-dir build -R Integration

# Run specific test suite
./build/test/integration/consensus_integration_tests

# Run specific test case
./build/test/integration/consensus_integration_tests --gtest_filter="*BasicRestart*"
```

## Adding New Integration Tests

1. Create `<module>_integration_test.cpp`
2. Add to `CMakeLists.txt`
3. Link required GVDB modules
4. Use Google Test framework

Example:
```cpp
#include "module_a/component_a.h"
#include "module_b/component_b.h"
#include <gtest/gtest.h>

TEST(ModuleIntegration, ComponentInteraction) {
  ComponentA comp_a;
  ComponentB comp_b;

  // Test interaction between components
  comp_b.SetDataSource(&comp_a);
  EXPECT_TRUE(comp_b.ProcessData());
}
```

## When to Use Integration Tests

✅ **Use integration tests when:**
- Testing interactions between 2+ modules
- Verifying interfaces/contracts between components
- Testing scenarios requiring real database/persistence
- Validating state transitions across components

❌ **Don't use integration tests when:**
- Testing single component (use unit tests)
- Testing full system via API (use e2e tests)
- Test can be done with mocks (use unit tests)

## Performance Expectations

| Test | Expected Duration |
|------|-------------------|
| Single node tests | 1-5s |
| Multi-node tests | 5-10s |
| Full suite | <30s |
