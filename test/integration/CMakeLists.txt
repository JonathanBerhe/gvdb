# Integration tests - multi-component testing

# Consensus integration tests
add_executable(consensus_integration_tests
  consensus_integration_test.cpp
)

target_link_libraries(consensus_integration_tests
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME ConsensusIntegrationTests COMMAND consensus_integration_tests)

# Set test properties
set_tests_properties(ConsensusIntegrationTests PROPERTIES
  TIMEOUT 300  # Integration tests can take longer
  LABELS "integration"
)
