# Unit tests

# Core module tests
add_executable(core_tests
  core_test.cpp
)

target_link_libraries(core_tests
  PRIVATE
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME CoreTests COMMAND core_tests)

# Set test properties
set_tests_properties(CoreTests PROPERTIES
  TIMEOUT 60
)

# Index module tests
add_executable(index_tests
  index_test.cpp
)

target_link_libraries(index_tests
  PRIVATE
    gvdb_index
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME IndexTests COMMAND index_tests)

# Set test properties
set_tests_properties(IndexTests PROPERTIES
  TIMEOUT 120
)

# Storage module tests
add_executable(storage_tests
  storage_test.cpp
)

target_link_libraries(storage_tests
  PRIVATE
    gvdb_storage
    gvdb_index
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME StorageTests COMMAND storage_tests)

# Set test properties
set_tests_properties(StorageTests PROPERTIES
  TIMEOUT 120
)

# Utils module tests
add_executable(utils_tests
  utils_test.cpp
)

target_link_libraries(utils_tests
  PRIVATE
    gvdb_utils
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME UtilsTests COMMAND utils_tests)

# Set test properties
set_tests_properties(UtilsTests PROPERTIES
  TIMEOUT 60
)

# Compute module tests
add_executable(compute_tests
  compute_test.cpp
)

target_link_libraries(compute_tests
  PRIVATE
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME ComputeTests COMMAND compute_tests)

# Set test properties
set_tests_properties(ComputeTests PROPERTIES
  TIMEOUT 120
)

# Network module tests
add_executable(network_tests
  network_test.cpp
)

target_link_libraries(network_tests
  PRIVATE
    network
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME NetworkTests COMMAND network_tests)

# Set test properties
set_tests_properties(NetworkTests PROPERTIES
  TIMEOUT 120
)

# Cluster module tests (simplified - no storage/compute dependencies)
add_executable(cluster_tests
  cluster_simple_test.cpp
)

target_link_libraries(cluster_tests
  PRIVATE
    gvdb_cluster
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME ClusterTests COMMAND cluster_tests)

# Set test properties
set_tests_properties(ClusterTests PROPERTIES
  TIMEOUT 120
)

# Consensus module tests
add_executable(consensus_tests
  consensus_test.cpp
)

target_link_libraries(consensus_tests
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_core
    GTest::gtest
    GTest::gtest_main
)

# Add test to CTest
add_test(NAME ConsensusTests COMMAND consensus_tests)

# Set test properties
set_tests_properties(ConsensusTests PROPERTIES
  TIMEOUT 120
)
