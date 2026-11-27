# Unit tests of the index module (src/index). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(index_tests DISCOVER
  SOURCES
    index_test.cpp
    sparse_index_test.cpp
    turboquant_test.cpp
  DEPS gvdb::index gvdb::core
  TIMEOUT 120 LABELS unit)

if(GVDB_WITH_CUDA)
  gvdb_add_test(cuda_smoke_tests
    SOURCES cuda_smoke_test.cpp
    DEPS gvdb::index gvdb::core faiss
    TIMEOUT 120 LABELS unit gpu cuda)
endif()

if(GVDB_WITH_METAL)
  gvdb_add_test(metal_distance_tests
    SOURCES metal_distance_test.cpp
    DEPS gvdb::index gvdb::core gvdb::metal_cpp faiss
    # Loads the kernel source from <build>/kernels.
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
    TIMEOUT 120 LABELS unit gpu metal)
endif()
