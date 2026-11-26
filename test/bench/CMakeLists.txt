# GPU benchmarks. They run by hand (make bench-metal, make bench-cuda), not
# under CTest.

if(GVDB_WITH_METAL)
  gvdb_add_executable(gvdb-metal-bench
    SOURCES metal_bench.cpp
    DEPS gvdb::index gvdb::core gvdb::metal_cpp faiss
    INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/index
  )
endif()

if(GVDB_WITH_CUDA)
  # Uses the faiss-gpu headers directly (GpuCloner, StandardGpuResources),
  # which include cublas_v2.h.
  gvdb_add_executable(gvdb-cuda-bench
    SOURCES cuda_bench.cpp
    DEPS gvdb::index gvdb::core faiss CUDA::cudart CUDA::cublas
    INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/index
  )
endif()
