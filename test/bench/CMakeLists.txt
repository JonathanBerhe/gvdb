# Benchmarks. They run by hand (make bench-search, make bench-metal,
# make bench-cuda), not under CTest.

# CPU search latency of the query executor over FAISS segments.
gvdb_add_executable(gvdb-search-bench
  SOURCES search_bench.cpp
  DEPS gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core
)

if(GVDB_WITH_METAL)
  gvdb_add_executable(gvdb-metal-bench
    SOURCES metal_bench.cpp
    DEPS gvdb::index gvdb::core gvdb::metal_cpp faiss
  )
endif()

if(GVDB_WITH_CUDA)
  # Uses the faiss-gpu headers directly (GpuCloner, StandardGpuResources),
  # which include cublas_v2.h.
  gvdb_add_executable(gvdb-cuda-bench
    SOURCES cuda_bench.cpp
    DEPS gvdb::index gvdb::core faiss CUDA::cudart CUDA::cublas
  )
endif()
