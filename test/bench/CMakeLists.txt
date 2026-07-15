# Benchmark binaries

if(APPLE AND GVDB_WITH_METAL)
  add_executable(gvdb-metal-bench
    metal_bench.cpp
  )

  target_include_directories(gvdb-metal-bench PRIVATE
    ${CMAKE_SOURCE_DIR}/src/index
    ${metal-cpp_SOURCE_DIR}
  )

  target_link_libraries(gvdb-metal-bench
    PRIVATE
      gvdb_index
      gvdb_core
      faiss
      "-framework Metal"
      "-framework Foundation"
  )

  set_target_properties(gvdb-metal-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
endif()

if(GVDB_WITH_CUDA)
  add_executable(gvdb-cuda-bench
    cuda_bench.cpp
  )

  target_include_directories(gvdb-cuda-bench PRIVATE
    ${CMAKE_SOURCE_DIR}/src/index
  )

  # The bench uses faiss-gpu headers directly (GpuCloner / StandardGpuResources),
  # which pull in cublas_v2.h — so it needs the CUDA toolkit include dirs the
  # imported targets carry, same as gvdb_index. CUDAToolkit is found at the
  # root scope under GVDB_WITH_CUDA.
  target_link_libraries(gvdb-cuda-bench
    PRIVATE
      gvdb_index
      gvdb_core
      faiss
      CUDA::cudart
      CUDA::cublas
  )

  set_target_properties(gvdb-cuda-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
endif()
