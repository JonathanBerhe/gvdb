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

  target_link_libraries(gvdb-cuda-bench
    PRIVATE
      gvdb_index
      gvdb_core
      faiss
  )

  set_target_properties(gvdb-cuda-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
endif()
