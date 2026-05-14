# Index module - Vector indexing with Faiss

# Create index library
add_library(gvdb_index
  faiss_base.cpp
  faiss_flat.cpp
  faiss_hnsw.cpp
  faiss_ivf.cpp
  index_factory.cpp
  index_manager.cpp
  turboquant/turboquant_codec.cpp
  turboquant/turboquant_index.cpp
  turboquant/ivf_turboquant_index.cpp
  bm25_index.cpp
  sparse_index.cpp
)

# Metal GPU acceleration (macOS only)
if(APPLE AND GVDB_WITH_METAL)
  target_sources(gvdb_index PRIVATE
    metal/metal_compute.mm
    metal/metal_flat_index.mm
  )
  target_include_directories(gvdb_index PRIVATE
    ${metal-cpp_SOURCE_DIR}
  )
  target_link_libraries(gvdb_index PRIVATE
    "-framework Metal"
    "-framework Foundation"
  )
  # Copy kernel source to build dir at build time
  add_custom_command(TARGET gvdb_index POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      ${CMAKE_CURRENT_SOURCE_DIR}/metal/kernels/distance.metal
      ${CMAKE_BINARY_DIR}/kernels/distance.metal
  )
endif()

# NVIDIA CUDA GPU acceleration (via faiss-gpu)
if(GVDB_WITH_CUDA)
  target_sources(gvdb_index PRIVATE
    cuda/cuda_compute.cpp
  )
  target_link_libraries(gvdb_index PRIVATE CUDA::cudart CUDA::cublas)
endif()

# Set target properties
set_target_properties(gvdb_index PROPERTIES
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF
  POSITION_INDEPENDENT_CODE ON
)

# Include directories
target_include_directories(gvdb_index
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/turboquant
    /opt/homebrew/opt/libomp/include
)

# Link dependencies. faiss + faiss_gpu have circular references (faiss_gpu
# calls back into faiss CPU symbols like ParameterSpace, IndexShardsIVF). A
# single-pass static link can't resolve them; LINK_GROUP RESCAN emits
# -Wl,--start-group/--end-group so ld multi-passes.
if(GVDB_WITH_CUDA)
  # faiss's interface adds faiss_gpu with WHOLE_ARCHIVE; we must use the
  # same feature when including faiss_gpu inside the rescan group or CMake
  # rejects the link with a "feature mismatch" error.
  target_link_libraries(gvdb_index
    PUBLIC
      gvdb_core
      "$<LINK_GROUP:RESCAN,faiss,$<LINK_LIBRARY:WHOLE_ARCHIVE,faiss_gpu>>"
  )
else()
  target_link_libraries(gvdb_index
    PUBLIC
      gvdb_core
      faiss
  )
endif()

# Compiler warnings
target_compile_options(gvdb_index PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
)

# Export library
install(TARGETS gvdb_index
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
)
