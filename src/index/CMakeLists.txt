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

# Link dependencies
target_link_libraries(gvdb_index
  PUBLIC
    gvdb_core
    faiss
)

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
