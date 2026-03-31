# Compute Module - Query Execution Engine

# Create compute library
add_library(gvdb_compute
  query_executor.cpp
)

# Link dependencies
target_link_libraries(gvdb_compute
  PUBLIC
    gvdb_core
    gvdb_storage
    gvdb_utils
  PRIVATE
    gvdb_index
)

# Include directories
target_include_directories(gvdb_compute
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Compiler flags for this module only
target_compile_options(gvdb_compute PRIVATE
  -Wall
  -Wextra
  -Werror
  # GCC 13 false positives on spdlog/fmt internals
  $<$<CXX_COMPILER_ID:GNU>:-Wno-array-bounds -Wno-stringop-overflow>
)
