# Utils Module - Utilities and infrastructure

# Create utils library
add_library(gvdb_utils
  logger.cpp
  thread_pool.cpp
  metrics.cpp
  config.cpp
  server_bootstrap.cpp
  env_flags.cpp
  query_cache.cpp
)

# Link dependencies
target_link_libraries(gvdb_utils
  PUBLIC
    gvdb_core
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
  PRIVATE
    yaml-cpp
    ${_GRPC_GRPCPP}
)

# Include directories
target_include_directories(gvdb_utils
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Compiler flags for this module only
target_compile_options(gvdb_utils PRIVATE
  -Wall
  -Wextra
  -Werror
  # GCC 13 false positives on spdlog/fmt internals
  $<$<CXX_COMPILER_ID:GNU>:-Wno-array-bounds -Wno-stringop-overflow>
)
