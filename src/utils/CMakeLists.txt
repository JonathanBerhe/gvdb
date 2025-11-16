# Utils Module - Utilities and infrastructure

# Create utils library
add_library(gvdb_utils
  logger.cpp
  thread_pool.cpp
)

# Link dependencies
target_link_libraries(gvdb_utils
  PUBLIC
    gvdb_core
    spdlog::spdlog
  PRIVATE
    yaml-cpp
    prometheus-cpp::core
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
)
