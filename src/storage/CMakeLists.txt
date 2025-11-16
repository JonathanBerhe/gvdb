# Storage module - Segment management and persistence

add_library(gvdb_storage
  segment.cpp
  segment_manager.cpp
  local_storage.cpp
  storage_factory.cpp
)

target_include_directories(gvdb_storage
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(gvdb_storage
  PUBLIC
    gvdb_core
    gvdb_index
    absl::status
    absl::statusor
    absl::strings
)

# Enable C++17 features
target_compile_features(gvdb_storage PUBLIC cxx_std_17)

# Enable warnings
target_compile_options(gvdb_storage PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
)
