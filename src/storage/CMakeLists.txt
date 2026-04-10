# Storage module - Segment management and persistence

add_library(gvdb_storage
  segment.cpp
  segment_manager.cpp
  scalar_index.cpp
  object_store.cpp
  segment_cache.cpp
  segment_manifest.cpp
  tiered_segment_manager.cpp
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
    gvdb_utils
    absl::status
    absl::statusor
    absl::strings
)

# S3/MinIO object storage support (optional)
if(GVDB_WITH_S3)
  target_sources(gvdb_storage PRIVATE s3_object_store.cpp)
  target_link_libraries(gvdb_storage PUBLIC aws-cpp-sdk-s3 aws-cpp-sdk-core)
  target_compile_definitions(gvdb_storage PUBLIC GVDB_HAS_S3=1)
  message(STATUS "S3/MinIO storage support enabled")
endif()

# Enable C++17 features
target_compile_features(gvdb_storage PUBLIC cxx_std_17)

# Enable warnings
target_compile_options(gvdb_storage PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
)
