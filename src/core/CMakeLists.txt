# Core module - Foundation layer with zero internal dependencies

# Create core library
add_library(gvdb_core
  vector.cpp
  status.cpp
  metadata.cpp
  filter.cpp
)

# Set target properties
set_target_properties(gvdb_core PROPERTIES
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF
  POSITION_INDEPENDENT_CODE ON
)

# Include directories
target_include_directories(gvdb_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
)

# Link dependencies (only abseil allowed for core module)
target_link_libraries(gvdb_core
  PUBLIC
    absl::status
    absl::statusor
    absl::strings
)

# Compiler warnings (applied only to our code, not dependencies)
# Remove -Werror for now due to Abseil using Clang extensions
target_compile_options(gvdb_core PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
)

# Export library
install(TARGETS gvdb_core
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
)
