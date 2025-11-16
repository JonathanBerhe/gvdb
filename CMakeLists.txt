cmake_minimum_required(VERSION 3.15)
project(gvdb VERSION 0.1.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags (don't set globally to avoid affecting dependencies)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG")

# Export compile commands for tooling
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# CMake modules path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Use FetchContent to download dependencies
include(FetchContent)

# Option to use system dependencies if available
option(USE_SYSTEM_DEPENDENCIES "Use system-installed dependencies instead of fetching" OFF)

# ============================================================================
# Abseil C++ Library
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(absl QUIET)
endif()

if(NOT absl_FOUND)
  message(STATUS "Fetching Abseil...")
  FetchContent_Declare(
    abseil-cpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG        20240116.2  # LTS 20240116, Patch 2
    GIT_SHALLOW    TRUE
  )

  # Configure Abseil
  set(ABSL_PROPAGATE_CXX_STD ON)
  set(ABSL_ENABLE_INSTALL OFF)
  set(BUILD_TESTING OFF)

  FetchContent_MakeAvailable(abseil-cpp)
endif()

# ============================================================================
# Google Test
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(GTest QUIET)
endif()

if(NOT GTest_FOUND)
  message(STATUS "Fetching GoogleTest...")
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
    GIT_SHALLOW    TRUE
  )

  # Configure GoogleTest
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF)

  FetchContent_MakeAvailable(googletest)
endif()

# ============================================================================
# OpenMP - Parallel processing support
# ============================================================================
# For containers: Install libomp in your Dockerfile
# For macOS development: brew install libomp
# For Linux: apt-get install libomp-dev or yum install libomp-devel
find_package(OpenMP)

if(NOT OpenMP_FOUND)
  message(WARNING "OpenMP not found. Faiss will build without parallel support.")
  message(WARNING "Install OpenMP:")
  message(WARNING "  macOS: brew install libomp")
  message(WARNING "  Ubuntu/Debian: sudo apt-get install libomp-dev")
  message(WARNING "  RHEL/CentOS: sudo yum install libomp-devel")
  message(WARNING "  Container: Add libomp to your Dockerfile")
endif()

# ============================================================================
# Faiss - Vector Similarity Search Library
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(faiss QUIET)
endif()

if(NOT faiss_FOUND)
  message(STATUS "Fetching Faiss...")

  # Set OpenMP flags and include paths before Faiss configuration
  if(OpenMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS} -I/opt/homebrew/opt/libomp/include")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS} -I/opt/homebrew/opt/libomp/include")
  endif()

  FetchContent_Declare(
    faiss
    GIT_REPOSITORY https://github.com/facebookresearch/faiss.git
    GIT_TAG        v1.8.0
    GIT_SHALLOW    TRUE
  )

  # Configure Faiss with OpenMP enabled for parallel processing
  set(FAISS_ENABLE_GPU OFF CACHE BOOL "" FORCE)
  set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "" FORCE)
  set(FAISS_ENABLE_C_API OFF CACHE BOOL "" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(faiss)
endif()

# ============================================================================
# spdlog - Fast C++ Logging Library
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(spdlog QUIET)
endif()

if(NOT spdlog_FOUND)
  message(STATUS "Fetching spdlog...")
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.12.0
    GIT_SHALLOW    TRUE
  )

  set(SPDLOG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(spdlog)
endif()

# ============================================================================
# yaml-cpp - YAML Parser and Emitter
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(yaml-cpp QUIET)
endif()

if(NOT yaml-cpp_FOUND)
  message(STATUS "Fetching yaml-cpp...")
  FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG        yaml-cpp-0.7.0
    GIT_SHALLOW    TRUE
  )

  set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
  set(YAML_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(yaml-cpp)
endif()

# ============================================================================
# prometheus-cpp - Prometheus Client Library
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(prometheus-cpp QUIET)
endif()

if(NOT prometheus-cpp_FOUND)
  message(STATUS "Fetching prometheus-cpp...")
  FetchContent_Declare(
    prometheus-cpp
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
    GIT_TAG        v1.1.0
    GIT_SHALLOW    TRUE
  )

  set(ENABLE_PUSH OFF CACHE BOOL "" FORCE)
  set(ENABLE_COMPRESSION OFF CACHE BOOL "" FORCE)
  set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(prometheus-cpp)
endif()

# ============================================================================
# gRPC and Protobuf - RPC Framework
# ============================================================================
# Note: We always fetch gRPC/Protobuf to avoid version conflicts with system abseil
message(STATUS "Fetching gRPC and Protobuf...")

# Disable installation for all FetchContent dependencies
set(CMAKE_SKIP_INSTALL_RULES ON)
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY ON)

# Fetch gRPC (includes Protobuf as a submodule)
FetchContent_Declare(
  grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG        v1.66.0  # C++20 compatible, works with abseil LTS 20240116
  GIT_SHALLOW    TRUE
)

# Configure gRPC - disable unnecessary plugins and installation
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
set(gRPC_ABSL_PROVIDER "module" CACHE STRING "" FORCE)  # Use our FetchContent abseil
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(grpc)

# Re-enable installation for our own targets
set(CMAKE_SKIP_INSTALL_RULES OFF)

# Set up Protobuf and gRPC targets
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

# ============================================================================
# Proto Library Generation
# ============================================================================
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_PATH}/vectordb.proto")
set(GENERATED_PROTO_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTO_PATH})

# Generated files
set(PROTO_SRCS "${GENERATED_PROTO_PATH}/vectordb.pb.cc")
set(PROTO_HDRS "${GENERATED_PROTO_PATH}/vectordb.pb.h")
set(GRPC_SRCS "${GENERATED_PROTO_PATH}/vectordb.grpc.pb.cc")
set(GRPC_HDRS "${GENERATED_PROTO_PATH}/vectordb.grpc.pb.h")

# Custom command to generate proto files
add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out=${GENERATED_PROTO_PATH}
       --cpp_out=${GENERATED_PROTO_PATH}
       -I ${PROTO_PATH}
       --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf and gRPC C++ files"
)

# Create proto library
add_library(proto_lib
  ${PROTO_SRCS}
  ${PROTO_HDRS}
  ${GRPC_SRCS}
  ${GRPC_HDRS}
)

target_link_libraries(proto_lib
  PUBLIC
    ${_PROTOBUF_LIBPROTOBUF}
    ${_GRPC_GRPCPP}
)

target_include_directories(proto_lib
  PUBLIC
    ${GENERATED_PROTO_PATH}
)

# Enable testing
enable_testing()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Add subdirectories
add_subdirectory(src/core)
add_subdirectory(src/index)
add_subdirectory(src/storage)
add_subdirectory(src/utils)
add_subdirectory(src/compute)
add_subdirectory(src/network)
add_subdirectory(src/cluster)
add_subdirectory(src/consensus)
add_subdirectory(src/main)
add_subdirectory(test/unit)
