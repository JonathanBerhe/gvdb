cmake_minimum_required(VERSION 3.15)
project(gvdb VERSION 0.12.0 LANGUAGES CXX) # x-release-please-version

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

# Option to enable S3/MinIO object storage support (requires AWS SDK)
option(GVDB_WITH_S3 "Enable S3/MinIO object storage support" OFF)

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

  # Configure Abseil (save/restore BUILD_TESTING to avoid polluting parent scope)
  set(ABSL_PROPAGATE_CXX_STD ON)
  set(ABSL_ENABLE_INSTALL OFF)
  set(_SAVE_BUILD_TESTING ${BUILD_TESTING})
  set(BUILD_TESTING OFF)

  FetchContent_MakeAvailable(abseil-cpp)

  set(BUILD_TESTING ${_SAVE_BUILD_TESTING})
endif()

# ============================================================================
# doctest - Lightweight C++ Testing Framework
# ============================================================================
message(STATUS "Fetching doctest...")
FetchContent_Declare(
  doctest
  GIT_REPOSITORY https://github.com/doctest/doctest.git
  GIT_TAG        v2.4.11
  GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(doctest)

# ============================================================================
# OpenMP - Parallel processing support
# ============================================================================
# For containers: Install libomp in your Dockerfile
# For macOS development: brew install libomp
# For Linux: apt-get install libomp-dev or yum install libomp-devel
if(APPLE AND EXISTS "/opt/homebrew/opt/libomp")
  set(OpenMP_ROOT "/opt/homebrew/opt/libomp")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
  set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp")
  set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp")
  set(OpenMP_C_LIB_NAMES "omp")
  set(OpenMP_CXX_LIB_NAMES "omp")
  set(OpenMP_omp_LIBRARY "/opt/homebrew/opt/libomp/lib/libomp.dylib")
endif()
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
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

  # Save BUILD_TESTING before Faiss overrides it, restore after
  set(_SAVE_BT_FAISS ${BUILD_TESTING})
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(faiss)
  set(BUILD_TESTING ${_SAVE_BT_FAISS} CACHE BOOL "" FORCE)
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
# NuRaft - Modular Raft Implementation
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(NuRaft QUIET)
endif()

if(NOT NuRaft_FOUND)
  message(STATUS "Fetching NuRaft...")
  FetchContent_Declare(
    NuRaft
    GIT_REPOSITORY https://github.com/eBay/NuRaft.git
    GIT_TAG        v3.0.0
    GIT_SHALLOW    TRUE
  )

  # Configure NuRaft - disable unnecessary features
  set(DISABLE_SSL ON CACHE BOOL "" FORCE)
  set(ENABLE_RAFT_STATS OFF CACHE BOOL "" FORCE)
  set(BUILD_TESTS OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(NuRaft)
endif()

# ============================================================================
# RocksDB - Persistent Storage Engine
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(RocksDB QUIET)
endif()

if(NOT RocksDB_FOUND)
  message(STATUS "Fetching RocksDB...")
  FetchContent_Declare(
    rocksdb
    GIT_REPOSITORY https://github.com/facebook/rocksdb.git
    GIT_TAG        v9.7.3  # Latest stable release
    GIT_SHALLOW    TRUE
  )

  # Configure RocksDB - disable unnecessary features for faster build
  set(WITH_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
  set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
  set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)  # Disable Snappy compression
  set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
  set(WITH_BZ2 OFF CACHE BOOL "" FORCE)
  set(USE_RTTI ON CACHE BOOL "" FORCE)
  set(PORTABLE ON CACHE BOOL "" FORCE)  # Build portable binary
  set(ROCKSDB_LITE OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(rocksdb)
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

# ============================================================================
# AWS SDK for C++ (S3 Object Storage) - Optional
# ============================================================================
if(GVDB_WITH_S3)
  message(STATUS "Fetching AWS SDK for C++ (S3 only)...")
  FetchContent_Declare(
    awssdk
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG        1.11.789
    GIT_SHALLOW    TRUE
  )
  set(BUILD_ONLY "s3" CACHE STRING "" FORCE)
  set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
  set(MINIMIZE_SIZE ON CACHE BOOL "" FORCE)
  # Use the CRT's bundled aws-lc for crypto instead of system OpenSSL.
  # System OpenSSL 3.x on Ubuntu 24.04 has EVP_PKEY API changes that
  # break the CRT's s2n-tls build. aws-lc is OpenSSL 1.1.1-compatible.
  set(USE_OPENSSL OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(awssdk)
endif()

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
set(VECTORDB_PROTO_FILE "${PROTO_PATH}/vectordb.proto")
set(INTERNAL_PROTO_FILE "${PROTO_PATH}/internal.proto")
set(GENERATED_PROTO_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTO_PATH})

# Generated files for vectordb.proto (client-facing API)
set(VECTORDB_PROTO_SRCS "${GENERATED_PROTO_PATH}/vectordb.pb.cc")
set(VECTORDB_PROTO_HDRS "${GENERATED_PROTO_PATH}/vectordb.pb.h")
set(VECTORDB_GRPC_SRCS "${GENERATED_PROTO_PATH}/vectordb.grpc.pb.cc")
set(VECTORDB_GRPC_HDRS "${GENERATED_PROTO_PATH}/vectordb.grpc.pb.h")

# Generated files for internal.proto (node-to-node communication)
set(INTERNAL_PROTO_SRCS "${GENERATED_PROTO_PATH}/internal.pb.cc")
set(INTERNAL_PROTO_HDRS "${GENERATED_PROTO_PATH}/internal.pb.h")
set(INTERNAL_GRPC_SRCS "${GENERATED_PROTO_PATH}/internal.grpc.pb.cc")
set(INTERNAL_GRPC_HDRS "${GENERATED_PROTO_PATH}/internal.grpc.pb.h")

# Custom command to generate vectordb.proto files
add_custom_command(
  OUTPUT ${VECTORDB_PROTO_SRCS} ${VECTORDB_PROTO_HDRS} ${VECTORDB_GRPC_SRCS} ${VECTORDB_GRPC_HDRS}
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out=${GENERATED_PROTO_PATH}
       --cpp_out=${GENERATED_PROTO_PATH}
       -I ${PROTO_PATH}
       --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
       ${VECTORDB_PROTO_FILE}
  DEPENDS ${VECTORDB_PROTO_FILE}
  COMMENT "Generating vectordb protobuf and gRPC C++ files"
)

# Custom command to generate internal.proto files
add_custom_command(
  OUTPUT ${INTERNAL_PROTO_SRCS} ${INTERNAL_PROTO_HDRS} ${INTERNAL_GRPC_SRCS} ${INTERNAL_GRPC_HDRS}
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out=${GENERATED_PROTO_PATH}
       --cpp_out=${GENERATED_PROTO_PATH}
       -I ${PROTO_PATH}
       --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
       ${INTERNAL_PROTO_FILE}
  DEPENDS ${INTERNAL_PROTO_FILE} ${VECTORDB_PROTO_FILE}
  COMMENT "Generating internal protobuf and gRPC C++ files"
)

# Create proto library (includes both vectordb and internal protos)
add_library(proto_lib
  ${VECTORDB_PROTO_SRCS}
  ${VECTORDB_PROTO_HDRS}
  ${VECTORDB_GRPC_SRCS}
  ${VECTORDB_GRPC_HDRS}
  ${INTERNAL_PROTO_SRCS}
  ${INTERNAL_PROTO_HDRS}
  ${INTERNAL_GRPC_SRCS}
  ${INTERNAL_GRPC_HDRS}
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
add_subdirectory(src/auth)
add_subdirectory(src/compute)
add_subdirectory(src/network)
add_subdirectory(src/cluster)
add_subdirectory(src/consensus)
add_subdirectory(src/main)
if(BUILD_TESTING)
  add_subdirectory(test/unit)
  add_subdirectory(test/integration)
endif()
