cmake_minimum_required(VERSION 3.15)
project(gvdb VERSION 0.39.0 LANGUAGES CXX) # x-release-please-version

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

# Option to enable Google Cloud Storage support (requires google-cloud-cpp).
# Unlike the AWS SDK (fetched from source), google-cloud-cpp is expected to be
# provided by the system / a package manager (vcpkg feature "storage", apt, or
# a source install) via find_package — its maintainers discourage FetchContent
# and it must share the abseil/protobuf/gRPC this project already builds.
option(GVDB_WITH_GCS "Enable Google Cloud Storage object storage support" OFF)

# Option to enable Apple Metal GPU acceleration (macOS only)
if(APPLE)
  option(GVDB_WITH_METAL "Enable Apple Metal GPU acceleration" OFF)
endif()

# Option to enable NVIDIA CUDA GPU acceleration via faiss-gpu
option(GVDB_WITH_CUDA "Enable NVIDIA CUDA GPU acceleration via faiss-gpu" OFF)

# ============================================================================
# Abseil C++ Library
# ============================================================================
if(USE_SYSTEM_DEPENDENCIES)
  find_package(absl QUIET)
endif()

if(NOT absl_FOUND)
  message(STATUS "Fetching Abseil...")
  # 20240722.0 LTS (was 20240116.2). Bumped because the older release
  # hardcodes -msse4.1 in absl/random/internal/randen_hwaes regardless
  # of target architecture, which apple-clang rejects on arm64. The
  # 20240722 LTS gates that flag on x86_64 detection. gRPC v1.66.0
  # (pinned below) lists this exact abseil commit in its bazel deps,
  # so the bump doesn't ripple.
  FetchContent_Declare(
    abseil-cpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG        20240722.0
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
# xxHash - Fast non-cryptographic 64-bit checksum
# ============================================================================
# Used by backup/restore manifests to verify archive integrity. Built as a
# static library via xxHash's cmake_unofficial subdirectory, which exposes the
# xxHash::xxhash target.
if(USE_SYSTEM_DEPENDENCIES)
  find_package(xxHash QUIET)
endif()

if(NOT xxHash_FOUND)
  message(STATUS "Fetching xxHash...")
  FetchContent_Declare(
    xxhash
    GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
    GIT_TAG        v0.8.2
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  cmake_unofficial
  )

  set(XXHASH_BUILD_XXHSUM OFF CACHE BOOL "" FORCE)
  set(XXHASH_BUILD_ENABLE_INLINE_API ON CACHE BOOL "" FORCE)
  set(_SAVE_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(xxhash)

  set(BUILD_SHARED_LIBS ${_SAVE_BUILD_SHARED_LIBS} CACHE BOOL "" FORCE)
endif()

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

  # Configure Faiss with OpenMP enabled for parallel processing.
  # GPU is opt-in via GVDB_WITH_CUDA; the else-branch clamps the cache so
  # an earlier ON value can't leak into a CPU build.
  if(GVDB_WITH_CUDA)
    # Default to common datacenter SMs: T4 (75), A100 (80), A10/A10G (86),
    # L4/L40 (89), H100 (90). Overridable via -DCMAKE_CUDA_ARCHITECTURES=...
    if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
      set(CMAKE_CUDA_ARCHITECTURES "75;80;86;89;90" CACHE STRING
          "CUDA SMs to compile for; default covers T4 through H100")
    endif()
    enable_language(CUDA)
    find_package(CUDAToolkit REQUIRED)
    set(FAISS_ENABLE_GPU ON CACHE BOOL "" FORCE)
    add_compile_definitions(GVDB_HAS_CUDA=1)
  else()
    set(FAISS_ENABLE_GPU OFF CACHE BOOL "" FORCE)
  endif()
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
  # 1.14.1 bundles fmt 10 cleanly. The 1.12 release pairs with an older
  # fmt that triggers a consteval failure when re-evaluated by Apple's
  # clang on macOS SDK 26.4+. We keep the bundled-fmt path (rather than
  # enabling SPDLOG_USE_STD_FORMAT) because our utils/logger.h uses
  # fmt::runtime to accept runtime format strings — std::format has no
  # runtime-string adapter that matches that API.
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.14.1
    GIT_SHALLOW    TRUE
  )

  set(SPDLOG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  # Apple clang on macOS SDK 26.4+ rejects consteval calls in contexts
  # mainline clang/gcc accept (the front-end's constexpr propagation is
  # narrower). fmt's `basic_format_string` ctor is consteval, and
  # spdlog's fmt_helper.h calls it in places Apple clang flags. Empty-
  # defining `FMT_CONSTEVAL` makes fmt fall back to plain constexpr — a
  # no-op semantically (fmt uses consteval only for compile-time
  # format-string validation, which the constexpr fallback still
  # performs). Set as a project-wide compile definition so every TU
  # that includes any spdlog/fmt header sees it.
  if(APPLE)
    add_compile_definitions(FMT_CONSTEVAL=)
  endif()
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

# When GCS is enabled, google-cloud-cpp (from vcpkg) links OpenSSL and its
# curl links OpenSSL + zlib. gRPC's default bundled BoringSSL exports the same
# OpenSSL-ABI crypto symbols, so linking both into one binary fails with
# "multiple definition" (i2d_DSAPrivateKey, EVP_PKEY_CTX_*, ...). Point gRPC at
# the same OpenSSL/zlib via find_package so there is a single crypto/zlib
# library; the vcpkg toolchain makes find_package resolve to the vcpkg copies
# google-cloud-cpp already uses. Only affects the GCS build — the default and
# S3 builds keep gRPC's bundled BoringSSL.
if(GVDB_WITH_GCS)
  set(gRPC_SSL_PROVIDER "package" CACHE STRING "" FORCE)
  set(gRPC_ZLIB_PROVIDER "package" CACHE STRING "" FORCE)
endif()

# On macOS, use the system (brew) zlib instead of gRPC's vendored copy.
# gRPC's bundled zlib unconditionally defines `fdopen(fd,mode) NULL` in
# zutil.h on macOS, which collides with SDK 26.4+'s <stdio.h> and fails
# the build before any of our code is parsed. brew zlib is keg-only at
# /opt/homebrew/opt/zlib; expose it to find_package via ZLIB_ROOT.
if(APPLE)
  set(gRPC_ZLIB_PROVIDER "package" CACHE STRING "" FORCE)
  if(EXISTS "/opt/homebrew/opt/zlib")
    set(ZLIB_ROOT "/opt/homebrew/opt/zlib" CACHE PATH "" FORCE)
  endif()
endif()

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

# ============================================================================
# google-cloud-cpp (GCS Object Storage) - Optional
# ============================================================================
# Resolved via find_package rather than FetchContent: google-cloud-cpp's
# maintainers discourage FetchContent (long builds, and it must reconcile with
# the abseil/protobuf/gRPC this project already vendors). Provide it via vcpkg
# (feature "storage"), a system package, or a source install. Only the storage
# component is required.
if(GVDB_WITH_GCS)
  message(STATUS "Locating google-cloud-cpp (storage)...")
  find_package(google_cloud_cpp_storage CONFIG REQUIRED)
endif()

# ============================================================================
# Apple metal-cpp (Metal GPU Acceleration) - Optional, macOS only
# ============================================================================
if(APPLE AND GVDB_WITH_METAL)
  message(STATUS "Fetching metal-cpp...")
  FetchContent_Declare(
    metal-cpp
    GIT_REPOSITORY https://github.com/bkaradzic/metal-cpp.git
    GIT_TAG        metal-cpp_26
    GIT_SHALLOW    TRUE
  )
  FetchContent_MakeAvailable(metal-cpp)
  add_compile_definitions(GVDB_HAS_METAL=1)
  enable_language(OBJCXX)
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
  add_subdirectory(test/bench)
endif()
