# Third-party dependencies.
#
# Everything comes from FetchContent at a pinned tag, except google-cloud-cpp
# (find_package, see GVDB_WITH_GCS) and the system libraries that FAISS finds
# itself (OpenMP, BLAS/LAPACK) or that the GPU lanes need (CUDA, Metal).

include(FetchContent)

# gvdb_fetch() sets BUILD_TESTING, BUILD_SHARED_LIBS and two other variables
# as plain variables in its own scope. These two policies make dependencies
# honor them, also the ones whose cmake_minimum_required() predates CMake
# 3.13 and 3.21.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW) # option() honors normal variables
set(CMAKE_POLICY_DEFAULT_CMP0126 NEW) # set(CACHE) keeps normal variables

# gvdb_fetch(<name> GIT_REPOSITORY <url> GIT_TAG <tag>
#            [SOURCE_SUBDIR <dir>] [GIT_SUBMODULES <path>...]
#            [OPTIONS <VAR=VALUE>...])
#
# Fetches a dependency and adds it to the build as a static library, without
# its tests or install rules. Those settings live in this function's scope,
# so they reach the dependency's directories but never GVDB's own targets.
#
# OPTIONS become internal cache entries, so they win even in a dependency
# that sets CMP0077 to OLD (the AWS SDK does), where option() would replace
# a plain variable with its own default. Values must not contain semicolons.
#
# GIT_SUBMODULES clones only the listed submodules, without their own
# submodules; without it, every submodule is cloned recursively.
function(gvdb_fetch name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "GIT_REPOSITORY;GIT_TAG;SOURCE_SUBDIR" "GIT_SUBMODULES;OPTIONS")

  set(BUILD_TESTING OFF)
  set(BUILD_SHARED_LIBS OFF)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  # Lets CMake 4 configure dependencies that still declare
  # cmake_minimum_required() below 3.5: only the c-ares that gRPC 1.66
  # bundles. A gRPC update can remove this line.
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

  foreach(option IN LISTS arg_OPTIONS)
    if(NOT option MATCHES "^([A-Za-z0-9_]+)=(.*)$")
      message(FATAL_ERROR "gvdb_fetch(${name}): option '${option}' is not NAME=VALUE")
    endif()
    set(${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "Set by gvdb_fetch(${name})")
  endforeach()

  set(source_subdir)
  if(arg_SOURCE_SUBDIR)
    set(source_subdir SOURCE_SUBDIR ${arg_SOURCE_SUBDIR})
  endif()

  set(submodules)
  if(arg_GIT_SUBMODULES)
    set(submodules GIT_SUBMODULES ${arg_GIT_SUBMODULES} GIT_SUBMODULES_RECURSE FALSE)
  endif()

  message(STATUS "Fetching ${name} ${arg_GIT_TAG}")
  FetchContent_Declare(${name}
    GIT_REPOSITORY ${arg_GIT_REPOSITORY}
    GIT_TAG        ${arg_GIT_TAG}
    GIT_SHALLOW    TRUE
    ${submodules}
    ${source_subdir}
    SYSTEM
    EXCLUDE_FROM_ALL
  )
  FetchContent_MakeAvailable(${name})
endfunction()

# ----------------------------------------------------------------------------
# abseil. 20240722.0 gates its -msse4.1 flag on x86_64 (the earlier LTS broke
# arm64 builds) and is the commit gRPC v1.66.0 pins in its own dependencies.
# ----------------------------------------------------------------------------
gvdb_fetch(abseil-cpp
  GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
  GIT_TAG        20240722.0
  OPTIONS
    ABSL_PROPAGATE_CXX_STD=ON
    ABSL_ENABLE_INSTALL=OFF
)

# ----------------------------------------------------------------------------
# doctest (test framework)
# ----------------------------------------------------------------------------
if(GVDB_BUILD_TESTS)
  gvdb_fetch(doctest
    GIT_REPOSITORY https://github.com/doctest/doctest.git
    GIT_TAG        v2.5.3
    OPTIONS
      DOCTEST_WITH_TESTS=OFF
      DOCTEST_NO_INSTALL=ON
  )
endif()

# ----------------------------------------------------------------------------
# CUDA toolchain for faiss-gpu. enable_language() must run at file scope.
# ----------------------------------------------------------------------------
if(GVDB_WITH_CUDA)
  # Default to common datacenter SMs: T4 (75), A100 (80), A10/A10G (86),
  # L4/L40 (89), H100 (90). Override with -DCMAKE_CUDA_ARCHITECTURES=...
  if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES "75;80;86;89;90" CACHE STRING
        "CUDA SMs to compile for; default covers T4 through H100")
  endif()
  enable_language(CUDA)
  find_package(CUDAToolkit REQUIRED)
endif()

# ----------------------------------------------------------------------------
# OpenMP. FAISS links it privately, but faiss/impl/HNSW.h includes <omp.h>,
# so gvdb_index links OpenMP::OpenMP_CXX too. On macOS, OpenMP_ROOT (set by
# the presets) points FindOpenMP at Homebrew's keg-only libomp.
# ----------------------------------------------------------------------------
find_package(OpenMP REQUIRED)

# ----------------------------------------------------------------------------
# FAISS (finds BLAS and LAPACK itself)
# ----------------------------------------------------------------------------
gvdb_fetch(faiss
  GIT_REPOSITORY https://github.com/facebookresearch/faiss.git
  GIT_TAG        v1.8.0
  OPTIONS
    FAISS_ENABLE_GPU=${GVDB_WITH_CUDA}
    FAISS_ENABLE_PYTHON=OFF
    FAISS_ENABLE_C_API=OFF
)

# ----------------------------------------------------------------------------
# spdlog with its bundled fmt 12. The bundled fmt stays (not
# SPDLOG_USE_STD_FORMAT) because common/logger.h needs fmt::runtime, which has
# no std::format equivalent before C++26.
# ----------------------------------------------------------------------------
gvdb_fetch(spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG        v1.17.0
  OPTIONS
    SPDLOG_BUILD_SHARED=OFF
)

# ----------------------------------------------------------------------------
# yaml-cpp (server configuration)
# ----------------------------------------------------------------------------
gvdb_fetch(yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        yaml-cpp-0.9.0
  OPTIONS
    YAML_CPP_BUILD_TESTS=OFF
    YAML_CPP_BUILD_TOOLS=OFF
    YAML_BUILD_SHARED_LIBS=OFF
)

# ----------------------------------------------------------------------------
# prometheus-cpp (metrics endpoint)
# ----------------------------------------------------------------------------
gvdb_fetch(prometheus-cpp
  GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
  GIT_TAG        v1.1.0
  OPTIONS
    ENABLE_PUSH=OFF
    ENABLE_COMPRESSION=OFF
    ENABLE_TESTING=OFF
)

# ----------------------------------------------------------------------------
# NuRaft (Raft consensus). Needs standalone asio headers from the system.
# ----------------------------------------------------------------------------
gvdb_fetch(NuRaft
  GIT_REPOSITORY https://github.com/eBay/NuRaft.git
  GIT_TAG        v3.0.0
  OPTIONS
    DISABLE_SSL=ON
    ENABLE_RAFT_STATS=OFF
    BUILD_TESTS=OFF
)

# ----------------------------------------------------------------------------
# RocksDB (Raft log store), with every compression codec and tool disabled
# ----------------------------------------------------------------------------
gvdb_fetch(rocksdb
  GIT_REPOSITORY https://github.com/facebook/rocksdb.git
  GIT_TAG        v10.4.2
  OPTIONS
    WITH_TESTS=OFF
    WITH_TOOLS=OFF
    WITH_BENCHMARK_TOOLS=OFF
    WITH_CORE_TOOLS=OFF
    WITH_GFLAGS=OFF
    FAIL_ON_WARNINGS=OFF
    ROCKSDB_BUILD_SHARED=OFF
    WITH_SNAPPY=OFF
    WITH_LZ4=OFF
    WITH_ZLIB=OFF
    WITH_ZSTD=OFF
    WITH_BZ2=OFF
    USE_RTTI=ON
    PORTABLE=ON
    ROCKSDB_LITE=OFF
)

# ----------------------------------------------------------------------------
# gRPC with its protobuf submodule, built against the abseil above.
#
# zlib: gRPC's bundled zlib defines fdopen(fd,mode) as NULL on macOS, which
# collides with <stdio.h> from SDK 26.4+, so macOS uses a packaged zlib
# (ZLIB_ROOT from the presets points at Homebrew's).
#
# OpenSSL: with GCS on, google-cloud-cpp links OpenSSL, and gRPC's bundled
# BoringSSL exports the same symbols, so the link fails with duplicates.
# The GCS build points gRPC at the same OpenSSL and zlib instead.
#
# Submodules: the C++ libraries and code generators need only these five.
# abseil comes from the fetch above (gRPC skips its own copy when absl
# targets exist). The other submodules (bloaty, benchmark, googletest,
# opentelemetry-cpp, googleapis, ...) serve gRPC's tests and other
# languages, and cloning them recursively takes over ten minutes.
# ----------------------------------------------------------------------------
set(gvdb_grpc_providers)
if(APPLE OR GVDB_WITH_GCS)
  list(APPEND gvdb_grpc_providers gRPC_ZLIB_PROVIDER=package)
endif()
if(GVDB_WITH_GCS)
  list(APPEND gvdb_grpc_providers gRPC_SSL_PROVIDER=package)
endif()

gvdb_fetch(grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG        v1.66.0
  GIT_SUBMODULES
    third_party/protobuf
    third_party/re2
    third_party/cares/cares
    third_party/boringssl-with-bazel
    third_party/zlib
  OPTIONS
    gRPC_ABSL_PROVIDER=module
    gRPC_BUILD_TESTS=OFF
    gRPC_BUILD_CSHARP_EXT=OFF
    gRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF
    gRPC_BUILD_GRPC_NODE_PLUGIN=OFF
    gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF
    gRPC_BUILD_GRPC_PHP_PLUGIN=OFF
    gRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF
    gRPC_BUILD_GRPC_RUBY_PLUGIN=OFF
    gRPC_INSTALL=OFF
    protobuf_INSTALL=OFF
    utf8_range_ENABLE_INSTALL=OFF
    ${gvdb_grpc_providers}
)

# ----------------------------------------------------------------------------
# AWS SDK, S3 client only. It uses the CRT's bundled aws-lc instead of the
# system OpenSSL, because OpenSSL 3 on Ubuntu 24.04 breaks the s2n-tls build.
# BUILD_SHARED_LIBS is an option here too: the SDK's legacy build sets CMP0077
# to OLD, so its option(BUILD_SHARED_LIBS ... ON) ignores the scoped variable.
# ----------------------------------------------------------------------------
if(GVDB_WITH_S3)
  gvdb_fetch(awssdk
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG        1.11.789
    OPTIONS
      BUILD_ONLY=s3
      BUILD_SHARED_LIBS=OFF
      ENABLE_TESTING=OFF
      AUTORUN_UNIT_TESTS=OFF
      MINIMIZE_SIZE=ON
      USE_OPENSSL=OFF
  )
endif()

# ----------------------------------------------------------------------------
# google-cloud-cpp, storage component only
# ----------------------------------------------------------------------------
if(GVDB_WITH_GCS)
  find_package(google_cloud_cpp_storage CONFIG REQUIRED)
endif()

# ----------------------------------------------------------------------------
# Apple metal-cpp headers, exposed as the gvdb::metal_cpp interface target
# together with the Metal and Foundation frameworks
# ----------------------------------------------------------------------------
if(GVDB_WITH_METAL)
  enable_language(OBJCXX)
  gvdb_fetch(metal-cpp
    GIT_REPOSITORY https://github.com/bkaradzic/metal-cpp.git
    GIT_TAG        metal-cpp_26
  )
  FetchContent_GetProperties(metal-cpp SOURCE_DIR gvdb_metal_cpp_dir)
  add_library(gvdb_metal_cpp INTERFACE)
  add_library(gvdb::metal_cpp ALIAS gvdb_metal_cpp)
  target_include_directories(gvdb_metal_cpp SYSTEM INTERFACE "${gvdb_metal_cpp_dir}")
  target_link_libraries(gvdb_metal_cpp INTERFACE "-framework Metal" "-framework Foundation")
endif()
