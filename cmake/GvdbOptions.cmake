# Build options for GVDB. Presets in CMakePresets.json set them for the
# usual configurations (dev, release, ci, coverage, sanitizers, GPU lanes).

include(CMakeDependentOption)

option(GVDB_BUILD_TESTS "Build the unit tests, integration tests and benchmarks" ${PROJECT_IS_TOP_LEVEL})

option(GVDB_WITH_S3 "Enable the S3/MinIO object store (fetches the AWS SDK)" OFF)

# google-cloud-cpp comes from find_package (vcpkg, a system package or a
# source install), not FetchContent: its maintainers discourage
# FetchContent, and it must share the abseil, protobuf and gRPC that GVDB
# already builds.
option(GVDB_WITH_GCS "Enable the Google Cloud Storage object store (google-cloud-cpp from find_package)" OFF)

option(GVDB_WITH_CUDA "Enable NVIDIA CUDA acceleration through faiss-gpu" OFF)

cmake_dependent_option(GVDB_WITH_METAL "Enable Apple Metal acceleration" OFF "APPLE" OFF)

option(GVDB_WARNINGS_AS_ERRORS "Treat compiler warnings in GVDB targets as errors" OFF)

option(GVDB_COVERAGE "Instrument GVDB targets for Clang source-based coverage" OFF)

set(GVDB_SANITIZERS "" CACHE STRING
    "Value for -fsanitize= applied to GVDB and its dependencies, for example address,undefined")
