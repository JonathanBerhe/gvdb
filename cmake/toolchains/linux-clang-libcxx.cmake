# Linux toolchain: Clang 22 with LLVM's C++ standard library (libc++) and
# the lld linker. The packages come from apt.llvm.org (suite
# llvm-toolchain-<codename>-22): clang-22, lld-22, libc++-22-dev,
# libc++abi-22-dev and libomp-22-dev.
#
# CUDA 13.4 and later accept Clang 22 with libc++ 22 as the host compiler,
# and Clang 22 is the newest major version that nvcc supports, so the
# version stays at 22 until a CUDA release lists a newer one.
#
# CI, the release image, the CUDA build and the vcpkg triplet for GCS
# (tools/vcpkg/triplets) all use this file, so every C++ object, the
# dependencies included, is built against the same standard library.

set(GVDB_LLVM_VERSION 22 CACHE STRING "Major version of the Clang toolchain")

set(CMAKE_C_COMPILER clang-${GVDB_LLVM_VERSION})
set(CMAKE_CXX_COMPILER clang++-${GVDB_LLVM_VERSION})

# CMake passes CMAKE_CXX_FLAGS to every C++ compile and to the link of
# targets that it links as C++, so libc++ is used on both sides.
set(CMAKE_CXX_FLAGS_INIT "-stdlib=libc++")

set(CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=lld")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=lld")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-fuse-ld=lld")

# CUDA builds (GVDB_WITH_CUDA): nvcc compiles host code with the same Clang and
# libc++. nvcc 13.5 accepts -stdlib=libc++ for a Clang host compiler.
set(CMAKE_CUDA_HOST_COMPILER ${CMAKE_CXX_COMPILER})
set(CMAKE_CUDA_FLAGS_INIT "-stdlib=libc++")
