# vcpkg triplet for the GCS build (the gcs preset): x64 Linux, static
# libraries, built with the project's toolchain (Clang 22, libc++, lld).
# google-cloud-cpp and the GVDB code link into one program, so both must use
# the same C++ standard library. vcpkg's default x64-linux triplet uses GCC
# and libstdc++.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/../../../cmake/toolchains/linux-clang-libcxx.cmake")
