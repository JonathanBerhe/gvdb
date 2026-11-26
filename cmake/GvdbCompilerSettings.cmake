# Language standard, warnings and instrumentation.
#
# GVDB_CXX_STANDARD is the only place that names the C++ standard.
# CMAKE_CXX_STANDARD makes the fetched dependencies build at the same
# standard as GVDB, which abseil requires for a consistent ABI.

set(GVDB_CXX_STANDARD 20)

set(CMAKE_CXX_STANDARD ${GVDB_CXX_STANDARD})
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Sanitizers must instrument the dependencies too (gRPC and RocksDB run
# their own threads), so these flags apply to every target configured
# after this point.
if(GVDB_SANITIZERS)
  add_compile_options(-fsanitize=${GVDB_SANITIZERS} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${GVDB_SANITIZERS})
endif()

# Settings for GVDB-owned targets only. The helpers in GvdbHelpers.cmake
# link this target PRIVATE into every library, executable and test.
add_library(gvdb_project_options INTERFACE)

target_compile_features(gvdb_project_options INTERFACE cxx_std_${GVDB_CXX_STANDARD})

target_compile_options(gvdb_project_options INTERFACE
  $<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  $<$<COMPILE_LANG_AND_ID:OBJCXX,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
  # GCC 13 reports false positives inside inlined spdlog and fmt code.
  $<$<COMPILE_LANG_AND_ID:CXX,GNU>:-Wno-array-bounds -Wno-stringop-overflow>
)

# Clang 22 reports __COUNTER__ under -Wpedantic as a C2y extension, and the
# doctest macros use it in every test file. The flag must come after
# -Wpedantic, which turns the warning on again. GCC accepts any -Wno-... flag,
# so the check uses the positive form.
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wc2y-extensions GVDB_KNOWS_WC2Y_EXTENSIONS)
if(GVDB_KNOWS_WC2Y_EXTENSIONS)
  target_compile_options(gvdb_project_options INTERFACE
    $<$<COMPILE_LANGUAGE:CXX,OBJCXX>:-Wno-c2y-extensions>)
endif()

if(GVDB_COVERAGE)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "GVDB_COVERAGE needs Clang or AppleClang (source-based coverage)")
  endif()
  target_compile_options(gvdb_project_options INTERFACE -fprofile-instr-generate -fcoverage-mapping)
  target_link_options(gvdb_project_options INTERFACE -fprofile-instr-generate)
endif()
