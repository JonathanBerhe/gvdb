cmake_minimum_required(VERSION 3.28...4.5)
project(gvdb VERSION 0.38.0 LANGUAGES CXX) # x-release-please-version

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

include(GvdbOptions)
include(GvdbCompilerSettings)
include(GvdbHelpers)
include(GvdbDependencies)

add_subdirectory(proto)
add_subdirectory(src)

if(GVDB_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()
