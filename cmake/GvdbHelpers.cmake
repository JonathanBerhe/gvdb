# Helpers that create GVDB-owned targets with the shared settings from
# gvdb_project_options (standard, warnings, coverage).

function(_gvdb_configure_target target)
  target_link_libraries(${target} PRIVATE gvdb_project_options)
  set_target_properties(${target} PROPERTIES COMPILE_WARNING_AS_ERROR ${GVDB_WARNINGS_AS_ERRORS})
endfunction()

# gvdb_add_library(<name> [SOURCES <file>...] [PUBLIC_DEPS <dep>...] [PRIVATE_DEPS <dep>...])
#
# Creates the static library gvdb_<name> and its alias gvdb::<name>. Without
# SOURCES, the library is header-only: an INTERFACE target that gives the
# include directory and PUBLIC_DEPS to the targets that link it. Every library
# shares one include root, src/, so a header is included by its path below
# src/ (for example "storage/segment/segment.h").
function(gvdb_add_library name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "SOURCES;PUBLIC_DEPS;PRIVATE_DEPS")
  set(target gvdb_${name})
  if(NOT arg_SOURCES)
    if(arg_PRIVATE_DEPS)
      message(FATAL_ERROR "gvdb_add_library(${name}): a header-only library has no PRIVATE_DEPS")
    endif()
    add_library(${target} INTERFACE)
    add_library(gvdb::${name} ALIAS ${target})
    target_include_directories(${target} INTERFACE "${PROJECT_SOURCE_DIR}/src")
    target_link_libraries(${target} INTERFACE ${arg_PUBLIC_DEPS})
    return()
  endif()
  add_library(${target} STATIC ${arg_SOURCES})
  add_library(gvdb::${name} ALIAS ${target})
  target_include_directories(${target} PUBLIC "${PROJECT_SOURCE_DIR}/src")
  target_link_libraries(${target} PUBLIC ${arg_PUBLIC_DEPS} PRIVATE ${arg_PRIVATE_DEPS})
  _gvdb_configure_target(${target})
endfunction()

# gvdb_add_executable(<name> SOURCES <file>... DEPS <dep>...)
#
# Creates a program in <build>/bin.
function(gvdb_add_executable name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "SOURCES;DEPS")
  add_executable(${name} ${arg_SOURCES})
  target_link_libraries(${name} PRIVATE ${arg_DEPS})
  set_target_properties(${name} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
  if(GVDB_STATIC_LIBCXX)
    # With -stdlib=libc++, Clang links libc++ and libc++abi statically.
    target_link_options(${name} PRIVATE -static-libstdc++)
  endif()
  _gvdb_configure_target(${name})
endfunction()

# gvdb_add_test(<name> SOURCES <file>... DEPS <dep>... TIMEOUT <seconds>
#               LABELS <label>... [DISCOVER] [WORKING_DIRECTORY <dir>]
#               [ENVIRONMENT <VAR=value>...] [CUSTOM_MAIN] [SKIP_RETURN_CODE <code>])
#
# Creates a doctest executable and registers it with CTest under the same name.
# With DISCOVER, CTest instead runs each test case of the executable as its own
# test, named "<name>:<test case>", so CTest can run the cases in parallel.
# The build lists the cases after it links the executable. TIMEOUT, LABELS,
# WORKING_DIRECTORY and ENVIRONMENT then apply to each case.
# Every test links gvdb::test_support, so it can include "support/...".
# CUSTOM_MAIN links doctest without its main(), for tests that define their
# own (for example to skip when a required service is not configured).
# SKIP_RETURN_CODE makes CTest report that exit code as skipped. ENVIRONMENT
# sets variables for the test process when CTest runs it.
function(gvdb_add_test name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "CUSTOM_MAIN;DISCOVER" "TIMEOUT;WORKING_DIRECTORY;SKIP_RETURN_CODE" "SOURCES;DEPS;LABELS;ENVIRONMENT")
  if(NOT arg_TIMEOUT OR NOT arg_LABELS)
    message(FATAL_ERROR "gvdb_add_test(${name}): TIMEOUT and LABELS are required")
  endif()

  if(arg_CUSTOM_MAIN)
    set(doctest_target doctest::doctest)
  else()
    set(doctest_target doctest::doctest_with_main)
  endif()

  add_executable(${name} ${arg_SOURCES})
  target_link_libraries(${name} PRIVATE ${arg_DEPS} ${doctest_target})
  if(TARGET gvdb::test_support)
    target_link_libraries(${name} PRIVATE gvdb::test_support)
  endif()
  _gvdb_configure_target(${name})

  if(arg_DISCOVER)
    set(properties TIMEOUT ${arg_TIMEOUT} LABELS "${arg_LABELS}")
    if(arg_ENVIRONMENT)
      list(APPEND properties ENVIRONMENT "${arg_ENVIRONMENT}")
    endif()
    set(working_directory "${CMAKE_CURRENT_BINARY_DIR}")
    if(arg_WORKING_DIRECTORY)
      set(working_directory "${arg_WORKING_DIRECTORY}")
    endif()
    doctest_discover_tests(${name}
      TEST_PREFIX "${name}:"
      WORKING_DIRECTORY "${working_directory}"
      PROPERTIES ${properties})
    return()
  endif()

  add_test(NAME ${name} COMMAND ${name})
  set_tests_properties(${name} PROPERTIES TIMEOUT ${arg_TIMEOUT} LABELS "${arg_LABELS}")
  if(arg_WORKING_DIRECTORY)
    set_tests_properties(${name} PROPERTIES WORKING_DIRECTORY "${arg_WORKING_DIRECTORY}")
  endif()
  if(DEFINED arg_SKIP_RETURN_CODE)
    set_tests_properties(${name} PROPERTIES SKIP_RETURN_CODE ${arg_SKIP_RETURN_CODE})
  endif()
  if(arg_ENVIRONMENT)
    set_tests_properties(${name} PROPERTIES ENVIRONMENT "${arg_ENVIRONMENT}")
  endif()
endfunction()

# gvdb_add_proto_library(<target> PROTOS <file>...)
#
# Generates the C++ protobuf and gRPC code for the given files and builds it
# as a static library. Consumers include the generated headers by file name,
# for example "vectordb.grpc.pb.h". The include directory is SYSTEM, so
# warnings in generated code never reach GVDB targets.
function(gvdb_add_proto_library target)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "PROTOS")
  set(out_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
  file(MAKE_DIRECTORY "${out_dir}")

  set(proto_files)
  foreach(proto IN LISTS arg_PROTOS)
    cmake_path(ABSOLUTE_PATH proto OUTPUT_VARIABLE proto_abs)
    list(APPEND proto_files "${proto_abs}")
  endforeach()

  set(sources)
  foreach(proto_abs IN LISTS proto_files)
    cmake_path(GET proto_abs PARENT_PATH proto_dir)
    cmake_path(GET proto_abs STEM stem)
    set(outputs
      "${out_dir}/${stem}.pb.cc" "${out_dir}/${stem}.pb.h"
      "${out_dir}/${stem}.grpc.pb.cc" "${out_dir}/${stem}.grpc.pb.h")
    # Each file depends on all files of the library, because they may import
    # each other.
    add_custom_command(
      OUTPUT ${outputs}
      COMMAND protoc
        --cpp_out=${out_dir}
        --grpc_out=${out_dir}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
        -I ${proto_dir}
        ${proto_abs}
      DEPENDS ${proto_files} protoc grpc_cpp_plugin
      COMMENT "Generating C++ protobuf and gRPC code for ${stem}.proto"
      VERBATIM
    )
    list(APPEND sources ${outputs})
  endforeach()

  add_library(${target} STATIC ${sources})
  target_include_directories(${target} SYSTEM PUBLIC "${out_dir}")
  target_link_libraries(${target} PUBLIC libprotobuf grpc++)
endfunction()
