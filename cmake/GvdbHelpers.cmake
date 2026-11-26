# Helpers that create GVDB-owned targets with the shared settings from
# gvdb_project_options (standard, warnings, coverage).

function(_gvdb_configure_target target)
  target_link_libraries(${target} PRIVATE gvdb_project_options)
  set_target_properties(${target} PROPERTIES COMPILE_WARNING_AS_ERROR ${GVDB_WARNINGS_AS_ERRORS})
endfunction()

# gvdb_add_library(<name> SOURCES <file>... [PUBLIC_DEPS <dep>...] [PRIVATE_DEPS <dep>...])
#
# Creates the static library gvdb_<name> and its alias gvdb::<name>.
function(gvdb_add_library name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "SOURCES;PUBLIC_DEPS;PRIVATE_DEPS")
  set(target gvdb_${name})
  add_library(${target} STATIC ${arg_SOURCES})
  add_library(gvdb::${name} ALIAS ${target})
  target_include_directories(${target} PUBLIC "${PROJECT_SOURCE_DIR}/include")
  target_link_libraries(${target} PUBLIC ${arg_PUBLIC_DEPS} PRIVATE ${arg_PRIVATE_DEPS})
  _gvdb_configure_target(${target})
endfunction()

# gvdb_add_executable(<name> SOURCES <file>... DEPS <dep>... [INCLUDE_DIRS <dir>...])
#
# Creates a program in <build>/bin.
function(gvdb_add_executable name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "SOURCES;DEPS;INCLUDE_DIRS")
  add_executable(${name} ${arg_SOURCES})
  target_include_directories(${name} PRIVATE ${arg_INCLUDE_DIRS})
  target_link_libraries(${name} PRIVATE ${arg_DEPS})
  set_target_properties(${name} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
  _gvdb_configure_target(${name})
endfunction()

# gvdb_add_test(<name> SOURCES <file>... DEPS <dep>... TIMEOUT <seconds>
#               LABELS <label>... [INCLUDE_DIRS <dir>...] [WORKING_DIRECTORY <dir>])
#
# Creates a doctest executable and registers it with CTest under the same name.
function(gvdb_add_test name)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "TIMEOUT;WORKING_DIRECTORY" "SOURCES;DEPS;LABELS;INCLUDE_DIRS")
  if(NOT arg_TIMEOUT OR NOT arg_LABELS)
    message(FATAL_ERROR "gvdb_add_test(${name}): TIMEOUT and LABELS are required")
  endif()

  add_executable(${name} ${arg_SOURCES})
  target_include_directories(${name} PRIVATE ${arg_INCLUDE_DIRS})
  target_link_libraries(${name} PRIVATE ${arg_DEPS} doctest::doctest_with_main)
  _gvdb_configure_target(${name})

  add_test(NAME ${name} COMMAND ${name})
  set_tests_properties(${name} PROPERTIES TIMEOUT ${arg_TIMEOUT} LABELS "${arg_LABELS}")
  if(arg_WORKING_DIRECTORY)
    set_tests_properties(${name} PROPERTIES WORKING_DIRECTORY "${arg_WORKING_DIRECTORY}")
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
