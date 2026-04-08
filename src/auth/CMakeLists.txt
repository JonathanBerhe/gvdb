# Auth Module - RBAC and authentication context

add_library(gvdb_auth
  rbac.cpp
  auth_context.cpp
)

target_link_libraries(gvdb_auth
  PUBLIC
    gvdb_utils
    absl::flat_hash_map
    absl::status
    absl::statusor
    absl::strings
)

target_include_directories(gvdb_auth
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)
