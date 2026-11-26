# Auth module: RBAC and the per-request authentication context.
gvdb_add_library(auth
  SOURCES
    rbac.cpp
    auth_context.cpp
  PUBLIC_DEPS
    gvdb::utils
    absl::flat_hash_map
    absl::status
    absl::statusor
    absl::strings
)
