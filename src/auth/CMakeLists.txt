# Auth module: RBAC, the per-request authentication and audit contexts, and
# the audit log.
gvdb_add_library(auth
  SOURCES
    rbac.cpp
    auth_context.cpp
    audit_context.cpp
    audit_logger.cpp
  PUBLIC_DEPS
    absl::flat_hash_map
    absl::status
    absl::statusor
    absl::strings
  PRIVATE_DEPS
    gvdb::common
)
