# Compute module: query execution.
gvdb_add_library(compute
  SOURCES
    query_executor.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::storage
    gvdb::utils
  PRIVATE_DEPS
    gvdb::index
)
