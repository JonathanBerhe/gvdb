# Compute module: query execution and the query result cache.
gvdb_add_library(compute
  SOURCES
    query_executor.cpp
    query_cache.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::storage
    gvdb::common
)
