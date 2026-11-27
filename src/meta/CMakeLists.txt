# Meta module: the cluster metadata that the coordinator keeps and the data
# and query nodes cache (nodes and collections). Header-only.
gvdb_add_library(meta
  PUBLIC_DEPS
    gvdb::core
)
