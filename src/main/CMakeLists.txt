# Server programs, one per role.

gvdb_add_executable(gvdb-single-node
  SOURCES single_node_main.cpp
  DEPS
    gvdb::consensus
    gvdb::cluster
    gvdb::network
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::utils
    gvdb::core
    gvdb::proto
    absl::strings
)

gvdb_add_executable(gvdb-coordinator
  SOURCES coordinator_main.cpp
  DEPS
    gvdb::consensus
    gvdb::cluster
    gvdb::network
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::utils
    gvdb::core
    absl::strings
    absl::str_format
)

gvdb_add_executable(gvdb-data-node
  SOURCES data_node_main.cpp
  DEPS
    gvdb::cluster
    gvdb::network
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::utils
    gvdb::core
    gvdb::proto
    absl::strings
)

gvdb_add_executable(gvdb-query-node
  SOURCES query_node_main.cpp
  DEPS
    gvdb::cluster
    gvdb::network
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::utils
    gvdb::core
    gvdb::proto
    absl::strings
)

gvdb_add_executable(gvdb-proxy
  SOURCES proxy_main.cpp
  DEPS
    gvdb::network
    gvdb::cluster
    gvdb::utils
    gvdb::core
    gvdb::proto
    absl::strings
)

install(TARGETS gvdb-single-node gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy
  RUNTIME DESTINATION bin
)
