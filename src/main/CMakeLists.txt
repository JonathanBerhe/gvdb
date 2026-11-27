# Server programs, one per role. Each main reads its flags and wires the
# modules together with the builders in src/server.

gvdb_add_executable(gvdb-single-node
  SOURCES single_node_main.cpp
  DEPS
    gvdb::server
    gvdb::service
    gvdb::cluster
    gvdb::consensus
    gvdb::compute
    gvdb::config
    gvdb::common
    absl::strings
)

gvdb_add_executable(gvdb-coordinator
  SOURCES coordinator_main.cpp
  DEPS
    gvdb::server
    gvdb::service
    gvdb::cluster
    gvdb::consensus
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::config
    gvdb::common
)

gvdb_add_executable(gvdb-data-node
  SOURCES data_node_main.cpp
  DEPS
    gvdb::server
    gvdb::service
    gvdb::cluster
    gvdb::compute
    gvdb::config
    gvdb::common
    gvdb::core
    gvdb::proto
)

gvdb_add_executable(gvdb-query-node
  SOURCES query_node_main.cpp
  DEPS
    gvdb::server
    gvdb::service
    gvdb::cluster
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::common
    gvdb::proto
)

gvdb_add_executable(gvdb-proxy
  SOURCES proxy_main.cpp
  DEPS
    gvdb::server
    gvdb::service
    gvdb::config
    gvdb::common
)

install(TARGETS gvdb-single-node gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy
  RUNTIME DESTINATION bin
)
