# Server programs, one per role.

gvdb_add_executable(gvdb-single-node
  SOURCES single_node_main.cpp
  DEPS
    gvdb::server
    gvdb::config
    gvdb::service
    gvdb::cluster
    gvdb::consensus
    gvdb::rpc
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::auth
    gvdb::core
    gvdb::proto
    absl::strings
)

gvdb_add_executable(gvdb-coordinator
  SOURCES coordinator_main.cpp
  DEPS
    gvdb::server
    gvdb::config
    gvdb::service
    gvdb::cluster
    gvdb::consensus
    gvdb::rpc
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::auth
    gvdb::common
    gvdb::core
    absl::strings
    absl::str_format
)

gvdb_add_executable(gvdb-data-node
  SOURCES data_node_main.cpp
  DEPS
    gvdb::server
    gvdb::config
    gvdb::service
    gvdb::cluster
    gvdb::rpc
    gvdb::compute
    gvdb::storage
    gvdb::index
    gvdb::auth
    gvdb::common
    gvdb::core
    gvdb::proto
    absl::strings
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
    gvdb::core
    gvdb::proto
    absl::strings
)

gvdb_add_executable(gvdb-proxy
  SOURCES proxy_main.cpp
  DEPS
    gvdb::server
    gvdb::config
    gvdb::service
    gvdb::rpc
    gvdb::auth
    gvdb::common
    gvdb::core
    gvdb::proto
    absl::strings
)

install(TARGETS gvdb-single-node gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy
  RUNTIME DESTINATION bin
)
