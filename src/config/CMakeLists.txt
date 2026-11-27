# Config module: the YAML configuration file of the server programs. It fills
# the configuration structs that the lower modules define (auth, storage).
gvdb_add_library(config
  SOURCES
    config.cpp
  PUBLIC_DEPS
    gvdb::auth
    gvdb::storage
    absl::status
    absl::statusor
  PRIVATE_DEPS
    gvdb::core
    yaml-cpp
)
