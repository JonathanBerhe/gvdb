# Server module: the startup and shutdown steps that every server program
# shares (signal handlers, logger, metrics endpoint, gRPC server).
gvdb_add_library(server
  SOURCES
    bootstrap.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::common
    gvdb::config
    grpc++
)
