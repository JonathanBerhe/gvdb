# Utils module: logging, metrics, configuration, thread pool and server
# bootstrap.
gvdb_add_library(utils
  SOURCES
    logger.cpp
    thread_pool.cpp
    metrics.cpp
    config.cpp
    server_bootstrap.cpp
    env_flags.cpp
    query_cache.cpp
    audit_logger.cpp
    checksum.cpp
    json_util.cpp
  PUBLIC_DEPS
    gvdb::core
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
  PRIVATE_DEPS
    yaml-cpp
    xxHash::xxhash
    grpc++
)
