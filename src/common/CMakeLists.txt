# Common module: logging, metrics, timers, the thread pool, JSON helpers and
# file helpers.
gvdb_add_library(common
  SOURCES
    logger.cpp
    metrics.cpp
    thread_pool.cpp
    json_util.cpp
    file_util.cpp
  PUBLIC_DEPS
    gvdb::core
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
)
