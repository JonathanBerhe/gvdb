# Common module: logging, metrics, timers, the thread pool, JSON helpers and
# environment flags.
gvdb_add_library(common
  SOURCES
    logger.cpp
    metrics.cpp
    thread_pool.cpp
    json_util.cpp
    env_flags.cpp
  PUBLIC_DEPS
    gvdb::core
    spdlog::spdlog
    prometheus-cpp::core
    prometheus-cpp::pull
)
