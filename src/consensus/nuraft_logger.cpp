// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/nuraft_logger.h"

#include "common/logger.h"

namespace gvdb::consensus {

void NuRaftLoggerAdapter::put_details(int level, const char* /*source_file*/,
                                      const char* /*func_name*/, size_t /*line_number*/,
                                      const std::string& msg) {
  // Map NuRaft log levels to our logger
  if (level <= 2) {  // ERROR, FATAL
    common::Logger::Instance().Error("[NuRaft] {}", msg);
  } else if (level == 3) {  // WARN
    common::Logger::Instance().Warn("[NuRaft] {}", msg);
  } else if (level == 4) {  // INFO
    common::Logger::Instance().Info("[NuRaft] {}", msg);
  } else {  // DEBUG, TRACE
    common::Logger::Instance().Debug("[NuRaft] {}", msg);
  }
}

void NuRaftLoggerAdapter::set_level(int level) { level_ = level; }

int NuRaftLoggerAdapter::get_level() { return level_; }

}  // namespace gvdb::consensus
