// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Private to the consensus module.

#pragma once

#include <libnuraft/nuraft.hxx>

#include <cstddef>
#include <string>

namespace gvdb {
namespace consensus {

// NuRaft logger adapter to use our logging system
class NuRaftLoggerAdapter : public nuraft::logger {
 public:
  NuRaftLoggerAdapter() = default;

  void put_details(int level, const char* source_file, const char* func_name,
                   size_t line_number, const std::string& msg) override;
  void set_level(int level) override;
  int get_level() override;

 private:
  int level_ = 4;  // INFO by default
};

}  // namespace consensus
}  // namespace gvdb
