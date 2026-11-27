// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace gvdb {
namespace server {

// Splits a comma-separated list: "a,b" gives {"a", "b"}. Every comma starts
// a new item, so "" gives {""} and "a," gives {"a", ""}.
std::vector<std::string> SplitList(const std::string& list);

// The command-line flags of one server program. A flag can name an
// environment variable. When that variable is set and not empty, its value
// replaces the value from the command line.
class FlagSet {
 public:
  // A flag with a value: `--name VALUE`. `value_name` is the VALUE word in
  // the usage text. A '\n' in `help` starts a new usage line.
  void AddString(std::string name, std::string value_name, std::string help,
                 std::string* target, std::string env = "");
  void AddInt(std::string name, std::string value_name, std::string help, int* target);
  void AddSize(std::string name, std::string value_name, std::string help,
               std::size_t* target);
  // A comma-separated list. Each use of the flag appends its items. The
  // environment variable replaces the whole list.
  void AddList(std::string name, std::string value_name, std::string help,
               std::vector<std::string>* target, std::string env = "");
  // A flag with a value that `set` stores.
  void AddValue(std::string name, std::string value_name, std::string help,
                std::function<void(const std::string&)> set);
  // A flag without a value.
  void AddSwitch(std::string name, std::string help, std::function<void()> set);

  // Reads the flags in argv, then the environment variables. Returns
  // nullopt when the program must run. Otherwise returns the exit code:
  // 0 after --help or -h, 1 after an unknown flag or a flag without its
  // value. Both cases print the usage text. A value that is not a number
  // for an integer flag throws std::invalid_argument.
  std::optional<int> Parse(int argc, char** argv);

  void PrintUsage(std::ostream& out, const std::string& program) const;

 private:
  struct Flag {
    std::string name;
    std::string value_name;  // Empty for a switch.
    std::string help;
    std::string env;
    std::function<void(const std::string&)> set;
    std::function<void(const std::string&)> set_from_env;
  };

  const Flag* Find(const std::string& name) const;

  std::vector<Flag> flags_;
};

}  // namespace server
}  // namespace gvdb
