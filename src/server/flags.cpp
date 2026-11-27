// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "server/flags.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <utility>

namespace gvdb::server {

namespace {

constexpr char kHelpFlag[] = "--help";
constexpr char kHelpText[] = "Show this help message";

// The flag and its value word as the usage text shows them.
std::string Synopsis(const std::string& name, const std::string& value_name) {
  return value_name.empty() ? name : name + " " + value_name;
}

}  // namespace

std::vector<std::string> SplitList(const std::string& list) {
  std::vector<std::string> items;
  std::size_t start = 0;
  std::size_t end = list.find(',');
  while (end != std::string::npos) {
    items.push_back(list.substr(start, end - start));
    start = end + 1;
    end = list.find(',', start);
  }
  items.push_back(list.substr(start));
  return items;
}

void FlagSet::AddString(std::string name, std::string value_name, std::string help,
                        std::string* target, std::string env) {
  auto assign = [target](const std::string& value) { *target = value; };
  flags_.push_back(
      {std::move(name), std::move(value_name), std::move(help), std::move(env), assign, assign});
}

void FlagSet::AddInt(std::string name, std::string value_name, std::string help, int* target) {
  AddValue(std::move(name), std::move(value_name), std::move(help),
           [target](const std::string& value) { *target = std::stoi(value); });
}

void FlagSet::AddSize(std::string name, std::string value_name, std::string help,
                      std::size_t* target) {
  AddValue(std::move(name), std::move(value_name), std::move(help),
           [target](const std::string& value) { *target = std::stoull(value); });
}

void FlagSet::AddList(std::string name, std::string value_name, std::string help,
                      std::vector<std::string>* target, std::string env) {
  flags_.push_back({std::move(name), std::move(value_name), std::move(help), std::move(env),
                    [target](const std::string& value) {
                      for (auto& item : SplitList(value)) target->push_back(std::move(item));
                    },
                    [target](const std::string& value) { *target = SplitList(value); }});
}

void FlagSet::AddValue(std::string name, std::string value_name, std::string help,
                       std::function<void(const std::string&)> set) {
  flags_.push_back(
      {std::move(name), std::move(value_name), std::move(help), "", std::move(set), nullptr});
}

void FlagSet::AddSwitch(std::string name, std::string help, std::function<void()> set) {
  flags_.push_back({std::move(name), "", std::move(help), "",
                    [set = std::move(set)](const std::string&) { set(); }, nullptr});
}

const FlagSet::Flag* FlagSet::Find(const std::string& name) const {
  auto it = std::ranges::find_if(flags_, [&](const Flag& flag) { return flag.name == name; });
  return it == flags_.end() ? nullptr : &*it;
}

std::optional<int> FlagSet::Parse(int argc, char** argv) {
  const std::string program = argc > 0 ? argv[0] : "";
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == kHelpFlag || arg == "-h") {
      PrintUsage(std::cout, program);
      return 0;
    }
    const Flag* flag = Find(arg);
    const bool takes_value = flag != nullptr && !flag->value_name.empty();
    if (flag == nullptr || (takes_value && i + 1 >= argc)) {
      std::cerr << "Unknown argument: " << arg << std::endl;
      PrintUsage(std::cout, program);
      return 1;
    }
    flag->set(takes_value ? std::string(argv[++i]) : std::string());
  }

  for (const Flag& flag : flags_) {
    if (flag.env.empty()) continue;
    const char* value = std::getenv(flag.env.c_str());
    if (value != nullptr && value[0] != '\0') flag.set_from_env(value);
  }
  return std::nullopt;
}

void FlagSet::PrintUsage(std::ostream& out, const std::string& program) const {
  std::size_t width = std::string(kHelpFlag).size();
  for (const Flag& flag : flags_) {
    width = std::max(width, Synopsis(flag.name, flag.value_name).size());
  }
  const std::string indent(2 + width + 2, ' ');

  auto print = [&](const std::string& synopsis, const std::string& help) {
    std::string line = "  " + synopsis;
    line.resize(indent.size(), ' ');
    std::size_t start = 0;
    for (std::size_t end = help.find('\n'); end != std::string::npos;
         end = help.find('\n', start)) {
      out << line << help.substr(start, end - start) << "\n";
      line = indent;
      start = end + 1;
    }
    out << line << help.substr(start) << "\n";
  };

  out << "Usage: " << program << " [options]\n"
      << "Options:\n";
  for (const Flag& flag : flags_) {
    print(Synopsis(flag.name, flag.value_name), flag.help);
  }
  print(kHelpFlag, kHelpText);
}

}  // namespace gvdb::server
