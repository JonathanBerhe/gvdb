// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The flag table of the server programs.

#include <doctest/doctest.h>

#include <cstdlib>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "server/flags.h"

namespace gvdb {
namespace server {
namespace {

// Parses `args` after a program name.
std::optional<int> ParseArgs(FlagSet& flags, std::vector<std::string> args) {
  args.insert(args.begin(), "program");
  std::vector<char*> argv;
  for (auto& arg : args) argv.push_back(arg.data());
  return flags.Parse(static_cast<int>(argv.size()), argv.data());
}

// Sets an environment variable for the life of the object.
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) { ::setenv(name, value, 1); }
  ~ScopedEnv() { ::unsetenv(name_); }

 private:
  const char* name_;
};

}  // namespace

TEST_CASE("SplitList splits at every comma") {
  CHECK_EQ(SplitList("a,b,c"), std::vector<std::string>{"a", "b", "c"});
  CHECK_EQ(SplitList("a"), std::vector<std::string>{"a"});
  CHECK_EQ(SplitList(""), std::vector<std::string>{""});
  CHECK_EQ(SplitList("a,"), std::vector<std::string>{"a", ""});
}

TEST_CASE("FlagSet stores values and keeps the defaults of flags that are not given") {
  std::string address = "0.0.0.0:1";
  std::string data_dir = "/default";
  int node_id = 7;
  size_t memory_gb = 8;
  FlagSet flags;
  flags.AddString("--bind-address", "ADDR", "", &address);
  flags.AddString("--data-dir", "PATH", "", &data_dir);
  flags.AddInt("--node-id", "ID", "", &node_id);
  flags.AddSize("--memory-limit-gb", "SIZE", "", &memory_gb);

  CHECK_FALSE(ParseArgs(flags, {"--bind-address", "host:2", "--node-id", "42",
                                "--memory-limit-gb", "16"}));
  CHECK_EQ(address, "host:2");
  CHECK_EQ(data_dir, "/default");
  CHECK_EQ(node_id, 42);
  CHECK_EQ(memory_gb, 16u);
}

TEST_CASE("FlagSet list flags append the items of each use") {
  std::vector<std::string> coordinators;
  FlagSet flags;
  flags.AddList("--coordinator", "ADDR", "", &coordinators);

  CHECK_FALSE(ParseArgs(flags, {"--coordinator", "a:1,b:2", "--coordinator", "c:3"}));
  CHECK_EQ(coordinators, std::vector<std::string>{"a:1", "b:2", "c:3"});
}

TEST_CASE("FlagSet runs value callbacks and switches in command-line order") {
  std::vector<std::string> peers;
  bool single_node = true;
  FlagSet flags;
  flags.AddValue("--raft-peers", "PEERS", "", [&](const std::string& value) {
    peers = SplitList(value);
    single_node = false;
  });
  flags.AddSwitch("--single-node", "", [&] { single_node = true; });

  CHECK_FALSE(ParseArgs(flags, {"--raft-peers", "1:a:1,2:b:1"}));
  CHECK_EQ(peers.size(), 2u);
  CHECK_FALSE(single_node);

  CHECK_FALSE(ParseArgs(flags, {"--raft-peers", "1:a:1", "--single-node"}));
  CHECK(single_node);
}

TEST_CASE("FlagSet environment variables replace the command-line value when set") {
  std::string address = "default:1";
  std::vector<std::string> peers;
  FlagSet flags;
  flags.AddString("--bind-address", "ADDR", "", &address, "GVDB_FLAGS_TEST_ADDRESS");
  flags.AddList("--peers", "PEERS", "", &peers, "GVDB_FLAGS_TEST_PEERS");

  SUBCASE("a set variable wins, and a list variable replaces the whole list") {
    ScopedEnv env_address("GVDB_FLAGS_TEST_ADDRESS", "env:2");
    ScopedEnv env_peers("GVDB_FLAGS_TEST_PEERS", "x:1,y:2");
    CHECK_FALSE(ParseArgs(flags, {"--bind-address", "cli:3", "--peers", "cli:4"}));
    CHECK_EQ(address, "env:2");
    CHECK_EQ(peers, std::vector<std::string>{"x:1", "y:2"});
  }
  SUBCASE("an empty variable keeps the command-line value") {
    ScopedEnv env_address("GVDB_FLAGS_TEST_ADDRESS", "");
    CHECK_FALSE(ParseArgs(flags, {"--bind-address", "cli:3"}));
    CHECK_EQ(address, "cli:3");
  }
}

TEST_CASE("FlagSet returns the exit code for help and for a bad command line") {
  int port = 0;
  FlagSet flags;
  flags.AddInt("--port", "PORT", "", &port);

  CHECK_EQ(ParseArgs(flags, {"--help"}), 0);
  CHECK_EQ(ParseArgs(flags, {"-h"}), 0);
  CHECK_EQ(ParseArgs(flags, {"--no-such-flag"}), 1);
  // A flag that takes a value cannot be the last argument.
  CHECK_EQ(ParseArgs(flags, {"--port"}), 1);
  CHECK_THROWS_AS(ParseArgs(flags, {"--port", "not-a-number"}), std::invalid_argument);
}

TEST_CASE("FlagSet usage text aligns the help column and indents extra help lines") {
  std::string value;
  FlagSet flags;
  flags.AddString("--a", "X", "First line\nsecond line", &value);
  flags.AddSwitch("--long-switch", "A switch", [] {});

  std::ostringstream out;
  flags.PrintUsage(out, "prog");
  CHECK_EQ(out.str(),
           "Usage: prog [options]\n"
           "Options:\n"
           "  --a X          First line\n"
           "                 second line\n"
           "  --long-switch  A switch\n"
           "  --help         Show this help message\n");
}

}  // namespace server
}  // namespace gvdb
