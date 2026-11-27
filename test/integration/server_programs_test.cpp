// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Starts the five server programs as real processes. The tests cover the
// command line, the startup wiring of each program, a collection round trip
// and a clean exit on SIGTERM. The other tests build the services in
// process, so only this test runs the code in src/main.

#include <doctest/doctest.h>
#include <grpcpp/grpcpp.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fcntl.h>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "support/temp_dir.h"
#include "support/wait.h"
#include "vectordb.grpc.pb.h"

extern char** environ;

namespace gvdb {
namespace test {
namespace {

using std::chrono::seconds;

// A TCP port that was free a moment ago. Another process can take it before
// the program binds it, but the window is short.
int FreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  REQUIRE(fd >= 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = 0;
  REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
  socklen_t length = sizeof(addr);
  REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &length) == 0);
  ::close(fd);
  return ntohs(addr.sin_port);
}

std::string LocalAddress(int port) { return "127.0.0.1:" + std::to_string(port); }

// A program in a child process. Its stdout and stderr go to one log file.
// The environment holds no GVDB_ variable, so the flags decide everything.
// The destructor kills a program that still runs.
class Program {
 public:
  Program(const std::string& path, const std::vector<std::string>& args,
          std::filesystem::path log_file)
      : log_file_(std::move(log_file)) {
    std::vector<std::string> argv_storage = {path};
    argv_storage.insert(argv_storage.end(), args.begin(), args.end());
    std::vector<char*> argv;
    for (auto& arg : argv_storage) argv.push_back(arg.data());
    argv.push_back(nullptr);

    std::vector<char*> envp;
    for (char** env = environ; *env != nullptr; ++env) {
      if (std::strncmp(*env, "GVDB_", 5) != 0) envp.push_back(*env);
    }
    envp.push_back(nullptr);

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_file_.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);
    const int error = posix_spawn(&pid_, path.c_str(), &actions, nullptr, argv.data(), envp.data());
    posix_spawn_file_actions_destroy(&actions);
    REQUIRE_MESSAGE(error == 0, "posix_spawn " << path << ": " << std::strerror(error));
  }

  ~Program() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
    }
  }

  Program(const Program&) = delete;
  Program& operator=(const Program&) = delete;

  // Waits for the exit and returns the exit code. Returns -1 when the
  // program does not exit within `timeout` or when a signal ends it.
  int Wait(seconds timeout = seconds(30)) {
    int status = 0;
    const bool exited = WaitFor([&] { return ::waitpid(pid_, &status, WNOHANG) == pid_; }, timeout,
                                std::chrono::milliseconds(20));
    if (!exited) return -1;
    pid_ = -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  }

  // Sends SIGTERM, then waits like Wait(). The TTL sweep loop of the single
  // node and the data node sleeps 30 s and reads the shutdown flag only
  // after each sleep, so their exit can take that long.
  int Stop(seconds timeout = seconds(60)) {
    ::kill(pid_, SIGTERM);
    return Wait(timeout);
  }

  std::string Log() const {
    std::ifstream file(log_file_);
    std::stringstream text;
    text << file.rdbuf();
    return text.str();
  }

 private:
  pid_t pid_ = -1;
  std::filesystem::path log_file_;
};

std::unique_ptr<proto::VectorDBService::Stub> Client(const std::string& address) {
  return proto::VectorDBService::NewStub(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

// True when the program at `address` answers HealthCheck within 30 s.
bool Serves(const std::string& address) {
  auto client = Client(address);
  return WaitFor(
      [&] {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + seconds(1));
        proto::HealthCheckRequest req;
        proto::HealthCheckResponse resp;
        return client->HealthCheck(&ctx, req, &resp).ok();
      },
      seconds(30), std::chrono::milliseconds(100));
}

grpc::Status CreateCollection(proto::VectorDBService::Stub& client, const std::string& name) {
  proto::CreateCollectionRequest req;
  req.set_collection_name(name);
  req.set_dimension(4);
  req.set_metric(proto::CreateCollectionRequest::L2);
  req.set_index_type(proto::CreateCollectionRequest::FLAT);
  proto::CreateCollectionResponse resp;
  grpc::ClientContext ctx;
  return client.CreateCollection(&ctx, req, &resp);
}

// Inserts the vectors (i, 0, 0, 0) with id i for i in 1..5, then checks
// that a search for (1, 0, 0, 0) finds id 1 first.
void InsertAndSearch(proto::VectorDBService::Stub& client, const std::string& name) {
  {
    proto::InsertRequest req;
    req.set_collection_name(name);
    for (int i = 1; i <= 5; ++i) {
      auto* vec = req.add_vectors();
      vec->set_id(i);
      auto* values = vec->mutable_vector();
      values->set_dimension(4);
      for (float value : {static_cast<float>(i), 0.0f, 0.0f, 0.0f}) values->add_values(value);
    }
    proto::InsertResponse resp;
    grpc::ClientContext ctx;
    auto status = client.Insert(&ctx, req, &resp);
    INFO("Insert: " << status.error_message());
    REQUIRE(status.ok());
    CHECK_EQ(resp.inserted_count(), 5);
  }
  {
    proto::SearchRequest req;
    req.set_collection_name(name);
    req.set_top_k(3);
    auto* query = req.mutable_query_vector();
    query->set_dimension(4);
    for (float value : {1.0f, 0.0f, 0.0f, 0.0f}) query->add_values(value);
    proto::SearchResponse resp;
    grpc::ClientContext ctx;
    auto status = client.Search(&ctx, req, &resp);
    INFO("Search: " << status.error_message());
    REQUIRE(status.ok());
    REQUIRE_GT(resp.results_size(), 0);
    CHECK_EQ(resp.results(0).id(), 1);
  }
}

// Stops the program and checks that it exits with 0 after its last
// shutdown step.
void StopCleanly(Program& program, const std::string& name) {
  const int exit_code = program.Stop();
  INFO(name << " log:\n" << program.Log());
  CHECK_EQ(exit_code, 0);
  CHECK_NE(program.Log().find("Shutdown complete. Goodbye!"), std::string::npos);
}

}  // namespace

TEST_CASE("Every server program exits with 0 after --help and with 1 after an unknown flag") {
  ScopedTempDir dir("gvdb_programs");
  for (const std::string path : {GVDB_SINGLE_NODE_BIN, GVDB_COORDINATOR_BIN, GVDB_DATA_NODE_BIN,
                                 GVDB_QUERY_NODE_BIN, GVDB_PROXY_BIN}) {
    CAPTURE(path);
    Program help(path, {"--help"}, dir / "help.log");
    CHECK_EQ(help.Wait(), 0);
    CHECK_NE(help.Log().find("Usage: "), std::string::npos);

    Program unknown(path, {"--no-such-flag"}, dir / "unknown.log");
    CHECK_EQ(unknown.Wait(), 1);
    CHECK_NE(unknown.Log().find("Unknown argument: --no-such-flag"), std::string::npos);
  }
}

TEST_CASE("The single node serves a collection with tiered storage and exits on SIGTERM") {
  ScopedTempDir dir("gvdb_single_node");
  const auto config_file = dir / "gvdb.yaml";
  std::ofstream(config_file) << "storage:\n"
                             << "  object_store:\n"
                             << "    type: filesystem\n"
                             << "    bucket: " << (dir / "objects").string() << "\n";
  std::filesystem::create_directories(dir / "objects");
  const int port = FreePort();

  Program single_node(GVDB_SINGLE_NODE_BIN,
                      {"--config", config_file.string(), "--port", std::to_string(port),
                       "--data-dir", (dir / "data").string(), "--node-id", "1"},
                      dir / "single_node.log");
  {
    INFO("single node log:\n" << single_node.Log());
    REQUIRE(Serves(LocalAddress(port)));
    auto client = Client(LocalAddress(port));
    auto status = CreateCollection(*client, "smoke");
    INFO("CreateCollection: " << status.error_message());
    REQUIRE(status.ok());
    InsertAndSearch(*client, "smoke");
  }
  // The client channel is closed here. A server shutdown waits for every
  // open channel to answer its GOAWAY, which an idle channel can delay.
  StopCleanly(single_node, "single node");
}

// The proxy sends writes to the data nodes and reads to the query nodes,
// which search the data nodes, as in the Helm chart.
TEST_CASE("A coordinator, a data node, a query node and a proxy serve a collection") {
  ScopedTempDir dir("gvdb_cluster");
  const std::string coordinator_address = LocalAddress(FreePort());
  const std::string data_node_address = LocalAddress(FreePort());
  const std::string query_node_address = LocalAddress(FreePort());
  const std::string proxy_address = LocalAddress(FreePort());

  Program coordinator(GVDB_COORDINATOR_BIN,
                      {"--single-node", "--bind-address", coordinator_address, "--data-dir",
                       (dir / "coordinator").string()},
                      dir / "coordinator.log");
  REQUIRE(Serves(coordinator_address));
  Program data_node(GVDB_DATA_NODE_BIN,
                    {"--node-id", "101", "--bind-address", data_node_address, "--coordinator",
                     coordinator_address, "--data-dir", (dir / "data_node").string(),
                     "--drain-wait-seconds", "1"},
                    dir / "data_node.log");
  Program query_node(GVDB_QUERY_NODE_BIN,
                     {"--node-id", "201", "--bind-address", query_node_address, "--coordinator",
                      coordinator_address, "--data-dir", (dir / "query_node").string()},
                     dir / "query_node.log");
  Program proxy(GVDB_PROXY_BIN,
                {"--bind-address", proxy_address, "--coordinators", coordinator_address,
                 "--query-nodes", query_node_address, "--data-dir", (dir / "proxy").string()},
                dir / "proxy.log");
  REQUIRE(Serves(data_node_address));
  REQUIRE(Serves(query_node_address));
  REQUIRE(Serves(proxy_address));

  {
    INFO("coordinator log:\n" << coordinator.Log() << "\ndata node log:\n" << data_node.Log());
    auto client = Client(proxy_address);
    // The coordinator places shards only on data nodes that sent a
    // heartbeat, so the first calls can fail.
    grpc::Status status;
    CHECK(WaitFor([&] { return (status = CreateCollection(*client, "smoke")).ok(); }, seconds(30),
                  std::chrono::milliseconds(200)));
    INFO("CreateCollection: " << status.error_message());
    REQUIRE(status.ok());
    InsertAndSearch(*client, "smoke");
  }

  // Clients first: a program waits for the open channels of its clients
  // when it shuts down.
  StopCleanly(proxy, "proxy");
  StopCleanly(query_node, "query node");
  StopCleanly(data_node, "data node");
  StopCleanly(coordinator, "coordinator");
}

}  // namespace test
}  // namespace gvdb
