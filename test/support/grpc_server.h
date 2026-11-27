// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// An in-process gRPC server on a port that the operating system picks, so
// that test binaries never compete for a fixed port.

#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <initializer_list>
#include <memory>
#include <string>

namespace gvdb::test {

class TestGrpcServer {
 public:
  // Starts a server on localhost with every given service. ok() is false
  // when the server did not start.
  explicit TestGrpcServer(std::initializer_list<grpc::Service*> services) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port_);
    for (auto* service : services) {
      builder.RegisterService(service);
    }
    server_ = builder.BuildAndStart();
  }

  ~TestGrpcServer() { Shutdown(); }

  TestGrpcServer(const TestGrpcServer&) = delete;
  TestGrpcServer& operator=(const TestGrpcServer&) = delete;

  bool ok() const { return server_ != nullptr && port_ > 0; }
  int port() const { return port_; }
  std::string address() const { return "localhost:" + std::to_string(port_); }

  // A new insecure channel to the server.
  std::shared_ptr<grpc::Channel> NewChannel() const {
    return grpc::CreateChannel(address(), grpc::InsecureChannelCredentials());
  }

  // Stops the server. Calls still in flight get one second to finish, then
  // gRPC cancels them. Safe to call more than once.
  void Shutdown() {
    if (server_) {
      server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
      server_.reset();
    }
  }

  grpc::Server* get() const { return server_.get(); }

 private:
  int port_ = 0;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace gvdb::test
