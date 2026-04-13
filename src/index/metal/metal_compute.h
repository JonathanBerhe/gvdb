// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#ifdef GVDB_HAS_METAL

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations — avoid exposing Metal types in the header
namespace MTL {
class Device;
class CommandQueue;
class ComputePipelineState;
class Buffer;
}  // namespace MTL

namespace gvdb {
namespace index {
namespace metal {

// Low-level Metal compute wrapper.
// Manages device, command queue, pipeline states, and buffer allocation.
// Thread-safe for dispatch (Metal command buffers are thread-safe).
class MetalCompute {
 public:
  ~MetalCompute();

  // Returns true if a Metal GPU is available on this system.
  static bool IsAvailable();

  // Singleton access. Lazily initializes Metal device and compiles kernels.
  static MetalCompute& Instance();

  // Create a GPU buffer from CPU data. Uses shared storage mode (unified memory).
  MTL::Buffer* CreateBuffer(const void* data, size_t bytes);

  // Create an uninitialized GPU buffer.
  MTL::Buffer* CreateBuffer(size_t bytes);

  // Compute batch distances using pre-allocated vector buffer.
  // vec_buf: persistent Metal buffer holding [nb * dim] floats
  // queries: [nq * dim] float array (copied per call — small)
  // distances: output [nq * nb] float array (caller-allocated)
  void ComputeDistances(const std::string& kernel_name,
                        const float* queries, size_t nq,
                        MTL::Buffer* vec_buf, size_t nb,
                        size_t dim,
                        float* distances);

  // Get the Metal device (for external buffer creation)
  MTL::Device* GetDevice() { return device_; }

 private:
  MetalCompute();
  bool Initialize();
  bool CompileKernels();

  MTL::Device* device_ = nullptr;
  MTL::CommandQueue* queue_ = nullptr;
  std::unordered_map<std::string, MTL::ComputePipelineState*> pipelines_;
  bool initialized_ = false;
};

}  // namespace metal
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_HAS_METAL
