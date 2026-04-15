// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifdef GVDB_HAS_METAL

#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#include <Foundation/Foundation.hpp>
#include <Metal/Metal.hpp>

#include "metal_compute.h"

#include <fstream>
#include <sstream>
#include <mutex>

namespace gvdb {
namespace index {
namespace metal {

namespace {

// Read the .metal source file at runtime and compile it.
// This avoids the xcrun metallib build step in CMake.
std::string ReadShaderSource(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) return "";
  std::ostringstream ss;
  ss << file.rdbuf();
  return ss.str();
}

// Kernel names we need from distance.metal
const std::vector<std::string> kKernelNames = {
    "l2_distance", "inner_product", "cosine_distance"};

}  // namespace

MetalCompute::MetalCompute() = default;

MetalCompute::~MetalCompute() {
  for (auto& [name, pso] : pipelines_) {
    if (pso) pso->release();
  }
  if (queue_) queue_->release();
  if (device_) device_->release();
}

bool MetalCompute::IsAvailable() {
  // Trigger full initialization (device + kernel compilation) and return
  // whether it succeeded. This ensures CreateFlatIndex only returns a
  // MetalFlatIndex when the GPU is genuinely usable (device exists AND
  // kernels compile). Without this, a missing .metal file causes silent
  // fallback to zero-distances, producing wrong search results.
  return Instance().initialized_;
}

MetalCompute& MetalCompute::Instance() {
  static MetalCompute instance;
  static std::once_flag flag;
  std::call_once(flag, [&]() { instance.Initialize(); });
  return instance;
}

bool MetalCompute::Initialize() {
  @autoreleasepool {
    // MTLCreateSystemDefaultDevice() returns nil for headless CLI processes.
    // MTLCopyAllDevices() works without WindowServer connection.
    NS::Array* devices = MTL::CopyAllDevices();
    if (!devices || devices->count() == 0) {
      if (devices) devices->release();
      return false;
    }
    device_ = (MTL::Device*)devices->object(0);
    device_->retain();
    devices->release();
    if (!device_) return false;

    queue_ = device_->newCommandQueue();
    if (!queue_) return false;

    initialized_ = CompileKernels();
    return initialized_;
  }
}

bool MetalCompute::CompileKernels() {
  // Compile kernels from source at runtime.
  // Look for distance.metal relative to the binary, then fall back to
  // a compile-time embedded path.
  std::string source;

  // Try paths relative to the executable
  const char* search_paths[] = {
      "kernels/distance.metal",
      "../kernels/distance.metal",
      "../src/index/metal/kernels/distance.metal",
      "src/index/metal/kernels/distance.metal",
  };
  for (const char* p : search_paths) {
    source = ReadShaderSource(p);
    if (!source.empty()) break;
  }

  // Also try from GVDB_METAL_KERNEL_PATH env var
  if (source.empty()) {
    const char* env_path = std::getenv("GVDB_METAL_KERNEL_PATH");
    if (env_path) {
      source = ReadShaderSource(std::string(env_path) + "/distance.metal");
    }
  }

  if (source.empty()) return false;

  NS::Error* error = nullptr;
  auto* src = NS::String::string(source.c_str(), NS::UTF8StringEncoding);
  auto* library = device_->newLibrary(src, nullptr, &error);
  if (!library || error) return false;

  for (const auto& name : kKernelNames) {
    auto* fn_name = NS::String::string(name.c_str(), NS::UTF8StringEncoding);
    auto* fn = library->newFunction(fn_name);
    if (!fn) {
      library->release();
      return false;
    }

    auto* pso = device_->newComputePipelineState(fn, &error);
    fn->release();
    if (!pso || error) {
      library->release();
      return false;
    }

    pipelines_[name] = pso;
  }

  library->release();
  return true;
}

MTL::Buffer* MetalCompute::CreateBuffer(const void* data, size_t bytes) {
  return device_->newBuffer(data, bytes, MTL::ResourceStorageModeShared);
}

MTL::Buffer* MetalCompute::CreateBuffer(size_t bytes) {
  return device_->newBuffer(bytes, MTL::ResourceStorageModeShared);
}

void MetalCompute::ComputeDistances(const std::string& kernel_name,
                                     const float* queries, size_t nq,
                                     MTL::Buffer* vec_buf, size_t nb,
                                     size_t dim,
                                     float* distances) {
  if (!initialized_) return;

  auto it = pipelines_.find(kernel_name);
  if (it == pipelines_.end()) return;

  @autoreleasepool {
    auto* pso = it->second;

    // Only query and distance buffers are per-search (small).
    // Vector buffer is persistent (passed in by caller).
    auto* q_buf = device_->newBuffer(queries, nq * dim * sizeof(float),
                                     MTL::ResourceStorageModeShared);
    auto* d_buf = device_->newBuffer(nq * nb * sizeof(float),
                                     MTL::ResourceStorageModeShared);

    uint32_t dim32 = static_cast<uint32_t>(dim);
    uint32_t nb32 = static_cast<uint32_t>(nb);

    auto* cmd = queue_->commandBuffer();
    auto* enc = cmd->computeCommandEncoder();
    enc->setComputePipelineState(pso);
    enc->setBuffer(q_buf, 0, 0);
    enc->setBuffer(vec_buf, 0, 1);
    enc->setBuffer(d_buf, 0, 2);
    enc->setBytes(&dim32, sizeof(uint32_t), 3);
    enc->setBytes(&nb32, sizeof(uint32_t), 4);

    MTL::Size grid(nq, nb, 1);
    auto w = pso->threadExecutionWidth();
    auto h = pso->maxTotalThreadsPerThreadgroup() / w;
    MTL::Size group(w, h > 0 ? h : 1, 1);

    enc->dispatchThreads(grid, group);
    enc->endEncoding();
    cmd->commit();
    cmd->waitUntilCompleted();

    std::memcpy(distances, d_buf->contents(), nq * nb * sizeof(float));

    q_buf->release();
    d_buf->release();
  }
}

}  // namespace metal
}  // namespace index
}  // namespace gvdb

#endif  // GVDB_HAS_METAL
