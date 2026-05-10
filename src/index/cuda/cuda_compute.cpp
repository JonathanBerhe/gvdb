// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cuda/cuda_compute.h"

#ifdef GVDB_HAS_CUDA
#include <cuda_runtime.h>
#endif

namespace gvdb {
namespace index {
namespace cuda {

bool IsAvailable() {
#ifdef GVDB_HAS_CUDA
  // Cache the probe — cudaGetDeviceCount triggers driver init the first time.
  static const bool available = []() {
    int count = 0;
    return cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
  }();
  return available;
#else
  return false;
#endif
}

}  // namespace cuda
}  // namespace index
}  // namespace gvdb
