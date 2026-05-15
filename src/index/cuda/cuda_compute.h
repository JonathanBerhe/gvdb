// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

namespace gvdb {
namespace index {
namespace cuda {

// True iff compiled with GVDB_HAS_CUDA and at least one CUDA device responds.
// Result is cached after the first call.
bool IsAvailable();

}  // namespace cuda
}  // namespace index
}  // namespace gvdb
