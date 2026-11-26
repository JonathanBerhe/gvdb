# Core module: types, status, vectors, metadata and filters. Depends only on
# abseil.
gvdb_add_library(core
  SOURCES
    vector.cpp
    status.cpp
    metadata.cpp
    filter.cpp
    sparse_vector.cpp
  PUBLIC_DEPS
    absl::status
    absl::statusor
    absl::strings
)
