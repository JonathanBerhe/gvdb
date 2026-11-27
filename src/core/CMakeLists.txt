# Core module: types, status, vectors, metadata and filters. Depends only on
# abseil.
gvdb_add_library(core
  SOURCES
    vector.cpp
    metadata.cpp
    filter.cpp
    sparse_vector.cpp
    type_names.cpp
  PUBLIC_DEPS
    absl::status
    absl::statusor
    absl::strings
  PRIVATE_DEPS
    absl::str_format
)
