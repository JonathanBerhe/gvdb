# Consensus module - Raft-based distributed consensus

add_library(gvdb_consensus STATIC
  timestamp_oracle.cpp
  metadata_store.cpp
  raft_node.cpp
)

target_include_directories(gvdb_consensus
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(gvdb_consensus
  PUBLIC
    gvdb_core
    gvdb_cluster
    absl::status
    absl::statusor
    absl::strings
    absl::synchronization
  PRIVATE
    gvdb_utils
)

# C++ standard
target_compile_features(gvdb_consensus PUBLIC cxx_std_17)

# Warnings
target_compile_options(gvdb_consensus PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra>
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
)
