// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/status.h"

#include <libnuraft/nuraft.hxx>
#include <memory>
#include <mutex>

namespace gvdb {
namespace consensus {

// Import NuRaft types
using nuraft::ulong;

// NuRaft state machine implementation for metadata operations
// Implements the nuraft::state_machine interface
//
// Thread-safe: NuRaft guarantees single-threaded access to commit()
class MetadataStateMachine : public nuraft::state_machine {
 public:
  MetadataStateMachine();
  ~MetadataStateMachine() override = default;

  // ============================================================================
  // nuraft::state_machine interface
  // ============================================================================

  // Apply a log entry to the state machine
  // Called by NuRaft when a log entry is committed
  // Single-threaded: NuRaft guarantees only one thread calls this at a time
  nuraft::ptr<nuraft::buffer> commit(
      const ulong log_idx,
      nuraft::buffer& data) override;

  // Pre-commit hook (optional, we don't use it)
  nuraft::ptr<nuraft::buffer> pre_commit(
      const ulong /*log_idx*/,
      nuraft::buffer& /*data*/) override {
    return nullptr;  // Not used
  }

  // Rollback a pre-committed entry (not used since we don't use pre_commit)
  void rollback(const ulong /*log_idx*/, nuraft::buffer& /*data*/) override {
    // Not used
  }

  // Read the committed index
  // This tells NuRaft which log entries have been applied
  ulong last_commit_index() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_committed_idx_;
  }

  // Create a snapshot of the current state
  // Called periodically by NuRaft to compact the log
  void create_snapshot(
      nuraft::snapshot& s,
      nuraft::async_result<bool>::handler_type& when_done) override;

  // Apply a snapshot to the state machine
  // Called when a follower needs to catch up from a snapshot
  bool apply_snapshot(nuraft::snapshot& s) override;

  // Read the last snapshot metadata
  nuraft::ptr<nuraft::snapshot> last_snapshot() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_snapshot_;
  }

  // Get the logical snapshot object
  int read_logical_snp_obj(
      nuraft::snapshot& s,
      void*& user_snp_ctx,
      ulong obj_id,
      nuraft::ptr<nuraft::buffer>& data_out,
      bool& is_last_obj) override;

  // Save a logical snapshot object
  void save_logical_snp_obj(
      nuraft::snapshot& s,
      ulong& obj_id,
      nuraft::buffer& data,
      bool is_first_obj,
      bool is_last_obj) override;

  // Free user snapshot context
  void free_user_snp_ctx(void*& /*user_snp_ctx*/) override {
    // Not used - we handle cleanup automatically
  }

 private:
  // Snapshot management
  mutable std::mutex mutex_;
  uint64_t last_committed_idx_ = 0;
  nuraft::ptr<nuraft::snapshot> last_snapshot_;
};

} // namespace consensus
} // namespace gvdb