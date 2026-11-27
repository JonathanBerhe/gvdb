// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/metadata_state_machine.h"
#include <cstring>
#include "common/logger.h"

namespace gvdb::consensus {

// Import NuRaft types
using nuraft::ulong;

MetadataStateMachine::MetadataStateMachine() {
  common::Logger::Instance().Info("MetadataStateMachine initialized");
}

// ============================================================================
// nuraft::state_machine interface implementation
// ============================================================================

nuraft::ptr<nuraft::buffer> MetadataStateMachine::commit(const ulong log_idx,
                                                         nuraft::buffer& /*data*/) {
  // Update last committed index
  {
    std::scoped_lock lock(mutex_);
    last_committed_idx_ = log_idx;
  }

  // Return success buffer
  auto result = nuraft::buffer::alloc(sizeof(uint8_t));
  result->put(static_cast<uint8_t>(1));  // 1 = success
  result->pos(0);
  return result;
}

void MetadataStateMachine::create_snapshot(nuraft::snapshot& s,
                                           nuraft::async_result<bool>::handler_type& when_done) {
  common::Logger::Instance().Info("Creating snapshot at log index {} (term {})",
                                  s.get_last_log_idx(), s.get_last_log_term());

  // For now, we create a simple snapshot
  // In production, this would serialize the entire metadata store state
  {
    std::scoped_lock lock(mutex_);
    last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(s.get_last_log_idx(), s.get_last_log_term(),
                                                      s.get_last_config());
  }

  nuraft::ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);

  common::Logger::Instance().Info("Snapshot created successfully");
}

bool MetadataStateMachine::apply_snapshot(nuraft::snapshot& s) {
  std::scoped_lock lock(mutex_);

  common::Logger::Instance().Info("Applying snapshot at log index {} (term {})",
                                  s.get_last_log_idx(), s.get_last_log_term());

  // Update snapshot and committed index
  last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(s.get_last_log_idx(), s.get_last_log_term(),
                                                    s.get_last_config());

  last_committed_idx_ = s.get_last_log_idx();

  common::Logger::Instance().Info("Snapshot applied successfully");
  return true;
}

int MetadataStateMachine::read_logical_snp_obj(nuraft::snapshot& /*s*/, void*& /*user_snp_ctx*/,
                                               ulong /*obj_id*/,
                                               nuraft::ptr<nuraft::buffer>& data_out,
                                               bool& is_last_obj) {
  // For now, we don't support logical snapshots
  // In production, this would serialize the metadata store state in chunks
  data_out = nullptr;
  is_last_obj = true;
  return 0;
}

void MetadataStateMachine::save_logical_snp_obj(nuraft::snapshot& /*s*/, ulong& /*obj_id*/,
                                                nuraft::buffer& /*data*/, bool /*is_first_obj*/,
                                                bool /*is_last_obj*/) {
  // For now, we don't support logical snapshots
  // In production, this would deserialize and restore metadata state
}

}  // namespace gvdb::consensus
