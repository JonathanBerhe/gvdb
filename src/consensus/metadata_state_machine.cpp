#include "consensus/metadata_state_machine.h"
#include "utils/logger.h"
#include <cstring>

namespace gvdb {
namespace consensus {

// Import NuRaft types
using nuraft::ulong;

MetadataStateMachine::MetadataStateMachine(MetadataStore* metadata_store)
    : metadata_store_(metadata_store) {
  if (!metadata_store_) {
    throw std::invalid_argument("MetadataStore pointer cannot be null");
  }
  utils::Logger::Instance().Info("MetadataStateMachine initialized");
}

// ============================================================================
// nuraft::state_machine interface implementation
// ============================================================================

nuraft::ptr<nuraft::buffer> MetadataStateMachine::commit(
    const ulong log_idx,
    nuraft::buffer& data) {

  // Deserialize the metadata operation
  MetadataOp op = DeserializeMetadataOp(data);

  // Apply the operation to the metadata store
  auto status = metadata_store_->Apply(op);

  if (!status.ok()) {
    utils::Logger::Instance().Error(
        "Failed to apply metadata operation at log index {}: {}",
        log_idx, status.ToString());
    // Return error buffer
    auto result = nuraft::buffer::alloc(sizeof(uint8_t));
    result->put(static_cast<uint8_t>(0));  // 0 = error
    result->pos(0);
    return result;
  }

  // Update last committed index
  {
    std::lock_guard<std::mutex> lock(mutex_);
    last_committed_idx_ = log_idx;
  }

  utils::Logger::Instance().Debug(
      "Applied metadata operation (type={}, ts={}) at log index {}",
      static_cast<int>(op.type), op.timestamp, log_idx);

  // Return success buffer
  auto result = nuraft::buffer::alloc(sizeof(uint8_t));
  result->put(static_cast<uint8_t>(1));  // 1 = success
  result->pos(0);
  return result;
}

void MetadataStateMachine::create_snapshot(
    nuraft::snapshot& s,
    nuraft::async_result<bool>::handler_type& when_done) {

  utils::Logger::Instance().Info(
      "Creating snapshot at log index {} (term {})",
      s.get_last_log_idx(), s.get_last_log_term());

  // For now, we create a simple snapshot
  // In production, this would serialize the entire metadata store state
  {
    std::lock_guard<std::mutex> lock(mutex_);
    last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(
        s.get_last_log_idx(),
        s.get_last_log_term(),
        s.get_last_config());
  }

  nuraft::ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);

  utils::Logger::Instance().Info("Snapshot created successfully");
}

bool MetadataStateMachine::apply_snapshot(nuraft::snapshot& s) {
  std::lock_guard<std::mutex> lock(mutex_);

  utils::Logger::Instance().Info(
      "Applying snapshot at log index {} (term {})",
      s.get_last_log_idx(), s.get_last_log_term());

  // Update snapshot and committed index
  last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(
      s.get_last_log_idx(),
      s.get_last_log_term(),
      s.get_last_config());

  last_committed_idx_ = s.get_last_log_idx();

  utils::Logger::Instance().Info("Snapshot applied successfully");
  return true;
}

int MetadataStateMachine::read_logical_snp_obj(
    nuraft::snapshot& s,
    void*& user_snp_ctx,
    ulong obj_id,
    nuraft::ptr<nuraft::buffer>& data_out,
    bool& is_last_obj) {

  // For now, we don't support logical snapshots
  // In production, this would serialize the metadata store state in chunks
  data_out = nullptr;
  is_last_obj = true;
  return 0;
}

void MetadataStateMachine::save_logical_snp_obj(
    nuraft::snapshot& s,
    ulong& obj_id,
    nuraft::buffer& data,
    bool is_first_obj,
    bool is_last_obj) {

  // For now, we don't support logical snapshots
  // In production, this would deserialize and restore metadata state
}

// ============================================================================
// Serialization helpers
// ============================================================================

nuraft::ptr<nuraft::buffer> MetadataStateMachine::SerializeMetadataOp(
    const MetadataOp& op) {

  // Calculate total size
  // Format: [type:1byte][timestamp:8bytes][data_size:4bytes][data:N bytes]
  size_t total_size = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(int32_t) + op.data.size();

  auto buf = nuraft::buffer::alloc(total_size);

  // Write type
  buf->put(static_cast<uint8_t>(op.type));

  // Write timestamp
  buf->put(static_cast<uint64_t>(op.timestamp));

  // Write data size
  buf->put(static_cast<int32_t>(op.data.size()));

  // Write data
  if (!op.data.empty()) {
    buf->put_raw(reinterpret_cast<const nuraft::byte*>(op.data.data()), op.data.size());
  }

  buf->pos(0);  // Reset position for reading
  return buf;
}

MetadataOp MetadataStateMachine::DeserializeMetadataOp(nuraft::buffer& buf) {
  MetadataOp op;

  buf.pos(0);  // Reset position

  // Read type
  op.type = static_cast<OpType>(buf.get_byte());

  // Read timestamp
  op.timestamp = buf.get_ulong();

  // Read data size
  int32_t data_size = buf.get_int();

  // Read data (safe copy using vector::assign)
  if (data_size > 0) {
    const void* ptr = buf.get_raw(data_size);
    if (ptr) {
      const uint8_t* byte_ptr = static_cast<const uint8_t*>(ptr);
      op.data.assign(byte_ptr, byte_ptr + data_size);
    }
  }

  return op;
}

} // namespace consensus
} // namespace gvdb
