// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "absl/status/statusor.h"
#include "consensus/gvdb_log_store.h"

#include <libnuraft/nuraft.hxx>

#include <memory>
#include <mutex>
#include <string>

namespace gvdb {
namespace consensus {

/**
 * GVDB implementation of NuRaft's state_mgr interface.
 *
 * Manages:
 * - Cluster configuration (list of servers)
 * - Server state (current term, voted for)
 * - Log store instance
 *
 * State is persisted in RocksDB, with crash recovery.
 *
 * Thread-safe: All operations are protected by mutex.
 */
class GvdbStateManager : public nuraft::state_mgr {
 public:
  // Creates a persistent state manager for the server `server_id` at
  // `endpoint`. The state lives in a RocksDB at `state_path` and the Raft
  // log in a RocksDB at `log_store_path`. Internal when the log store
  // cannot be opened.
  static absl::StatusOr<std::shared_ptr<GvdbStateManager>> Create(int32_t server_id,
                                                                  const std::string& endpoint,
                                                                  const std::string& log_store_path,
                                                                  const std::string& state_path);

  ~GvdbStateManager() override = default;

  // Disable copy and move
  GvdbStateManager(const GvdbStateManager&) = delete;
  GvdbStateManager& operator=(const GvdbStateManager&) = delete;

  // ============================================================================
  // nuraft::state_mgr interface implementation
  // ============================================================================

  /**
   * Load the last saved cluster config.
   * Called on Raft server initialization.
   *
   * @return Cluster configuration (never nullptr)
   */
  nuraft::ptr<nuraft::cluster_config> load_config() override;

  /**
   * Save cluster configuration.
   *
   * @param config Cluster config to save
   */
  void save_config(const nuraft::cluster_config& config) override;

  /**
   * Save server state (current term, voted for).
   *
   * @param state Server state to save
   */
  void save_state(const nuraft::srv_state& state) override;

  /**
   * Load the last saved server state.
   * Called on Raft server initialization.
   *
   * @return Server state, or nullptr if first initialization
   */
  nuraft::ptr<nuraft::srv_state> read_state() override;

  /**
   * Get the log store instance.
   *
   * @return Log store
   */
  nuraft::ptr<nuraft::log_store> load_log_store() override;

  /**
   * Get this server's ID.
   *
   * @return Server ID
   */
  int32_t server_id() override;

  /**
   * System exit handler.
   * Called on abnormal termination.
   *
   * @param exit_code Error code
   */
  void system_exit(const int exit_code) override;

  // ============================================================================
  // GVDB-specific APIs
  // ============================================================================

  /**
   * Get this server's configuration.
   * @return Server config
   */
  nuraft::ptr<nuraft::srv_config> get_srv_config() const { return my_srv_config_; }

 private:
  // Loads or seeds the state. Create() then opens the log store.
  GvdbStateManager(int32_t server_id, const std::string& endpoint, const std::string& state_path);

  // Mutex protecting all data structures
  mutable std::mutex mutex_;

  // State RocksDB directory
  std::string state_db_path_;

  // Server ID
  int32_t server_id_;

  // This server's endpoint
  std::string endpoint_;

  // This server's configuration
  nuraft::ptr<nuraft::srv_config> my_srv_config_;

  // Current cluster configuration
  nuraft::ptr<nuraft::cluster_config> cluster_config_;

  // Current server state (term, voted for)
  nuraft::ptr<nuraft::srv_state> server_state_;

  // Log store instance
  nuraft::ptr<GvdbLogStore> log_store_;

  // Helper methods for persistence
  bool LoadConfigFromDb();
  bool SaveConfigToDb();
  bool LoadStateFromDb();
  bool SaveStateToDb();
};

}  // namespace consensus
}  // namespace gvdb