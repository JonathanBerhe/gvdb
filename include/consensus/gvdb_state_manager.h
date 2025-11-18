#pragma once

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
 * Supports both in-memory and persistent (RocksDB-backed) modes:
 * - **In-Memory**: Suitable for development/testing (data lost on restart)
 * - **Persistent**: Production-ready with crash recovery
 *
 * Thread-safe: All operations are protected by mutex.
 */
class GvdbStateManager : public nuraft::state_mgr {
 public:
  /**
   * Create an in-memory state manager (for testing).
   *
   * @param server_id This server's ID
   * @param endpoint This server's endpoint (e.g., "localhost:9000")
   */
  GvdbStateManager(int32_t server_id, const std::string& endpoint);

  /**
   * Create a persistent state manager backed by RocksDB.
   *
   * @param server_id This server's ID
   * @param endpoint This server's endpoint
   * @param log_store_path Directory path for log store RocksDB
   * @param state_path Directory path for state RocksDB
   */
  GvdbStateManager(int32_t server_id, const std::string& endpoint,
                   const std::string& log_store_path, const std::string& state_path);

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
  // Mutex protecting all data structures
  mutable std::mutex mutex_;

  // Persistent mode flag
  bool persistent_mode_{false};
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
  bool load_config_from_db();
  bool save_config_to_db();
  bool load_state_from_db();
  bool save_state_to_db();
};

}  // namespace consensus
}  // namespace gvdb
