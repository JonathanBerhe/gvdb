// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/gvdb_state_manager.h"

#include "utils/logger.h"

#include <filesystem>
#include <fstream>

namespace gvdb {
namespace consensus {

using nuraft::cluster_config;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::srv_config;
using nuraft::srv_state;

GvdbStateManager::GvdbStateManager(int32_t server_id, const std::string& endpoint)
    : persistent_mode_(false), server_id_(server_id), endpoint_(endpoint) {

  // Create this server's configuration
  my_srv_config_ = nuraft::cs_new<srv_config>(server_id, endpoint);

  // Create initial cluster configuration (single server cluster)
  cluster_config_ = nuraft::cs_new<cluster_config>();
  cluster_config_->get_servers().push_back(my_srv_config_);

  // Create in-memory log store
  log_store_ = nuraft::cs_new<GvdbLogStore>();

  utils::Logger::Instance().Info(
      "GvdbStateManager initialized (in-memory, server_id={}, endpoint={})",
      server_id, endpoint);
}

GvdbStateManager::GvdbStateManager(int32_t server_id, const std::string& endpoint,
                                   const std::string& log_store_path,
                                   const std::string& state_path)
    : persistent_mode_(true), state_db_path_(state_path),
      server_id_(server_id), endpoint_(endpoint) {

  // Create this server's configuration
  my_srv_config_ = nuraft::cs_new<srv_config>(server_id, endpoint);

  // Try to load existing state from disk
  bool loaded_config = load_config_from_db();
  bool loaded_state = load_state_from_db();

  if (!loaded_config) {
    // First initialization - create single-server cluster
    cluster_config_ = nuraft::cs_new<cluster_config>();
    cluster_config_->get_servers().push_back(my_srv_config_);
    save_config_to_db();  // Persist initial config
    utils::Logger::Instance().Info("Created initial cluster configuration");
  }

  // Create persistent log store
  log_store_ = nuraft::cs_new<GvdbLogStore>(log_store_path);

  utils::Logger::Instance().Info(
      "GvdbStateManager initialized (persistent, server_id={}, endpoint={}, state_path={})",
      server_id, endpoint, state_path);
}

ptr<cluster_config> GvdbStateManager::load_config() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Return current cluster configuration
  // In production, this would read from disk
  return cluster_config_;
}

void GvdbStateManager::save_config(const cluster_config& config) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Serialize and save the config
  ptr<nuraft::buffer> buf = config.serialize();

  // Deserialize to create a copy
  cluster_config_ = cluster_config::deserialize(*buf);

  // Persist to disk if in persistent mode
  if (persistent_mode_) {
    if (!save_config_to_db()) {
      utils::Logger::Instance().Error("Failed to persist cluster config to disk");
    }
  }

  utils::Logger::Instance().Debug(
      "Saved cluster config ({} servers)", cluster_config_->get_servers().size());
}

void GvdbStateManager::save_state(const srv_state& state) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Serialize and save the server state
  ptr<nuraft::buffer> buf = state.serialize();

  // Deserialize to create a copy
  server_state_ = srv_state::deserialize(*buf);

  // Persist to disk if in persistent mode
  if (persistent_mode_) {
    if (!save_state_to_db()) {
      utils::Logger::Instance().Error("Failed to persist server state to disk");
    }
  }

  utils::Logger::Instance().Debug(
      "Saved server state (term={})", server_state_->get_term());
}

ptr<srv_state> GvdbStateManager::read_state() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Return saved server state
  // Returns nullptr on first initialization
  return server_state_;
}

ptr<log_store> GvdbStateManager::load_log_store() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Return the log store instance
  return log_store_;
}

int32_t GvdbStateManager::server_id() {
  return server_id_;
}

void GvdbStateManager::system_exit(const int exit_code) {
  utils::Logger::Instance().Error(
      "Raft system exit called with code {}", exit_code);

  // In production, might want to:
  // - Flush pending writes
  // - Close file handles
  // - Notify monitoring systems
  // - Graceful shutdown

  // For now, just log it
  // Do NOT call std::exit() here - let the application decide
}

// Persistence helper methods

bool GvdbStateManager::load_config_from_db() {
  namespace fs = std::filesystem;

  // Ensure state directory exists
  fs::path state_dir(state_db_path_);
  if (!fs::exists(state_dir)) {
    return false;  // Not an error - first initialization
  }

  fs::path config_file = state_dir / "cluster_config.bin";
  if (!fs::exists(config_file)) {
    return false;
  }

  try {
    // Read file
    std::ifstream ifs(config_file, std::ios::binary);
    if (!ifs) {
      utils::Logger::Instance().Error("Failed to open cluster config file: {}", config_file.string());
      return false;
    }

    // Read size
    uint32_t size;
    ifs.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Read data
    std::vector<char> data(size);
    ifs.read(data.data(), size);

    if (!ifs) {
      utils::Logger::Instance().Error("Failed to read cluster config file");
      return false;
    }

    // Deserialize
    auto buf = nuraft::buffer::alloc(size);
    std::memcpy(buf->data_begin(), data.data(), size);
    buf->pos(0);

    cluster_config_ = cluster_config::deserialize(*buf);

    utils::Logger::Instance().Info("Loaded cluster config from disk ({} servers)",
                                    cluster_config_->get_servers().size());
    return true;

  } catch (const std::exception& e) {
    utils::Logger::Instance().Error("Exception loading cluster config: {}", e.what());
    return false;
  }
}

bool GvdbStateManager::save_config_to_db() {
  namespace fs = std::filesystem;

  // Ensure state directory exists
  fs::path state_dir(state_db_path_);
  if (!fs::exists(state_dir)) {
    std::error_code ec;
    if (!fs::create_directories(state_dir, ec)) {
      utils::Logger::Instance().Error("Failed to create state directory: {}", ec.message());
      return false;
    }
  }

  fs::path config_file = state_dir / "cluster_config.bin";

  try {
    // Serialize
    auto buf = cluster_config_->serialize();

    // Write to file atomically (write to temp, then rename)
    fs::path temp_file = state_dir / "cluster_config.bin.tmp";
    {
      std::ofstream ofs(temp_file, std::ios::binary | std::ios::trunc);
      if (!ofs) {
        utils::Logger::Instance().Error("Failed to open temp config file");
        return false;
      }

      // Write size
      uint32_t size = static_cast<uint32_t>(buf->size());
      ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));

      // Write data
      ofs.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());

      ofs.flush();
      if (!ofs) {
        utils::Logger::Instance().Error("Failed to write cluster config");
        return false;
      }
    }

    // Atomic rename
    fs::rename(temp_file, config_file);

    utils::Logger::Instance().Debug("Persisted cluster config to {}", config_file.string());
    return true;

  } catch (const std::exception& e) {
    utils::Logger::Instance().Error("Exception saving cluster config: {}", e.what());
    return false;
  }
}

bool GvdbStateManager::load_state_from_db() {
  namespace fs = std::filesystem;

  fs::path state_dir(state_db_path_);
  if (!fs::exists(state_dir)) {
    return false;
  }

  fs::path state_file = state_dir / "server_state.bin";
  if (!fs::exists(state_file)) {
    return false;
  }

  try {
    std::ifstream ifs(state_file, std::ios::binary);
    if (!ifs) {
      return false;
    }

    // Read size
    uint32_t size;
    ifs.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Read data
    std::vector<char> data(size);
    ifs.read(data.data(), size);

    if (!ifs) {
      return false;
    }

    // Deserialize
    auto buf = nuraft::buffer::alloc(size);
    std::memcpy(buf->data_begin(), data.data(), size);
    buf->pos(0);

    server_state_ = srv_state::deserialize(*buf);

    utils::Logger::Instance().Info("Loaded server state from disk (term={})",
                                    server_state_->get_term());
    return true;

  } catch (const std::exception& e) {
    utils::Logger::Instance().Error("Exception loading server state: {}", e.what());
    return false;
  }
}

bool GvdbStateManager::save_state_to_db() {
  namespace fs = std::filesystem;

  if (!server_state_) {
    return true;  // Nothing to save
  }

  fs::path state_dir(state_db_path_);
  if (!fs::exists(state_dir)) {
    std::error_code ec;
    if (!fs::create_directories(state_dir, ec)) {
      utils::Logger::Instance().Error("Failed to create state directory: {}", ec.message());
      return false;
    }
  }

  fs::path state_file = state_dir / "server_state.bin";

  try {
    // Serialize
    auto buf = server_state_->serialize();

    // Write atomically
    fs::path temp_file = state_dir / "server_state.bin.tmp";
    {
      std::ofstream ofs(temp_file, std::ios::binary | std::ios::trunc);
      if (!ofs) {
        return false;
      }

      uint32_t size = static_cast<uint32_t>(buf->size());
      ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));
      ofs.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());

      ofs.flush();
      if (!ofs) {
        return false;
      }
    }

    fs::rename(temp_file, state_file);

    utils::Logger::Instance().Debug("Persisted server state to {}", state_file.string());
    return true;

  } catch (const std::exception& e) {
    utils::Logger::Instance().Error("Exception saving server state: {}", e.what());
    return false;
  }
}

}  // namespace consensus
}  // namespace gvdb