#ifndef GVDB_STORAGE_STORAGE_FACTORY_H_
#define GVDB_STORAGE_STORAGE_FACTORY_H_

#include <memory>

#include "core/config.h"
#include "core/interfaces.h"
#include "core/status.h"

namespace gvdb {
namespace storage {

// ============================================================================
// StorageFactory - Factory for creating storage instances
// ============================================================================
class StorageFactory : public core::IStorageFactory {
 public:
  explicit StorageFactory(core::IIndexFactory* index_factory);

  ~StorageFactory() override = default;

  // Create a storage instance based on configuration
  [[nodiscard]] core::StatusOr<std::unique_ptr<core::IStorage>> CreateStorage(
      const core::StorageConfig& config) override;

 private:
  core::IIndexFactory* index_factory_;
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_STORAGE_FACTORY_H_
