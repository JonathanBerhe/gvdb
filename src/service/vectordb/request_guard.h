// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <optional>
#include <string>

#include "auth/rbac.h"

namespace gvdb::service {

// The permission check that runs first in every VectorDBService handler. It
// reads the API key of the current request from auth::AuthContext and looks
// its role up in the RBAC store. Without an RBAC store every request passes.
class RequestGuard {
 public:
  explicit RequestGuard(std::shared_ptr<auth::RbacStore> rbac_store);

  // OK when the role of the current key has `permission` and, for a
  // non-empty `collection_name`, access to that collection.
  [[nodiscard]] grpc::Status Check(auth::Permission permission,
                                   const std::string& collection_name) const;

  // Check for a request on one collection. When the check passes, it also
  // records the collection, and the item count when given, for the audit log.
  [[nodiscard]] grpc::Status Authorize(auth::Permission permission,
                                       const std::string& collection_name,
                                       std::optional<size_t> item_count = std::nullopt) const;

  [[nodiscard]] bool enabled() const { return rbac_store_ != nullptr; }

 private:
  std::shared_ptr<auth::RbacStore> rbac_store_;
};

}  // namespace gvdb::service
