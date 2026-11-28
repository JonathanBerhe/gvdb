// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/vectordb/request_guard.h"

#include <utility>

#include "auth/audit_context.h"
#include "auth/auth_context.h"
#include "core/status.h"

namespace gvdb::service {

RequestGuard::RequestGuard(std::shared_ptr<auth::RbacStore> rbac_store)
    : rbac_store_(std::move(rbac_store)) {}

grpc::Status RequestGuard::Check(auth::Permission permission,
                                 const std::string& collection_name) const {
  if (!rbac_store_) return grpc::Status::OK;

  const auto& key = auth::AuthContext::GetCurrentKey();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "No API key in auth context");
  }

  auto* role = rbac_store_->Lookup(key);
  if (!role) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Unknown API key");
  }

  if (!auth::HasPermission(role->role, permission)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        "Role does not have permission for this operation");
  }

  if (!collection_name.empty() && !auth::HasCollectionAccess(*role, collection_name)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        "Access denied for collection: " + collection_name);
  }

  return grpc::Status::OK;
}

grpc::Status RequestGuard::Authorize(auth::Permission permission,
                                     const std::string& collection_name,
                                     std::optional<size_t> item_count) const {
  RETURN_IF_ERROR(Check(permission, collection_name));
  auth::AuditContext::SetCollection(collection_name);
  if (item_count) auth::AuditContext::SetItemCount(static_cast<int64_t>(*item_count));
  return grpc::Status::OK;
}

}  // namespace gvdb::service
