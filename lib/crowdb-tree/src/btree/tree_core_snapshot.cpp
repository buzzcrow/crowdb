// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
Status TreeCore::capture_snapshot(std::shared_ptr<PublishedSnapshotContext> *out)
{
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    return PublishedSnapshotContext::capture_admitted(shared_from_this(), out);
}

Status TreeCore::capture_snapshot(std::shared_ptr<const PublishedTreeState>  view,
                                  std::shared_ptr<PublishedSnapshotContext> *out)
{
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    if (!view || view->generation != capture()->generation) {
        return Status::unavailable("fixed snapshot belongs to an old Tree generation");
    }
    return PublishedSnapshotContext::capture_admitted(shared_from_this(), out, std::move(view));
}

Status TreeCore::acquire_snapshot_window(const std::shared_ptr<AllocationBudget>        &metadata,
                                         std::shared_ptr<TreeOperationLifetime::Ticket> *ticket,
                                         AsyncPageStore                                **backend)
{
    if (backend == nullptr) {
        return Status::invalid_argument("snapshot read window requires backend output");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    auto status = state_->lifetime->acquire(metadata, ticket);
    if (status.ok()) {
        *backend = state_->backend.get();
    }
    return status;
}
} // namespace crowdb::tree
