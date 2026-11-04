// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "async_completion_adapter.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <memory>
#include <mutex>
#include <utility>

namespace crowdb::tree
{

Status Crowdbtree::retry_snapshot_anchor()
{
    if (!snapshot_anchor_pending_) {
        return Status::Ok();
    }
    Status status = opt_.page_store->sync();
    if (status.ok()) {
        snapshot_anchor_pending_ = false;
    }
    else {
        CRB_LOG_ERROR("[{}] snapshot pending anchor sync failed: {}", name_, status.to_string());
    }
    return status;
}

void Crowdbtree::snapshot_prepare_async(std::function<void(Status, uint64_t)> on_done)
{
    if (snapshot_anchor_pending_) {
        Status submitted = opt_.async_page_store->submit_fsync(
            detail::own_async_completion([this, on_done](const Status &status) mutable {
                if (!status.ok()) {
                    CRB_LOG_ERROR("[{}] snapshot pending anchor sync failed: {}", name_, status.to_string());
                    release_snapshot_slot();
                    on_done(status, 0);
                    return;
                }
                snapshot_anchor_pending_ = false;
                snapshot_prepare_async(std::move(on_done));
            }));
        if (!submitted.ok()) {
            release_snapshot_slot();
            on_done(std::move(submitted), 0);
        }
        return;
    }
    auto   prepared = std::make_shared<PreparedSnapshot>();
    Status status;
    {
        std::scoped_lock lock(write_mutex_);
        status = prepare_snapshot_locked(prepared.get());
    }
    if (!status.ok()) {
        CRB_LOG_ERROR("[{}] snapshot prepare failed: {}", name_, status.to_string());
        release_snapshot_slot();
        on_done(std::move(status), 0);
        return;
    }
    snapshot_write_next_async(std::move(prepared), 0, std::move(on_done));
}

} // namespace crowdb::tree
