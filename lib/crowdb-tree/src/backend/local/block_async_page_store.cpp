// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Local asynchronous block-backend implementation.

#include "backend_io_allocation.h"
#include "block_account.h"
#include "block_async_io.h"
#include "btree/allocation_bound.h"

#include <utility>
#include <vector>

namespace crowdb::tree
{

// ── BlockAsyncPageStore ───────────────────────────────────────────

BlockAsyncPageStore::BlockAsyncPageStore(BlockPageStore *store, ::crowdb::common::DiskIOUring *uring)
    : store_(store),
      uring_(uring)
{
}

uint64_t BlockAsyncPageStore::submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete)
{
    detail::submit_block_io(*store_, *uring_, addr, buf, nullptr, len, std::move(on_complete));
    return 0;
}

uint64_t BlockAsyncPageStore::submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete)
{
    detail::submit_block_io(*store_, *uring_, addr, nullptr, buf, len, std::move(on_complete));
    return 0;
}

namespace
{
struct FsyncState : std::enable_shared_from_this<FsyncState>
{
    explicit FsyncState(const std::shared_ptr<AllocationBudget> &budget)
        : targets(MetadataAllocator<BlockSyncTarget>(budget))
    {
    }

    BlockPageStore::BlockSyncTargets targets;
    size_t                           idx = 0;
    AsyncCompletion                  callback;
    ::crowdb::common::DiskIOUring   *uring = nullptr;

    static void completed(void *context, int result) noexcept
    {
        static_cast<FsyncState *>(context)->next(result);
    }

    void next(int result) noexcept
    {
        if (result >= 0 && idx != 0) {
            const auto &target = targets[idx - 1];
            target.medium->acknowledge_sync(target.generation);
        }
        if (result < 0 || idx == targets.size()) {
            auto status = result < 0 ? Status::io_error() : Status::Ok();
            if (result == -ENOMEM) {
                status = Status::resource_exhausted();
            }
            callback.complete(std::move(status));
            return;
        }
        auto self = weak_from_this().lock();
        if (!self) {
            callback.complete(Status::internal_error("fsync continuation lost its owner"));
            return;
        }
        const int fd = targets[idx++].medium->fd();
        uring->submit_fsync_owned(fd, false,
                                  {.context    = this,
                                   .callback   = &completed,
                                   .owner      = std::move(self),
                                   .allocation = detail::io_allocation(callback.allocations)});
    }
};
} // namespace

namespace detail
{
std::optional<size_t> block_sync_allocation_bound(size_t extents) noexcept
{
    return AllocationBound(shared_allocation_bound<FsyncState>())
        .add_product(extents, sizeof(BlockSyncTarget))
        .add_product(2, ::crowdb::common::DiskIOUring::owned_submission_allocation_bytes())
        .value();
}
} // namespace detail

Status BlockAsyncPageStore::submit_fsync(AsyncCompletion on_complete)
try {
    auto budget = detail::block_allocation_budget(*store_, on_complete.allocations);
    if (!budget) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    auto targets = store_->sync_targets(budget);
    if (targets.empty()) {
        on_complete.complete(Status::Ok());
        return Status::Ok();
    }
    auto state                  = std::allocate_shared<FsyncState>(MetadataAllocator<FsyncState>(budget), budget);
    state->targets              = std::move(targets);
    state->callback             = std::move(on_complete);
    state->callback.allocations = std::move(budget);
    state->uring                = uring_;
    state->next(0);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

void BlockAsyncPageStore::cancel(uint64_t /*op_id*/)
{
    // No-op — per-op cancel is removed. Use DiskIOUring::cancel_fd for
    // fd-level cancellation.
}

} // namespace crowdb::tree
