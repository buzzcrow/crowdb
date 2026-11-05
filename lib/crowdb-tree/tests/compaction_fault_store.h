// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <atomic>

namespace crowdb::tree
{
// The actual BlockPageStore remains the allocator/domain authority. Its async
// adapter runs on ScheduledPageStore's backend lane and preserves each grant.
class TestCompactionFaultStore final : public AsyncPageStore
{
  public:
    explicit TestCompactionFaultStore(BlockPageStore &store) : store_(store)
    {
    }

    void arm(int index, bool sync)
    {
        writes_.store(0);
        syncs_.store(0);
        fail_write_.store(sync ? -1 : index);
        fail_sync_.store(sync ? index : -1);
    }

    [[nodiscard]] unsigned faults() const
    {
        return faults_.load();
    }

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr addr, void *data, size_t size, AsyncCompletion complete) override
    {
        complete.complete(store_.read_at(addr, static_cast<uint8_t *>(data), size, complete.allocations));
        return 0;
    }

    uint64_t submit_write(PageAddr addr, const void *data, size_t size, AsyncCompletion complete) override
    {
        if (writes_.fetch_add(1) == fail_write_.load()) {
            faults_.fetch_add(1);
            complete.complete(Status::io_error("injected compaction write failure"));
        }
        else {
            complete.complete(store_.write_at(addr, static_cast<const uint8_t *>(data), size, complete.allocations));
        }
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        if (syncs_.fetch_add(1) == fail_sync_.load()) {
            faults_.fetch_add(1);
            complete.complete(Status::io_error("injected compaction sync failure"));
        }
        else {
            complete.complete(store_.sync(complete.allocations));
        }
        return Status::Ok();
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }

  private:
    BlockPageStore       &store_;
    std::atomic<int>      writes_{0};
    std::atomic<int>      syncs_{0};
    std::atomic<int>      fail_write_{-1};
    std::atomic<int>      fail_sync_{-1};
    std::atomic<unsigned> faults_{0};
};
} // namespace crowdb::tree
