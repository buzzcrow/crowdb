// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_async_io.h"

#include "backend_io_allocation.h"
#include "block_account.h"
#include "btree/allocation_bound.h"
#include "btree/tree_work_counters.h"

#ifdef CROWDB_HAVE_LIBURING
#    include <algorithm>
#    include <atomic>
#    include <climits>
#    include <cstdlib>
#    include <cstring>

namespace crowdb::tree::detail
{
namespace
{
struct IoJoin
{
    explicit IoJoin(AsyncCompletion callback) : complete(std::move(callback))
    {
    }

    AsyncCompletion complete;
    // The submission loop owns one count so no inline completion can finish
    // the operation while more chunks may still be submitted.
    std::atomic<size_t> pending{1};
    std::atomic_flag    failed = ATOMIC_FLAG_INIT;
    std::atomic<bool>   missing{false};
    Status              first_error;
    Status              first_missing;

    void done(Status status)
    {
        if (status.code() == Code::kNotFound) {
            if (!missing.exchange(true, std::memory_order_relaxed)) {
                first_missing = std::move(status);
            }
        }
        else if (!status.ok() && !failed.test_and_set(std::memory_order_relaxed)) {
            first_error = std::move(status);
        }
        // The winning error is written before its count is released; the
        // last completion acquires that write through this release sequence.
        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // A short part must not hide a transport error in another part.
            if (first_error.ok() && missing.load(std::memory_order_relaxed)) {
                first_error = std::move(first_missing);
            }
            complete.complete(std::move(first_error));
        }
    }
};

struct IoPart
{
    std::shared_ptr<AllocationBudget> budget;
    std::shared_ptr<IoJoin>           join;
    std::shared_ptr<FileMedium>       medium;
    void                             *read_buffer = nullptr;
    void                             *aligned     = nullptr;
    size_t                            length      = 0;

    ~IoPart()
    {
        if (aligned != nullptr) {
            ::free(aligned);
            budget->record_allocation(AllocationKind::kIo, length, false);
            budget->release(length);
        }
    }

    void prepare(const void *write_buffer, uint32_t iu)
    {
        const void *buffer = read_buffer != nullptr ? read_buffer : write_buffer;
        if (iu <= 1 || reinterpret_cast<uintptr_t>(buffer) % 4096 == 0) {
            return;
        }
        if (!budget->claim(length)) {
            throw std::bad_alloc();
        }
        if (::posix_memalign(&aligned, 4096, length) != 0) {
            budget->release(length);
            throw std::bad_alloc();
        }
        budget->record_allocation(AllocationKind::kIo, length, true);
        if (read_buffer == nullptr) {
            std::memcpy(aligned, write_buffer, length);
            record_work(budget, TreeWorkCounter::BackendBounceCopyBytes, length, join->complete.cause);
        }
    }

    static void completed(void *context, int result) noexcept
    {
        auto &part = *static_cast<IoPart *>(context);
        if (part.read_buffer == nullptr) {
            part.medium->mark_dirty();
        }
        const bool success = std::cmp_equal(result, part.length);
        if (success && part.read_buffer != nullptr && part.aligned != nullptr) {
            std::memcpy(part.read_buffer, part.aligned, part.length);
            record_work(part.budget, TreeWorkCounter::BackendBounceCopyBytes, part.length, part.join->complete.cause);
        }
        auto status = success ? Status::Ok() : Status::io_error();
        if (result == -ENOMEM) {
            status = Status::resource_exhausted();
        }
        if (part.read_buffer != nullptr && result >= 0 && std::cmp_less(result, part.length)) {
            try {
                status = Status::not_found("BlockAsyncPageStore: short read (" + std::to_string(result) + " of " +
                                           std::to_string(part.length) + " bytes)");
            }
            catch (const std::bad_alloc &) {
                // Completion must still release accepted I/O when diagnostics cannot allocate.
                status = Status::not_found();
            }
        }
        part.join->done(std::move(status));
    }
};

Status submit_part(::crowdb::common::DiskIOUring &uring, const std::shared_ptr<IoJoin> &join,
                   std::shared_ptr<FileMedium> medium, uint64_t offset, void *read_buffer, const void *write_buffer,
                   size_t length, uint32_t iu)
{
    const auto &budget = join->complete.allocations;
    auto        part   = std::allocate_shared<IoPart>(MetadataAllocator<IoPart>(budget));
    part->budget       = budget;
    part->join         = join;
    part->medium       = std::move(medium);
    part->read_buffer  = read_buffer;
    part->length       = length;
    part->prepare(write_buffer, iu);
    ::crowdb::common::IOCompletion callback{
        .context = part.get(), .callback = &IoPart::completed, .owner = part, .allocation = io_allocation(budget)};
    join->pending.fetch_add(1, std::memory_order_relaxed);
    if (read_buffer != nullptr) {
        uring.submit_read_owned(part->medium->fd(), part->aligned != nullptr ? part->aligned : read_buffer, length,
                                static_cast<off_t>(offset), std::move(callback));
    }
    else {
        uring.submit_write_owned(part->medium->fd(), part->aligned != nullptr ? part->aligned : write_buffer, length,
                                 static_cast<off_t>(offset), std::move(callback));
    }
    return Status::Ok();
}

Status submit_parts(BlockPageStore &store, ::crowdb::common::DiskIOUring &uring, PageAddr address, void *read_buffer,
                    const void *write_buffer, size_t length, const std::shared_ptr<IoJoin> &join)
{
    const auto iu         = std::max(store.iu_size(), uint32_t{1});
    const auto block_size = store.block_size();
    const auto max_chunk  = (static_cast<uint64_t>(INT_MAX) / iu) * iu;
    size_t     done       = 0;
    while (done < length) {
        uint64_t offset = 0;
        auto     medium = store.file_for_offset(address + done, &offset, join->complete.allocations);
        if (!medium) {
            return read_buffer != nullptr && block_size != 0
                     ? Status::not_found("BlockAsyncPageStore: no fd for offset")
                     : Status::io_error("BlockAsyncPageStore: no fd for offset");
        }
        const auto   available = block_size == 0 ? max_chunk : block_size - offset;
        const size_t chunk     = std::min({static_cast<uint64_t>(length - done), available, max_chunk});
        if (chunk == 0 || offset % iu != 0 || chunk % iu != 0 || offset > static_cast<uint64_t>(INT64_MAX)) {
            return Status::invalid_argument();
        }
        auto status = submit_part(uring, join, std::move(medium), offset,
                                  read_buffer != nullptr ? static_cast<uint8_t *>(read_buffer) + done : nullptr,
                                  write_buffer != nullptr ? static_cast<const uint8_t *>(write_buffer) + done : nullptr,
                                  chunk, iu);
        if (!status.ok()) {
            return status;
        }
        done += chunk;
    }
    return Status::Ok();
}
} // namespace

std::optional<size_t> block_io_allocation_bound(size_t bytes, size_t parts) noexcept
{
    return AllocationBound(shared_allocation_bound<IoJoin>())
        .add(bytes)
        .add_product(parts, shared_allocation_bound<IoPart>() +
                                ::crowdb::common::DiskIOUring::owned_submission_allocation_bytes())
        .value();
}

void submit_block_io(BlockPageStore &store, ::crowdb::common::DiskIOUring &uring, PageAddr address, void *read_buffer,
                     const void *write_buffer, size_t length, AsyncCompletion complete)
{
    const auto iu = std::max(store.iu_size(), uint32_t{1});
    if (length == 0) {
        complete.complete(Status::Ok());
        return;
    }
    if ((read_buffer == nullptr) == (write_buffer == nullptr) || address > UINT64_MAX - length || address % iu != 0 ||
        length % iu != 0) {
        complete.complete(Status::invalid_argument());
        return;
    }
    std::shared_ptr<IoJoin> join;
    Status                  status;
    try {
        complete.allocations = block_allocation_budget(store, complete.allocations);
        if (!complete.allocations) {
            complete.complete(Status::invalid_argument("backend allocation budget belongs to another store"));
            return;
        }
        join = std::allocate_shared<IoJoin>(MetadataAllocator<IoJoin>(complete.allocations), complete);
        if (write_buffer != nullptr) {
            status = store.ensure_extents(address, length, complete.allocations);
        }
        if (status.ok()) {
            status = submit_parts(store, uring, address, read_buffer, write_buffer, length, join);
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    if (join) {
        join->done(std::move(status));
    }
    else {
        complete.complete(std::move(status));
    }
}
} // namespace crowdb::tree::detail
#endif
