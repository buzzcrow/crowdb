// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_block_gc.h"

#include "btree/allocation_bound.h"
#include "btree/tree_work_counters.h"
#include "host_completion.h"

namespace crowdb::tree
{
namespace
{
ExtentLedgerResult claim_block(ExtentLedger &ledger, uint64_t begin, uint64_t bytes,
                               const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    ExtentLedgerResult     result;
    detail::HostCompletion done;
    auto                   status = ledger.claim_free(begin, bytes, metadata, [&](ExtentLedgerResult value) {
        result = std::move(value);
        done.signal();
    });
    if (!status.ok()) {
        return {.status = std::move(status), .extent = {}};
    }
    done.wait();
    return result;
}
} // namespace

std::optional<size_t> local_block_gc_metadata_bound(LocalBlockGcLimits limits) noexcept
{
    if (limits.max_blocks == 0 || limits.max_deleted_bytes == 0) {
        return std::nullopt;
    }
    return detail::AllocationBound().add_product(limits.max_blocks, ExtentLedger::reservation_metadata_bound()).value();
}

uint64_t local_block_gc_unit(const PageStore &store) noexcept
{
    if (const auto *block = dynamic_cast<const BlockPageStore *>(&store)) {
        return block->block_size();
    }
    return dynamic_cast<const MemPageStore *>(&store) != nullptr ? MemPageStore::kBackingPageBytes : 0;
}

LocalBlockGcResult collect_free_local_blocks(PageStore &store, ExtentLedger &ledger, uint64_t first_block,
                                             LocalBlockGcLimits                                    limits,
                                             const std::shared_ptr<detail::AllocationReservation> &metadata,
                                             const std::shared_ptr<AllocationBudget>              &backend)
{
    LocalBlockGcResult result;
    const auto         bytes = local_block_gc_unit(store);
    if (!local_block_gc_metadata_bound(limits) || bytes == 0 || bytes % ledger.limits().iu_size != 0) {
        result.status = Status::invalid_argument("invalid local GC geometry or limits");
        return result;
    }
    if (bytes > limits.max_deleted_bytes) {
        result.status = Status::resource_exhausted("GC deletion budget cannot hold one block");
        return result;
    }
    auto      *memory = dynamic_cast<MemPageStore *>(&store);
    auto      *block  = dynamic_cast<BlockPageStore *>(&store);
    const auto begin  = ledger.limits().region_begin;
    const auto first  = (begin / bytes) + static_cast<uint64_t>(begin % bytes != 0);
    const auto end =
        std::min(ledger.stats().address_high_water / bytes, memory != nullptr ? UINT64_MAX : uint64_t{UINT32_MAX} + 1);
    result.next_block = std::max(first_block, first);
    while (result.next_block < end && result.inspected < limits.max_blocks &&
           bytes <= limits.max_deleted_bytes - result.deleted_bytes) {
        const auto index = result.next_block;
        auto       lease = claim_block(ledger, index * bytes, bytes, metadata);
        ++result.inspected;
        if (lease.status.code() == Code::kNotFound) {
            ++result.protected_blocks;
            ++result.next_block;
            continue;
        }
        if (!lease.status.ok()) {
            result.status = std::move(lease.status);
            return result;
        }
        bool deleted = false;
        if (memory != nullptr) {
            deleted = memory->release_backing_page(index);
        }
        else {
            uint64_t offset = 0;
            if (block->file_for_offset(index * bytes, &offset, backend)) {
                result.status = block->delete_block(static_cast<uint32_t>(index), backend);
                deleted       = result.status.ok();
            }
            if (!result.status.ok()) {
                return result;
            }
        }
        if (deleted) {
            ++result.deleted_blocks;
            result.deleted_bytes += bytes;
            record_work(metadata, TreeWorkCounter::BackingDeletedBytes, bytes, TreeWorkCause::GarbageCollection);
        }
        ++result.next_block;
    }
    result.complete = result.next_block >= end;
    return result;
}
} // namespace crowdb::tree
