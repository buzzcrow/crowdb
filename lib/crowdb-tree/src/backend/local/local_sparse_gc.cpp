// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_sparse_gc.h"

#include "host_completion.h"

#include <algorithm>
#include <cmath>

namespace crowdb::tree
{
LocalSparseBlock select_sparse_local_block(BlockPageStore &store, ExtentLedger &ledger, uint64_t first_block,
                                           const std::shared_ptr<detail::AllocationReservation> &metadata,
                                           double free_threshold, const std::shared_ptr<AllocationBudget> &backend)
{
    LocalSparseBlock result;
    const auto       bytes = store.block_size();
    if (free_threshold <= 0.0 || free_threshold >= 1.0 || std::isnan(free_threshold)) {
        result.status = Status::invalid_argument();
        return result;
    }
    if (bytes == 0 || bytes > 64 * 1024 * 1024 || bytes % ledger.limits().iu_size != 0) {
        result.status = Status::not_supported();
        return result;
    }
    const auto begin = ledger.limits().region_begin;
    const auto first = (begin / bytes) + static_cast<uint64_t>(begin % bytes != 0);
    const auto end   = std::min(ledger.stats().address_high_water / bytes, uint64_t{UINT32_MAX} + 1);
    auto       next  = std::max(first_block, first);
    for (size_t visits = 0; next < end && visits < 8; ++next, ++visits) {
        result.next_block = next + 1 < end ? next + 1 : 0;
        uint64_t offset   = 0;
        if (!store.file_for_offset(next * bytes, &offset, backend)) {
            continue;
        }
        ExtentLedgerResult     fence;
        detail::HostCompletion done;
        result.status = ledger.exclude_allocations(next * bytes, bytes, metadata, [&](ExtentLedgerResult value) {
            fence = std::move(value);
            done.signal();
        });
        if (!result.status.ok()) {
            return result;
        }
        done.wait();
        if (!fence.status.ok()) {
            result.status = std::move(fence.status);
            return result;
        }
        if (fence.occupied_bytes != 0 &&
            static_cast<long double>(bytes - fence.occupied_bytes) / bytes > free_threshold) {
            result.exclusion = std::move(fence.extent);
            return result;
        }
    }
    result.status = Status::not_found();
    return result;
}
} // namespace crowdb::tree
