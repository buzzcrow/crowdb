// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/published_read_internal.h"

#include <algorithm>

namespace crowdb::tree::published_read_detail
{
std::optional<size_t> read_metadata_allocation_bound(const PublishedReadLimits &limits, size_t workers) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    if (workers == 0 || limits.max_page_sources == 0 || limits.max_work == 0 || limits.max_entries == 0 ||
        limits.max_result_bytes == 0) {
        return {};
    }
    const auto callbacks = AllocationBound(workers).add(2).value();
    const auto page =
        AllocationBound(PageState::allocation_bound())
            .add_product(limits.max_page_sources, sizeof(BaseRef) + sizeof(std::shared_ptr<const OverlayBatch>))
            .value();
    const auto key          = AllocationBound(OwnedKey::allocation_overhead()).add(limits.max_key_bytes).add(1).value();
    const auto participants = AllocationBound().add_product(workers, limits.max_memtables).value();
    if (!callbacks || !page || !key || !participants) {
        return {};
    }
    const auto callback =
        AllocationBound(shared_allocation_bound<LoadEvent>()).add(std::max(*page, limits.page_metadata_bytes)).value();
    if (!callback) {
        return {};
    }
    // Route pending keys can coexist with the key used to resume the prior
    // decoder. PageState replacement keeps both source arrays alive briefly.
    return AllocationBound()
        .add(PublishedRouteCursor::allocation_bound(limits.max_route_depth))
        .add_product(*callbacks, *callback)
        .add_product(3, *key)
        .add_product(*participants, EpochManager::participant_allocation_bytes())
        .value();
}
} // namespace crowdb::tree::published_read_detail
