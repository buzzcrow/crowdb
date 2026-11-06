// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/published_state.h"

namespace crowdb::tree
{
size_t PublicationCoordinator::complete_view_reserve_bytes() noexcept
{
    return detail::shared_allocation_bound<PublishedTreeState>() + PublishedSources::allocation_bound();
}

std::shared_ptr<const PublishedTreeState> PublicationCoordinator::capture_complete() const
{
    auto        guard = epoch_.enter();
    const auto *root  = root_.load(std::memory_order_acquire);
    return root->complete_l1 ? root->complete_l1 : root->state;
}

std::shared_ptr<const PublishedTreeState>
PublicationCoordinator::prepare_complete_view(const Root &current, const PublishedTreeState &next,
                                              const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (!next.pending_handoff && next.flush_admission_owner == 0) {
        return {};
    }
    if (current.complete_l1) {
        return current.complete_l1;
    }
    // Publication serializes the transition from complete L1 to ownership of
    // a pending handoff. Share its immutable mapping without retaining live L0
    // tables or walking pages on the publication owner.
    auto complete     = detail::reserved_shared<PublishedTreeState>(metadata, *current.state);
    auto sources      = detail::reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor    = complete->coverage;
    complete->sources = std::move(sources);
    return complete;
}
} // namespace crowdb::tree
