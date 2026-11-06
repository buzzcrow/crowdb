// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "backing_ref.h"

#include <map>

namespace crowdb::tree
{
struct OverflowBackingPage
{
    std::shared_ptr<const BackingRef> backing;
    uint64_t                          next          = UINT64_MAX;
    uint32_t                          bytes         = 0;
    uint32_t                          frame_bytes   = 0;
    uint32_t                          logical_bytes = 0;
};

struct OverflowBackingSet
{
    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !pages.empty(); ++i) {
            pages.erase(pages.begin());
        }
        return pages.empty();
    }

    using Entry = std::pair<const uint64_t, OverflowBackingPage>;
    using Pages = std::map<uint64_t, OverflowBackingPage, std::less<>, detail::ReservationAllocator<Entry>>;

    explicit OverflowBackingSet(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : pages(std::less<>{}, detail::ReservationAllocator<Entry>(metadata))
    {
    }

    Pages pages;
};
} // namespace crowdb::tree
