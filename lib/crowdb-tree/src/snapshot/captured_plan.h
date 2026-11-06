// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"

#include <set>

namespace crowdb::tree
{
struct CapturedWritePlan
{
    size_t primary_frames = 0;
    // Exact primary receipts that survive unchanged into the new catalog.
    size_t reused_primary_frames     = 0;
    size_t new_overflow_frames       = 0;
    size_t reused_dependency_entries = 0;
    // Raw dependencies require extent leases but retain their existing PIDs.
    // Shared chains are counted per use to keep admission conservative.
    size_t resident_dependency_entries  = 0;
    size_t relocated_dependency_entries = 0;
    size_t reference_heads              = 0;
    size_t frame_count_upper            = 0;
    size_t segments                     = 0;
    // Publication batches use MappingView radix leaves, not durable segments.
    size_t rebase_batches = 0;
};

// Consumes layout-only stream results. One step visits one old dependency;
// only distinct durable/radix segment numbers accumulate, never frame payloads.
class CapturedLayoutPlanner
{
  public:
    CapturedLayoutPlanner(uint64_t next_page_id, uint64_t backend_id, CapturedWriteLimits limits, size_t max_segments,
                          std::shared_ptr<const CapturedDurablePages>    inherited,
                          std::shared_ptr<detail::AllocationReservation> metadata);
    Status accept(CapturedFrameResult::Plan page);
    Status step();
    Status finish();

    [[nodiscard]] bool pending() const noexcept
    {
        return page_.has_value();
    }

    [[nodiscard]] const CapturedWritePlan &result() const noexcept
    {
        return plan_;
    }

    [[nodiscard]] static std::optional<size_t> allocation_bound(uint64_t next_page_id, size_t max_segments) noexcept;

  private:
    Status                                                                  segment(uint64_t pid);
    uint64_t                                                                next_;
    uint64_t                                                                backend_;
    CapturedWriteLimits                                                     limits_;
    size_t                                                                  max_segments_;
    std::shared_ptr<const CapturedDurablePages>                             inherited_;
    std::set<uint64_t, std::less<>, detail::ReservationAllocator<uint64_t>> segments_;
    std::set<uint64_t, std::less<>, detail::ReservationAllocator<uint64_t>> rebase_segments_;
    std::optional<CapturedFrameResult::Plan>                                page_;
    size_t                                                                  reference_ = 0;
    uint64_t                                                                pid_       = kInvalidPageId;
    uint64_t                                                                remaining_ = 0;
    CapturedWritePlan                                                       plan_;
};
} // namespace crowdb::tree
