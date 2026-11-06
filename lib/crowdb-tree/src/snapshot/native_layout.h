// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/overflow_backing.h"
#include "snapshot/captured_frames.h"

#include <set>

namespace crowdb::tree
{
struct NativeLayoutLimits
{
    uint64_t max_frames           = 1024 * 1024;
    uint64_t max_dependency_steps = 1024 * 1024;
};

// Counts and owns physical old overflow descriptors, never their payloads.
// One step checks one dependency; the caller controls CPU continuation work.
class NativeLayoutPlanner
{
  public:
    NativeLayoutPlanner(std::shared_ptr<const PublishedTreeState> view, NativeLayoutLimits limits,
                        std::shared_ptr<detail::AllocationReservation> metadata, uint64_t backend_id = 0,
                        uint64_t incarnation = 0);
    static std::optional<size_t> allocation_bound(uint64_t next_page_id, NativeLayoutLimits limits) noexcept;
    Status                       accept(CapturedFrameResult::Plan page);
    Status                       step();
    Status                       finish();

    [[nodiscard]] bool pending() const noexcept
    {
        return page_.has_value();
    }

    [[nodiscard]] uint64_t new_overflow_frames() const noexcept
    {
        return new_overflow_;
    }

    [[nodiscard]] uint64_t frame_count() const noexcept
    {
        return primary_.size() + new_overflow_ + old_.pages.size();
    }

    [[nodiscard]] const OverflowBackingSet::Pages &old_frames() const noexcept
    {
        return old_.pages;
    }

  private:
    Status                                    include(uint64_t pid, const OverflowBackingPage &page);
    std::shared_ptr<const PublishedTreeState> view_;
    NativeLayoutLimits                        limits_;
    uint64_t                                  backend_id_;
    uint64_t                                  incarnation_;
    OverflowBackingSet                        old_;
    std::set<uint64_t, std::less<>, detail::ReservationAllocator<uint64_t>> primary_;
    std::optional<CapturedFrameResult::Plan>                                page_;
    size_t                                                                  reference_    = 0;
    uint64_t                                                                pid_          = kInvalidPageId;
    uint64_t                                                                remaining_    = 0;
    uint64_t                                                                steps_        = 0;
    uint64_t                                                                new_overflow_ = 0;
};
} // namespace crowdb::tree
