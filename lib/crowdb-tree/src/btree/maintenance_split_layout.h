// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "maintenance_publication.h"

namespace crowdb::tree::detail
{
struct SplitWindowLayout
{
    explicit SplitWindowLayout(const std::shared_ptr<AllocationReservation> &metadata)
        : separators(ReservationAllocator<OwnedKey>(metadata)),
          primary_bytes(ReservationAllocator<size_t>(metadata)),
          statistics(ReservationAllocator<LeafLayoutStats>(metadata))
    {
    }

    ReservedVector<OwnedKey> separators;
    // An unvisited suffix has no size entry; every other output is exact.
    ReservedVector<size_t>           primary_bytes;
    ReservedVector<LeafLayoutStats>  statistics;
    std::shared_ptr<const PageState> source;
};

// Bounded split planning preserves exact output metrics for publication.
class SplitWindowCursor
{
  public:
    SplitWindowCursor(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits);
    Status advance();
    // A work/output window yields without ending the complete flush layout.
    Status advance_complete();

    [[nodiscard]] bool done() const noexcept
    {
        return done_;
    }

    [[nodiscard]] std::optional<BaseLoadRequest> needs_base() const;

    [[nodiscard]] const SplitWindowLayout &layout() const noexcept
    {
        return layout_;
    }

    SplitWindowLayout take_layout()
    {
        return std::move(layout_);
    }

  private:
    Status                           advance(bool complete);
    std::shared_ptr<const PageState> page_;
    PageMaintenanceLimits            limits_;
    SplitWindowLayout                layout_;
    LogicalPageCursor                cursor_;
    LeafLayoutStats                  current_stats_;
    size_t                           current_bytes_   = kFrameHeaderSize + kFrameTrailerSize;
    size_t                           separator_bytes_ = 0;
    size_t                           entries_         = 0;
    bool                             done_            = false;
};

Status plan_split_window(const std::shared_ptr<const PageState> &page, const PageMaintenanceLimits &limits,
                         SplitWindowLayout *layout, MaintenancePublicationResult *result);
MaintenancePublicationResult build_split_layout_publication(std::shared_ptr<const PublishedTreeState> view,
                                                            uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                            const MaintenancePublicationOptions &options,
                                                            const SplitWindowLayout             &layout);
} // namespace crowdb::tree::detail
