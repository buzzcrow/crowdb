// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "tree_metrics.h"

#include <map>

namespace crowdb::tree
{
// Transient operations owned by one captured write/rebase pipeline. Durable
// page descriptors never retain these timers or their reserved metadata.
class WritebackMetrics
{
  public:
    WritebackMetrics(std::shared_ptr<TreeWorkMetrics>                      metrics,
                     const std::shared_ptr<detail::AllocationReservation> &metadata, size_t max_pages);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t pages) noexcept;
    [[nodiscard]] Status                       begin(uint64_t pid);
    void                                       finish(uint64_t pid, TreeOutcome outcome) noexcept;
    [[nodiscard]] bool                         finish_some(TreeOutcome outcome) noexcept;
    [[nodiscard]] bool                         retire_step() noexcept;

  private:
    using Entry = std::pair<const uint64_t, TreeMetricsOperation>;
    std::shared_ptr<TreeWorkMetrics>                                                           metrics_;
    std::map<uint64_t, TreeMetricsOperation, std::less<>, detail::ReservationAllocator<Entry>> pending_;
    size_t                                                                                     max_pages_;
};
} // namespace crowdb::tree
