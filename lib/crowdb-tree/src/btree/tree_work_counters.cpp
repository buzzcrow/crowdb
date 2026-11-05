// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_work_counters.h"

#include "allocation_reservation.h"
#include "tree_metrics.h"

namespace crowdb::tree
{
void TreeWorkCounters::add(TreeWorkCounter counter, uint64_t amount, TreeWorkCause cause) noexcept
{
    const auto c = static_cast<size_t>(counter);
    const auto t = static_cast<size_t>(cause);
    if (c < kTreeWorkCounterCount && t < kTreeWorkCauseCount) {
        values_[t][c].fetch_add(amount, std::memory_order_relaxed);
    }
}

TreeCounterTotals TreeWorkCounters::snapshot() const noexcept
{
    TreeCounterTotals result{};
    for (size_t t = 0; t < kTreeWorkCauseCount; ++t) {
        for (size_t c = 0; c < kTreeWorkCounterCount; ++c) {
            result[t][c] = values_[t][c].load(std::memory_order_relaxed);
        }
    }
    return result;
}

void record_work(const std::shared_ptr<TreeWorkMetrics> &metrics, TreeWorkCounter counter, uint64_t amount,
                 TreeWorkCause cause) noexcept
{
    if (metrics && amount != 0) {
        metrics->counters.add(counter, amount, cause);
    }
}

void record_work(const std::shared_ptr<detail::AllocationReservation> &metadata, TreeWorkCounter counter,
                 uint64_t amount, TreeWorkCause cause) noexcept
{
    if (metadata && amount != 0) {
        record_work(metadata->account()->work_metrics(), counter, amount, cause);
    }
}

void record_work(const std::shared_ptr<AllocationBudget> &budget, TreeWorkCounter counter, uint64_t amount,
                 TreeWorkCause cause) noexcept
{
    if (budget && amount != 0) {
        record_work(budget->retirement_metrics(), counter, amount, cause);
    }
}
} // namespace crowdb::tree
