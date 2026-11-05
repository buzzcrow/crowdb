// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "writeback_metrics.h"

#include "allocation_bound.h"

namespace crowdb::tree
{
WritebackMetrics::WritebackMetrics(std::shared_ptr<TreeWorkMetrics>                      metrics,
                                   const std::shared_ptr<detail::AllocationReservation> &metadata, size_t max_pages)
    : metrics_(std::move(metrics)),
      pending_(std::less<>{}, detail::ReservationAllocator<Entry>(metadata)),
      max_pages_(max_pages)
{
}

std::optional<size_t> WritebackMetrics::allocation_bound(size_t pages) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<WritebackMetrics>())
        .add_product(pages, sizeof(Entry) + (8 * sizeof(void *)))
        .value();
}

Status WritebackMetrics::begin(uint64_t pid)
{
    if (pending_.contains(pid)) {
        return Status::Ok();
    }
    if (pending_.size() >= max_pages_) {
        return Status::resource_exhausted("writeback metrics exceed the captured page bound");
    }
    pending_.try_emplace(pid, metrics_, TreeStage::PageWritebackPipeline, tree_metrics_now_ns());
    return Status::Ok();
}

void WritebackMetrics::finish(uint64_t pid, TreeOutcome outcome) noexcept
{
    const auto found = pending_.find(pid);
    if (found != pending_.end()) {
        (void)found->second.finish(outcome, tree_metrics_now_ns());
        pending_.erase(found);
    }
}

bool WritebackMetrics::finish_some(TreeOutcome outcome) noexcept
{
    for (size_t count = 0; count < 64 && !pending_.empty(); ++count) {
        const auto entry = pending_.begin();
        (void)entry->second.finish(outcome, tree_metrics_now_ns());
        pending_.erase(entry);
    }
    return pending_.empty();
}

bool WritebackMetrics::retire_step() noexcept
{
    return finish_some(TreeOutcome::Cancelled);
}
} // namespace crowdb::tree
