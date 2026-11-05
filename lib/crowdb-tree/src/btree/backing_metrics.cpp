// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backing_metrics.h"

#include "tree_metrics.h"

#include <cassert>
#include <utility>

namespace crowdb::tree
{
void BackingMetricExtent::activate(const std::shared_ptr<TreeResourceMetrics> &metrics, uint64_t capacity) noexcept
{
    assert(capacity_ == 0);
    metrics_  = metrics;
    capacity_ = capacity;
    allocated_.store(true, std::memory_order_relaxed);
    if (metrics) {
        const bool added = metrics->add(TreeResourceMetric::AllocatedBackingBytes, capacity);
        assert(added);
        (void)added;
    }
}

void BackingMetricExtent::mark_deleted() noexcept
{
    if (allocated_.exchange(false, std::memory_order_acq_rel)) {
        if (const auto metrics = metrics_.lock()) {
            const bool removed = metrics->remove(TreeResourceMetric::AllocatedBackingBytes, capacity_);
            assert(removed);
            (void)removed;
        }
    }
}

BackingMetricExtent::~BackingMetricExtent()
{
    mark_deleted();
}

} // namespace crowdb::tree
