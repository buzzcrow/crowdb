// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "resource_observation.h"

#include <cassert>

namespace crowdb::tree
{
ResourceObservation::ResourceObservation(const std::shared_ptr<AllocationBudget> &budget, TreeResourceMetric metric,
                                         uint64_t bytes) noexcept
    : metrics_(budget ? budget->resource_metrics() : nullptr),
      metric_(metric)
{
    begin(bytes);
}

ResourceObservation::~ResourceObservation()
{
    reset();
}

void ResourceObservation::begin(uint64_t bytes) noexcept
{
    assert(bytes_.load(std::memory_order_relaxed) == 0);
    if (metrics_ && bytes != 0) {
        const bool valid = metrics_->add(metric_, bytes);
        assert(valid);
        if (valid) {
            bytes_.store(bytes, std::memory_order_release);
        }
    }
}

void ResourceObservation::reset() noexcept
{
    const auto bytes = bytes_.exchange(0, std::memory_order_acq_rel);
    if (bytes != 0) {
        const bool valid = metrics_->remove(metric_, bytes);
        assert(valid);
        (void)valid;
    }
}
} // namespace crowdb::tree
