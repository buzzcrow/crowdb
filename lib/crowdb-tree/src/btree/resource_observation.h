// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "tree_metrics.h"

namespace crowdb::tree
{
// Embedded in the actual buffer/operation owner. Independent of the Tree,
// and safe to finish once from a callback or final owner destruction.
class ResourceObservation
{
  public:
    ResourceObservation(const std::shared_ptr<AllocationBudget> &budget, TreeResourceMetric metric,
                        uint64_t bytes = 0) noexcept;
    ~ResourceObservation();
    ResourceObservation(const ResourceObservation &)            = delete;
    ResourceObservation &operator=(const ResourceObservation &) = delete;
    // Called before making the owner visible to a completion callback.
    void begin(uint64_t bytes) noexcept;
    void reset() noexcept;

  private:
    std::shared_ptr<TreeResourceMetrics> metrics_;
    TreeResourceMetric                   metric_;
    std::atomic<uint64_t>                bytes_{0};
};
} // namespace crowdb::tree
