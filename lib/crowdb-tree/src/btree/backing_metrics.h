// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>

namespace crowdb::tree
{
class TreeResourceMetrics;

// Exactly one observation per physical extent or pack. The backend embeds it
// in its paid allocation owner; page views share an alias of that same owner.
class BackingMetricExtent
{
  public:
    BackingMetricExtent() = default;
    ~BackingMetricExtent();
    BackingMetricExtent(const BackingMetricExtent &)            = delete;
    BackingMetricExtent &operator=(const BackingMetricExtent &) = delete;
    // Ends the physical observation after an authoritative Delete acknowledgement.
    void mark_deleted() noexcept;
    void activate(const std::shared_ptr<TreeResourceMetrics> &metrics, uint64_t capacity) noexcept;

  private:
    std::weak_ptr<TreeResourceMetrics> metrics_;
    uint64_t                           capacity_ = 0;
    std::atomic<bool>                  allocated_{false};
};
} // namespace crowdb::tree
