// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_state.h"

#include <algorithm>
#include <cassert>

namespace crowdb::tree
{
size_t PublicationCoordinator::start_maintenance(uint64_t started_at_ns) noexcept
{
    const auto started = std::max(uint64_t{1}, started_at_ns);
    for (size_t i = 0; i < maintenance_times_.size(); ++i) {
        uint64_t empty = 0;
        if (maintenance_times_[i].compare_exchange_strong(empty, started, std::memory_order_release,
                                                          std::memory_order_relaxed)) {
            return i;
        }
    }
    return kMaintenanceSlots;
}

void PublicationCoordinator::finish_maintenance(size_t slot) noexcept
{
    assert(slot < kMaintenanceSlots);
    maintenance_times_[slot].store(0, std::memory_order_release);
}

uint64_t PublicationCoordinator::oldest_maintenance_since_ns() const noexcept
{
    uint64_t oldest = 0;
    for (const auto &slot : maintenance_times_) {
        const auto started = slot.load(std::memory_order_acquire);
        if (started != 0 && (oldest == 0 || started < oldest)) {
            oldest = started;
        }
    }
    return oldest;
}

} // namespace crowdb::tree
