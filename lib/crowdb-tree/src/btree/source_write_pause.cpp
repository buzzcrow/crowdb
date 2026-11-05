// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_write_budget.h"

#include <cassert>

namespace crowdb::tree
{
bool SourceWriteBudget::enter_bind() noexcept
{
    auto value = binders_.load(std::memory_order_acquire);
    for (;;) {
        while ((value & kPaused) == 0 && value != kPaused - 1) {
            if (binders_.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel)) {
                return true;
            }
        }
        pause_waiter_.store(true, std::memory_order_release);
        value = binders_.load(std::memory_order_acquire);
        if ((value & kPaused) != 0 || value == kPaused - 1) {
            return false;
        }
        // Resume won before waiter publication. Retry the bind instead of
        // waiting for an event which has already passed.
    }
}

void SourceWriteBudget::leave_bind() noexcept
{
    if (binders_.fetch_sub(1, std::memory_order_acq_rel) == kPaused + 1) {
        const auto complete = pause_complete_.load(std::memory_order_acquire);
        auto      *context  = pause_context_;
        complete(context);
    }
}

bool SourceWriteBudget::pause(PauseComplete complete, void *context) noexcept
{
    assert(complete != nullptr);
    // The controller and soft rotation compete for one reversible pause.
    // Publish callback storage before the last binder may deliver it.
    PauseComplete expected = nullptr;
    while (!pause_complete_.compare_exchange_strong(expected, complete, std::memory_order_acq_rel)) {
        pause_waiter_.store(true, std::memory_order_release);
        if (pause_complete_.load(std::memory_order_acquire) != nullptr) {
            return false;
        }
        expected = nullptr;
    }
    pause_context_      = context;
    const auto previous = binders_.fetch_or(kPaused, std::memory_order_acq_rel);
    if (previous == 0) {
        complete(context);
    }
    return true;
}

bool SourceWriteBudget::resume() noexcept
{
    assert(binders_.load(std::memory_order_acquire) == kPaused);
    binders_.store(0, std::memory_order_release);
    pause_complete_.store(nullptr, std::memory_order_release);
    return pause_waiter_.exchange(false, std::memory_order_acq_rel);
}

bool SourceWriteBudget::paused() const noexcept
{
    return (binders_.load(std::memory_order_acquire) & kPaused) != 0;
}

WriteFootprint SourceWriteBudget::paused_bound() const noexcept
{
    assert(binders_.load(std::memory_order_acquire) == kPaused);
    // Returned unused permits can only reduce these ordinary totals. Gap
    // admission remains open, so its complete maximum is always included.
    const auto add = [](size_t value, size_t allowance) {
        return allowance > SIZE_MAX - value ? SIZE_MAX : value + allowance;
    };
    return {.entries   = add(entries_.load(std::memory_order_acquire), options_.max_gap_entries),
            .key_bytes = add(key_bytes_.load(std::memory_order_acquire), options_.max_gap_key_bytes)};
}
} // namespace crowdb::tree
