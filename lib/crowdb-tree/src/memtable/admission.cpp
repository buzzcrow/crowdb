// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/memtable/memtable.h"

#include <cassert>

namespace crowdb::tree
{
bool MemTable::try_enter() noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while ((state & kClosed) == 0 && state != kClosed - 1) {
        if (writers_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

bool MemTable::validate_open() noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while ((state & kClosed) == 0) {
        if (writers_.compare_exchange_weak(state, state, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void MemTable::leave() noexcept
{
    const auto previous = writers_.fetch_sub(1, std::memory_order_acq_rel);
    assert((previous & ~kClosed) != 0);
    if (previous == kClosed + 1) {
        writers_.notify_all();
    }
}

void MemTable::close() noexcept
{
    writers_.fetch_or(kClosed, std::memory_order_acq_rel);
    writers_.notify_all();
}

void MemTable::wait_frozen() const noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while (state != kClosed) {
        writers_.wait(state, std::memory_order_acquire);
        state = writers_.load(std::memory_order_acquire);
    }
}
} // namespace crowdb::tree
