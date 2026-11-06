// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "epoch_internal.h"

#include <cassert>
#include <utility>

namespace crowdb::tree
{
bool EpochManager::retire_backend_step() noexcept
{
    if (!lifetime_) {
        return true;
    }
    // Block directories are created before runtime admission. Their short
    // guards cannot survive the directory's final backend-owner handoff.
    assert(!lifetime_->queue && lifetime_.use_count() == 1);
    if (lifetime_->participants == nullptr) {
        lifetime_->participants = participants_.exchange(nullptr, std::memory_order_relaxed);
    }
    if (lifetime_->pending == nullptr) {
        lifetime_->pending = deferred_.exchange(nullptr, std::memory_order_relaxed);
    }
    if (lifetime_->collected == nullptr) {
        lifetime_->collected = std::exchange(collected_, nullptr);
    }
    if (!retired_.empty()) {
        lifetime_->retired = std::move(retired_);
    }
    return lifetime_->retire_step();
}
} // namespace crowdb::tree
