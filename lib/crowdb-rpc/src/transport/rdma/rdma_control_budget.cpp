// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_control_stream.h"

#include <atomic>
#include <new>

namespace crowdb::rpc
{
struct ControlStreamBudgetState
{
    uint64_t              limit;
    std::function<void()> wake;
    std::atomic<uint64_t> used{0};
    std::atomic<uint64_t> peak{0};
    std::atomic<uint64_t> stalls{0};
};

ControlStreamBudget::ControlStreamBudget(uint64_t limit, std::function<void()> wake)
    : state_(std::make_shared<ControlStreamBudgetState>())
{
    state_->limit = limit;
    state_->wake  = std::move(wake);
}

ControlStreamLease::ControlStreamLease(std::shared_ptr<ControlStreamBudgetState> state, uint64_t bytes)
    : state_(std::move(state)),
      bytes_(bytes)
{
}

ControlStreamLease::~ControlStreamLease()
{
    state_->used.fetch_sub(bytes_, std::memory_order_acq_rel);
    if (state_->wake)
        state_->wake();
}

std::shared_ptr<ControlStreamLease> ControlStreamBudget::try_reserve(uint64_t bytes)
{
    uint64_t used = state_->used.load(std::memory_order_relaxed);
    do {
        if (bytes == 0 || bytes > state_->limit || used > state_->limit - bytes) {
            state_->stalls.fetch_add(1, std::memory_order_relaxed);
            return {};
        }
    } while (!state_->used.compare_exchange_weak(used, used + bytes, std::memory_order_acq_rel));
    uint64_t peak = state_->peak.load(std::memory_order_relaxed);
    while (peak < used + bytes && !state_->peak.compare_exchange_weak(peak, used + bytes, std::memory_order_relaxed)) {
    }
    // Build the lease before publishing its shared_ptr. If its control-block
    // allocation fails, shared_ptr deletes the lease and returns admission.
    ControlStreamLease *lease = nullptr;
    try {
        lease = new ControlStreamLease(state_, bytes);
    }
    catch (const std::bad_alloc &) {
        state_->used.fetch_sub(bytes, std::memory_order_acq_rel);
        if (state_->wake)
            state_->wake();
        return {};
    }
    try {
        return std::shared_ptr<ControlStreamLease>(lease);
    }
    catch (const std::bad_alloc &) {
        return {};
    }
}

uint64_t ControlStreamBudget::used() const
{
    return state_->used.load(std::memory_order_acquire);
}

uint64_t ControlStreamBudget::high_water() const
{
    return state_->peak.load(std::memory_order_relaxed);
}

uint64_t ControlStreamBudget::stalls() const
{
    return state_->stalls.load(std::memory_order_relaxed);
}

uint64_t ControlStreamBudget::limit() const
{
    return state_->limit;
}
} // namespace crowdb::rpc
