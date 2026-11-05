// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_runtime_internal.h"

#include <cassert>

namespace crowdb::tree::detail
{
FlushRuntime::ProgressState::~ProgressState()
{
    assert(!active.load(std::memory_order_acquire));
    detach();
}

void FlushRuntime::ProgressState::detach() noexcept
{
    if (detached.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    runtime->slots[slot].occupied.store(false, std::memory_order_release);
    lane->queued_.fetch_sub(1, std::memory_order_relaxed);
    runtime->queue_permits.fetch_sub(1, std::memory_order_relaxed);
}

void FlushRuntime::ProgressState::schedule(const std::shared_ptr<const void> &owner) noexcept
{
    // The registration owns the cell and its queue permits for its lifetime.
    // Connecting a static-pool scheduler constructs only the inline operation.
    runtime->lifecycle->fetch_add(1, std::memory_order_acq_rel);
    jobs.fetch_add(1, std::memory_order_acq_rel);
    lane->jobs_.fetch_add(1, std::memory_order_acq_rel);
    auto *job = ::new (static_cast<void *>(runtime->slots[slot].storage.data()))
        State::Job(runtime, slot, lane, task, context, weak_from_this().lock(), owner);
    runtime->queued.fetch_add(1, std::memory_order_relaxed);
    runtime->submitted.fetch_add(1, std::memory_order_relaxed);
    stdexec::start(job->operation);
}

void FlushRuntime::ProgressState::request(const std::shared_ptr<const void> &owner) noexcept
{
    assert(owner);
    auto value = ingress.load(std::memory_order_acquire);
    do {
        if ((value & State::kClosed) != 0) {
            return;
        }
    } while (!ingress.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    requested.store(true, std::memory_order_release);
    if (!active.exchange(true, std::memory_order_acq_rel)) {
        schedule(owner);
    }
    ingress.fetch_sub(1, std::memory_order_acq_rel);
    ingress.notify_all();
}

void FlushRuntime::ProgressState::complete(const std::shared_ptr<const void> &owner) noexcept
{
    // Pair with request's exchange: either this RMW acquires the preceding
    // requested publication, or the producer observes idle and schedules it.
    // A release store followed by an independent load can lose both wakes.
    active.exchange(false, std::memory_order_acq_rel);
    if (requested.load(std::memory_order_acquire) && !active.exchange(true, std::memory_order_acq_rel)) {
        schedule(owner);
    }
    jobs.fetch_sub(1, std::memory_order_acq_rel);
    jobs.notify_all();
}

void FlushRuntime::ProgressState::drain() noexcept
{
    auto value = jobs.load(std::memory_order_acquire);
    while (value != 0) {
        jobs.wait(value, std::memory_order_acquire);
        value = jobs.load(std::memory_order_acquire);
    }
}

void FlushRuntime::ProgressState::close_and_drain() noexcept
{
    auto value = ingress.fetch_or(State::kClosed, std::memory_order_acq_rel) | State::kClosed;
    while ((value & ~State::kClosed) != 0) {
        ingress.wait(value, std::memory_order_acquire);
        value = ingress.load(std::memory_order_acquire);
    }
    drain();
    detach();
}

FlushRuntime::Progress::Progress(std::shared_ptr<ProgressState> state) : state_(std::move(state))
{
}

void FlushRuntime::Progress::request(std::shared_ptr<const void> owner) noexcept
{
    state_->request(owner);
}

void FlushRuntime::Progress::drain() noexcept
{
    state_->drain();
}

void FlushRuntime::Progress::close_and_drain() noexcept
{
    state_->close_and_drain();
}

size_t FlushRuntime::progress_allocation_bound() noexcept
{
    return shared_allocation_bound<ProgressState>() + shared_allocation_bound<Progress>();
}

Status FlushRuntime::reserve_progress(std::shared_ptr<Lane> lane, Task task, void *context,
                                      std::shared_ptr<AllocationBudget> budget, std::shared_ptr<Progress> *out)
{
    if (!lane || task == nullptr || out == nullptr) {
        return Status::invalid_argument("runtime progress requires lane, task and output");
    }
    auto value = state_->lifecycle->load(std::memory_order_acquire);
    do {
        if ((value & State::kClosed) != 0) {
            return Status::unavailable("flush runtime closed");
        }
    } while (!state_->lifecycle->compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    const bool lane_acquired = flush_runtime_acquire(lane->queued_, lane->limit_);
    if (!lane_acquired || !flush_runtime_acquire(state_->queue_permits, state_->queue_limit)) {
        if (lane_acquired) {
            lane->queued_.fetch_sub(1, std::memory_order_relaxed);
        }
        state_->finish();
        return Status::resource_exhausted("runtime progress queue credit unavailable");
    }
    const auto                     slot = state_->claim_slot();
    std::shared_ptr<ProgressState> progress;
    Status                         status = Status::Ok();
    try {
        if (slot == SIZE_MAX) {
            throw std::bad_alloc();
        }
        progress = std::allocate_shared<ProgressState>(MetadataAllocator<ProgressState>(budget), state_.get(), slot,
                                                       lane, task, context);
        *out     = std::allocate_shared<Progress>(MetadataAllocator<Progress>(budget), progress);
    }
    catch (const std::bad_alloc &) {
        if (!progress) {
            if (slot != SIZE_MAX) {
                state_->slots[slot].occupied.store(false, std::memory_order_release);
            }
            lane->queued_.fetch_sub(1, std::memory_order_relaxed);
            state_->queue_permits.fetch_sub(1, std::memory_order_relaxed);
        }
        status = Status::resource_exhausted("runtime progress allocation rejected");
    }
    progress.reset();
    state_->finish();
    return status;
}
} // namespace crowdb::tree::detail
