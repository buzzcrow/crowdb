// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "allocation_bound.h"
#include "flush_runtime_internal.h"

#include <cassert>
#include <stdexcept>

namespace crowdb::tree::detail
{
namespace
{
thread_local bool executing_runtime_job = false;

struct RuntimeWorkerScope
{
    RuntimeWorkerScope() : previous(executing_runtime_job)
    {
        executing_runtime_job = true;
    }

    ~RuntimeWorkerScope()
    {
        executing_runtime_job = previous;
    }

    bool previous;
};
} // namespace

bool FlushRuntime::on_worker_thread() noexcept
{
    return executing_runtime_job;
}

void FlushRuntime::State::Receiver::set_value() const noexcept
{
    RuntimeWorkerScope                                  worker_scope;
    std::unique_ptr<Job, decltype(&State::release_job)> owned(job, &State::release_job);
    auto                                               *state   = owned->state;
    auto                                                started = flush_runtime_now_ns();
    state->inflight.fetch_add(1, std::memory_order_relaxed);
    state->queued.fetch_sub(1, std::memory_order_relaxed);
    if (!owned->progress) {
        owned->lane->queued_.fetch_sub(1, std::memory_order_relaxed);
        state->queue_permits.fetch_sub(1, std::memory_order_relaxed);
    }
    else {
        owned->progress->requested.exchange(false, std::memory_order_acq_rel);
    }
    state->queue_ns.fetch_add(started - owned->queued_at, std::memory_order_relaxed);
    owned->task(owned->context);
    auto progress = owned->progress;
    auto owner    = std::move(owned->context_owner);
    auto lane     = owned->lane;
    owned.reset();
    if (progress) {
        progress->complete(owner);
    }
    owner.reset();
    progress.reset();
    // Ownership cleanup still occupies this worker. Count it before publishing
    // completion, otherwise resource samples hide expensive retirement tails.
    state->execute_ns.fetch_add(flush_runtime_now_ns() - started, std::memory_order_relaxed);
    state->completed.fetch_add(1, std::memory_order_relaxed);
    state->inflight.fetch_sub(1, std::memory_order_relaxed);
    state->finish();
    lane->jobs_.fetch_sub(1, std::memory_order_acq_rel);
    lane->jobs_.notify_all();
}

void FlushRuntime::Lane::drain() noexcept
{
    auto count = jobs_.load(std::memory_order_acquire);
    while (count != 0) {
        jobs_.wait(count, std::memory_order_acquire);
        count = jobs_.load(std::memory_order_acquire);
    }
}

std::optional<size_t> FlushRuntime::allocation_bound(size_t workers, size_t queue_limit) noexcept
{
    if (workers == 0 || workers > UINT16_MAX || queue_limit == 0 || workers > SIZE_MAX - queue_limit) {
        return {};
    }
    return AllocationBound(sizeof(State))
        .add(exec::static_thread_pool::allocation_bound(static_cast<uint32_t>(workers)))
        .add_product(workers, PoolStorage::worker_bytes())
        .add_product(workers + queue_limit, sizeof(State::Slot))
        .add_product(workers, sizeof(exec::static_thread_pool::scheduler))
        .add(shared_allocation_bound<std::atomic<uint64_t>>())
        .value();
}

FlushRuntime::FlushRuntime(size_t workers, size_t queue_limit, std::shared_ptr<AllocationBudget> budget)
{
    if (!allocation_bound(workers, queue_limit)) {
        throw std::invalid_argument("flush runtime requires workers and queue capacity");
    }
    state_credit_ = AllocationLease(budget, sizeof(State), AllocationKind::kMetadata);
    state_        = std::make_unique<State>(workers, queue_limit, budget);
}

FlushRuntime::~FlushRuntime()
{
    close();
    drain();
    assert(state_->queue_permits.load(std::memory_order_acquire) == 0 &&
           "host must release progress registrations before runtime destruction");
}

Status FlushRuntime::try_submit(std::shared_ptr<Lane> lane, Task task, void *context)
{
    if (!lane || task == nullptr) {
        return Status::invalid_argument("flush runtime requires a lane and task");
    }
    auto value = state_->lifecycle->load(std::memory_order_acquire);
    do {
        if ((value & State::kClosed) != 0) {
            state_->rejected.fetch_add(1, std::memory_order_relaxed);
            return Status::unavailable("flush runtime closed");
        }
    } while (!state_->lifecycle->compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));

    bool lane_acquired = flush_runtime_acquire(lane->queued_, lane->limit_);
    if (!lane_acquired || !flush_runtime_acquire(state_->queue_permits, state_->queue_limit)) {
        if (lane_acquired) {
            lane->queued_.fetch_sub(1, std::memory_order_relaxed);
        }
        state_->rejected.fetch_add(1, std::memory_order_relaxed);
        state_->finish();
        return Status::resource_exhausted("flush runtime queue full");
    }
    auto *job = state_->prepare_job(lane, task, context);
    if (job == nullptr) {
        state_->queue_permits.fetch_sub(1, std::memory_order_relaxed);
        lane->queued_.fetch_sub(1, std::memory_order_relaxed);
        state_->rejected.fetch_add(1, std::memory_order_relaxed);
        state_->finish();
        return Status::resource_exhausted("flush runtime job slots unavailable");
    }
    state_->queued.fetch_add(1, std::memory_order_relaxed);
    state_->submitted.fetch_add(1, std::memory_order_relaxed);
    lane->jobs_.fetch_add(1, std::memory_order_acq_rel);
    stdexec::start(job->operation);
    return Status::Ok();
}

void FlushRuntime::close() noexcept
{
    state_->lifecycle->fetch_or(State::kClosed, std::memory_order_acq_rel);
}

void FlushRuntime::drain() noexcept
{
    auto value = state_->lifecycle->load(std::memory_order_acquire);
    while ((value & ~State::kClosed) != 0) {
        state_->lifecycle->wait(value, std::memory_order_acquire);
        value = state_->lifecycle->load(std::memory_order_acquire);
    }
}

FlushRuntime::Snapshot FlushRuntime::snapshot() const noexcept
{
    return {.queued     = state_->queued.load(std::memory_order_relaxed),
            .inflight   = state_->inflight.load(std::memory_order_relaxed),
            .submitted  = state_->submitted.load(std::memory_order_relaxed),
            .completed  = state_->completed.load(std::memory_order_relaxed),
            .rejected   = state_->rejected.load(std::memory_order_relaxed),
            .queue_ns   = state_->queue_ns.load(std::memory_order_relaxed),
            .execute_ns = state_->execute_ns.load(std::memory_order_relaxed)};
}

size_t FlushRuntime::worker_count() const noexcept
{
    return state_->worker_count;
}

size_t FlushRuntime::queue_permits() const noexcept
{
    return state_->queue_permits.load(std::memory_order_relaxed);
}

} // namespace crowdb::tree::detail
