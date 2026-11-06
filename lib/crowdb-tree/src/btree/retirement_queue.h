// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_bound.h"
#include "btree/flush_runtime.h"

namespace crowdb::tree::detail
{
class RetirementQueue;

class RetirementQueue : public std::enable_shared_from_this<RetirementQueue>
{
  public:
    static constexpr size_t kNodesPerStep = 64;

    struct Snapshot
    {
        size_t enqueued;
        size_t freed;
        size_t cpu_steps;
        size_t max_step_nodes;
    };

    static Status create(FlushRuntime &runtime, std::shared_ptr<FlushRuntime::Lane> lane,
                         const std::shared_ptr<AllocationBudget> &budget, std::shared_ptr<RetirementQueue> *out);
    [[nodiscard]] static size_t allocation_bound() noexcept;
    ~RetirementQueue();
    void retire(RetirementNode *node) noexcept;
    // Host-only, before the runtime is destroyed. This joins the reserved
    // registration. Late owners keep only this closed queue and allocation
    // accounts, and free iteratively without touching an executor.
    void                   close_and_drain() noexcept;
    [[nodiscard]] Snapshot snapshot() const noexcept;

  private:
    static void run(void *context) noexcept;
    enum class Step : uint8_t { kBusy, kEmpty, kWork };
    Step               step(size_t limit) noexcept;
    void               drain_host() noexcept;
    [[nodiscard]] bool pending() const noexcept;

    std::shared_ptr<FlushRuntime::Progress> progress_;
    std::atomic<RetirementNode *>           incoming_{nullptr};
    std::atomic_flag                        consumer_ = ATOMIC_FLAG_INIT;
    std::atomic<bool>                       fallback_queued_{false};
    RetirementQueue                        *fallback_next_ = nullptr;
    std::shared_ptr<RetirementQueue>        fallback_owner_;
    // 0 accepts CPU requests; 1 routes arrivals to the closing host; 2 uses
    // the caller's iterative fallback after the executor is detached.
    std::atomic<unsigned> state_{0};
    std::atomic<size_t>   enqueued_{0}, freed_{0}, cpu_steps_{0}, max_step_nodes_{0};
};
} // namespace crowdb::tree::detail
