// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/memory_budget.h"
#include "stdexec_adapter.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace crowdb::tree::detail
{

// CPU steps must return instead of waiting for children or I/O. Closing and
// destroying the runtime belongs to the host, never one of its workers.
class FlushRuntime
{
    struct ProgressState;

  public:
    struct Snapshot
    {
        uint64_t queued;
        uint64_t inflight;
        uint64_t submitted;
        uint64_t completed;
        uint64_t rejected;
        uint64_t queue_ns;
        uint64_t execute_ns;
    };

    class Lane
    {
      public:
        explicit Lane(size_t queue_limit) : limit_(queue_limit)
        {
        }

        // Host-thread boundary after producers have joined. Includes the
        // scheduler tail after user completion and context-owner release.
        void drain() noexcept;

        [[nodiscard]] size_t queue_permits() const noexcept
        {
            return queued_.load(std::memory_order_relaxed);
        }

      private:
        friend class FlushRuntime;
        size_t              limit_;
        std::atomic<size_t> queued_{0};
        std::atomic<size_t> jobs_{0};
    };

    using Task = void (*)(void *) noexcept;

    class Progress
    {
      public:
        explicit Progress(std::shared_ptr<ProgressState> state);
        // Every request for this registration uses the same context owner.
        // Coalesced requests retain the currently queued/running owner.
        void request(std::shared_ptr<const void> owner) noexcept;
        // Host-thread only. Drain this registration without waiting for other
        // Trees. Producers must stop before drain can be used as a boundary.
        void drain() noexcept;
        // Reject late requests, drain accepted work and return the fixed slot.
        // The closed handle may outlive the runtime. Do not call from its task.
        void close_and_drain() noexcept;

      private:
        std::shared_ptr<ProgressState> state_;
    };

    FlushRuntime(size_t workers, size_t queue_limit, std::shared_ptr<AllocationBudget> budget = {});
    ~FlushRuntime();
    FlushRuntime(const FlushRuntime &)            = delete;
    FlushRuntime &operator=(const FlushRuntime &) = delete;

    // No callback on rejection. The context must live through task return.
    // A lane caps queued work from one Tree; executing steps release that
    // credit before starting, so a single-worker nested submission progresses.
    [[nodiscard]] Status try_submit(std::shared_ptr<Lane> lane, Task task, void *context);
    // Registration reserves one real job slot and one lane/global queue
    // credit. Requests never allocate or fail because ordinary queues fill.
    // Registered work remains usable after close to finish admitted work.
    // The host joins producers and releases registrations before destroying
    // this runtime; drain includes all queued/running progress continuations.
    [[nodiscard]] Status reserve_progress(std::shared_ptr<Lane> lane, Task task, void *context,
                                          std::shared_ptr<AllocationBudget> budget, std::shared_ptr<Progress> *out);
    [[nodiscard]] static size_t progress_allocation_bound() noexcept;
    // Owned state, fixed job cells and lifecycle control. The host separately
    // provisions the shared executor's thread/bootstrap storage.
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t workers, size_t queue_limit) noexcept;
    void                                       close() noexcept;
    void                                       drain() noexcept;
    [[nodiscard]] Snapshot                     snapshot() const noexcept;
    [[nodiscard]] size_t                       worker_count() const noexcept;
    [[nodiscard]] size_t                       queue_permits() const noexcept;
    [[nodiscard]] static bool                  on_worker_thread() noexcept;

  private:
    struct State;
    AllocationLease        state_credit_;
    std::unique_ptr<State> state_;
};

// A dynamically sized range has a fixed number of reusable operation slots.
// Completion is copyable and retains the join until late transports release
// it; duplicates and stale slot generations cannot finish another child.
class BoundedRangeJoin
{
    struct State;

  public:
    class Completion
    {
      public:
        void operator()(Status status = Status::Ok()) const noexcept;

      private:
        friend struct State;

        Completion(std::shared_ptr<State> state, size_t slot, uint64_t generation)
            : state_(std::move(state)),
              slot_(slot),
              generation_(generation)
        {
        }

        std::shared_ptr<State> state_;
        size_t                 slot_;
        uint64_t               generation_;
    };

    using StartChild    = std::function<void(size_t, Completion)>;
    using StartFunction = void (*)(void *, size_t, Completion);

    // start_child initiates asynchronous work or a bounded CPU step. It may
    // complete inline; it must not wait. Throwing is converted to an error.
    BoundedRangeJoin(size_t ranges, size_t parallelism, StartChild start_child,
                     std::shared_ptr<AllocationBudget> budget = {});
    // The connected receiver retains context through terminal; late duplicate
    // completions never start another child or dereference this context.
    BoundedRangeJoin(size_t ranges, size_t parallelism, void *context, StartFunction start_child,
                     std::shared_ptr<AllocationBudget> budget = {});
    ~BoundedRangeJoin();
    BoundedRangeJoin(const BoundedRangeJoin &)            = delete;
    BoundedRangeJoin &operator=(const BoundedRangeJoin &) = delete;

    [[nodiscard]] CallbackSender               sender() noexcept;
    void                                       request_stop() noexcept;
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t parallelism) noexcept;

  private:
    static void            submit(void *source, CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};

} // namespace crowdb::tree::detail
