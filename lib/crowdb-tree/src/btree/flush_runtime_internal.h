// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_runtime.h"
#include "runtime/pool_storage.h"

#include <array>
#include <chrono>
#include <exec/static_thread_pool.hpp>
#include <vector>

namespace crowdb::tree::detail
{
inline bool flush_runtime_acquire(std::atomic<size_t> &counter, size_t limit) noexcept
{
    auto value = counter.load(std::memory_order_relaxed);
    while (value < limit) {
        if (counter.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

inline uint64_t flush_runtime_now_ns() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct FlushRuntime::State
{
    static constexpr uint64_t kClosed = uint64_t{1} << 63;

    struct Job;

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        Job *job;
        void set_value() const noexcept;
    };

    struct Job
    {
        using Operation = stdexec::connect_result_t<
            decltype(stdexec::schedule(std::declval<exec::static_thread_pool &>().get_scheduler())), Receiver>;

        Job(State *owner, size_t slot_index, std::shared_ptr<Lane> tree_lane, Task work, void *argument,
            std::shared_ptr<ProgressState> progress_owner = {},
            std::shared_ptr<const void>    argument_owner = {}) noexcept
            : state(owner),
              slot(slot_index),
              lane(std::move(tree_lane)),
              task(work),
              context(argument),
              progress(std::move(progress_owner)),
              context_owner(std::move(argument_owner)),
              queued_at(flush_runtime_now_ns()),
              operation(stdexec::connect(stdexec::schedule(state->schedulers[slot_index % state->schedulers.size()]),
                                         Receiver{this}))
        {
        }

        State                         *state;
        size_t                         slot;
        std::shared_ptr<Lane>          lane;
        Task                           task;
        void                          *context;
        std::shared_ptr<ProgressState> progress;
        std::shared_ptr<const void>    context_owner;
        uint64_t                       queued_at;
        Operation                      operation;
    };

    struct Slot
    {
        std::atomic<bool> occupied{false};
        alignas(Job) std::array<std::byte, sizeof(Job)> storage;
    };

    State(size_t workers, size_t limit, const std::shared_ptr<AllocationBudget> &budget)
        : pool_storage(budget),
          pool(static_cast<uint32_t>(workers), {}, exec::no_numa_policy{}, &pool_storage,
               pool_storage.thread_factory()),
          schedulers(MetadataAllocator<exec::static_thread_pool::scheduler>(budget)),
          worker_count(workers),
          queue_limit(limit),
          slots(workers + limit, MetadataAllocator<Slot>(budget)),
          lifecycle(std::allocate_shared<std::atomic<uint64_t>>(MetadataAllocator<std::atomic<uint64_t>>(budget), 0))
    {
        // Register on the bootstrap thread once. A cached target scheduler
        // submits through its existing shared MPSC remote queue, without
        // creating a queue for each new host or callback thread.
        schedulers.reserve(workers);
        for (size_t index = 0; index < workers; ++index) {
            schedulers.push_back(pool.get_scheduler_on_thread(index));
        }
    }

    [[nodiscard]] size_t claim_slot() noexcept
    {
        const auto first = next_slot.fetch_add(1, std::memory_order_relaxed) % slots.size();
        // Reserved progress and ordinary jobs share this fixed slot array.
        for (size_t offset = 0; offset < slots.size(); ++offset) {
            const auto index    = (first + offset) % slots.size();
            bool       expected = false;
            if (slots[index].occupied.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
                return index;
            }
        }
        return SIZE_MAX;
    }

    [[nodiscard]] Job *prepare_job(const std::shared_ptr<Lane> &lane, Task task, void *context) noexcept
    {
        const auto index = claim_slot();
        return index == SIZE_MAX ? nullptr
                                 : ::new (static_cast<void *>(slots[index].storage.data()))
                                       Job(this, index, lane, task, context);
    }

    static void release_job(Job *job) noexcept
    {
        auto      *state    = job->state;
        const auto index    = job->slot;
        const bool reserved = job->progress != nullptr;
        std::destroy_at(job);
        if (!reserved) {
            state->slots[index].occupied.store(false, std::memory_order_release);
        }
    }

    void finish() const noexcept
    {
        // Zero lets the host start teardown. Retain the notification word
        // until notification returns; pool teardown joins this receiver.
        auto completion = lifecycle;
        completion->fetch_sub(1, std::memory_order_acq_rel);
        completion->notify_all();
    }

    PoolStorage              pool_storage;
    exec::static_thread_pool pool;
    std::vector<exec::static_thread_pool::scheduler, MetadataAllocator<exec::static_thread_pool::scheduler>> schedulers;
    const size_t                               worker_count;
    size_t                                     queue_limit;
    std::vector<Slot, MetadataAllocator<Slot>> slots;
    std::atomic<size_t>                        next_slot{0};
    std::shared_ptr<std::atomic<uint64_t>>     lifecycle;
    std::atomic<size_t>                        queue_permits{0};
    std::atomic<size_t>                        queued{0};
    std::atomic<uint64_t>                      inflight{0};
    std::atomic<uint64_t>                      submitted{0};
    std::atomic<uint64_t>                      completed{0};
    std::atomic<uint64_t>                      rejected{0};
    std::atomic<uint64_t>                      queue_ns{0};
    std::atomic<uint64_t>                      execute_ns{0};
};

struct FlushRuntime::ProgressState : std::enable_shared_from_this<ProgressState>
{
    ProgressState(State *runtime, size_t index, std::shared_ptr<Lane> tree_lane, Task work, void *argument)
        : runtime(runtime),
          slot(index),
          lane(std::move(tree_lane)),
          task(work),
          context(argument)
    {
    }

    ~ProgressState();
    void request(const std::shared_ptr<const void> &owner) noexcept;
    void schedule(const std::shared_ptr<const void> &owner) noexcept;
    void complete(const std::shared_ptr<const void> &owner) noexcept;
    void drain() noexcept;
    void close_and_drain() noexcept;
    void detach() noexcept;

    State                *runtime;
    size_t                slot;
    std::shared_ptr<Lane> lane;
    Task                  task;
    void                 *context;
    std::atomic<bool>     active{false}, requested{false};
    std::atomic<uint64_t> ingress{0};
    std::atomic<size_t>   jobs{0};
    std::atomic<bool>     detached{false};
};

} // namespace crowdb::tree::detail
