// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_runtime.h"

#include <cassert>
#include <stdexcept>
#include <vector>

namespace crowdb::tree::detail
{
struct BoundedRangeJoin::State : std::enable_shared_from_this<State>
{
    struct Slot
    {
        // Low bits: idle=0, active=1, completing=2, complete=3.
        std::atomic<uint64_t> phase{0};
        Status                status;
    };

    State(size_t count, size_t width, StartChild work, const std::shared_ptr<AllocationBudget> &budget)
        : ranges(count),
          slots(width, MetadataAllocator<Slot>(budget)),
          start_child(std::move(work))
    {
    }

    void drive() noexcept
    {
        if (notifications.fetch_add(1, std::memory_order_acq_rel) != 0) {
            return;
        }
        auto keep_alive = weak_from_this().lock();
        assert(keep_alive);
        do {
            advance();
        } while (notifications.fetch_sub(1, std::memory_order_acq_rel) != 1);
    }

    void advance() noexcept
    {
        if (!started.load(std::memory_order_acquire) || terminal) {
            return;
        }
        for (size_t i = 0; i < slots.size(); ++i) {
            auto &slot  = slots[i];
            auto  phase = slot.phase.load(std::memory_order_acquire);
            if ((phase & 3) == 3) {
                --pending;
                if (first_failure.load(std::memory_order_acquire) == i + 1) {
                    error = std::move(slot.status);
                }
                phase = (phase & ~uint64_t{3}) + 4;
                slot.phase.store(phase, std::memory_order_release);
            }
            if ((phase & 3) != 0 || next == ranges || first_failure.load(std::memory_order_acquire) != 0 ||
                stopped.load(std::memory_order_acquire)) {
                continue;
            }
            ++pending;
            slot.phase.store(phase | 1, std::memory_order_release);
            Completion completion(weak_from_this().lock(), i, phase);
            try {
                const auto index = next++;
                if (start_function != nullptr) {
                    start_function(child_context, index, completion);
                }
                else {
                    start_child(index, completion);
                }
            }
            catch (...) {
                completion(Status::internal_error());
            }
        }
        bool sealed = next == ranges || first_failure.load(std::memory_order_acquire) != 0 ||
                      stopped.load(std::memory_order_acquire);
        if (sealed && pending == 0) {
            terminal    = true;
            auto signal = CallbackSignal::kValue;
            if (!error.ok()) {
                signal = CallbackSignal::kError;
            }
            else if (stopped.load(std::memory_order_acquire)) {
                signal = CallbackSignal::kStopped;
            }
            complete(context, signal, std::move(error));
        }
    }

    size_t                                     ranges;
    std::vector<Slot, MetadataAllocator<Slot>> slots;
    StartChild                                 start_child;
    StartFunction                              start_function = nullptr;
    void                                      *child_context  = nullptr;
    std::atomic<size_t>                        notifications{0};
    std::atomic<bool>                          stopped{false};
    std::atomic<size_t>                        first_failure{0};
    std::atomic<bool>                          started{false};
    std::atomic_flag                           submitted = ATOMIC_FLAG_INIT;
    CallbackComplete                           complete  = nullptr;
    void                                      *context   = nullptr;
    size_t                                     next      = 0;
    size_t                                     pending   = 0;
    bool                                       terminal  = false;
    Status                                     error;
};

void BoundedRangeJoin::Completion::operator()(Status status) const noexcept
{
    auto &slot     = state_->slots[slot_];
    auto  expected = generation_ | 1;
    if (!slot.phase.compare_exchange_strong(expected, generation_ | 2, std::memory_order_acq_rel)) {
        return;
    }
    slot.status = std::move(status);
    if (!slot.status.ok()) {
        size_t first = 0;
        state_->first_failure.compare_exchange_strong(first, slot_ + 1, std::memory_order_acq_rel);
    }
    slot.phase.store(generation_ | 3, std::memory_order_release);
    state_->drive();
}

std::optional<size_t> BoundedRangeJoin::allocation_bound(size_t parallelism) noexcept
{
    return AllocationBound(sizeof(BoundedRangeJoin))
        .add(shared_allocation_bound<State>())
        .add_product(parallelism, sizeof(State::Slot))
        .value();
}

BoundedRangeJoin::BoundedRangeJoin(size_t ranges, size_t parallelism, StartChild start_child,
                                   std::shared_ptr<AllocationBudget> budget)
{
    if (parallelism == 0 || !start_child) {
        throw std::invalid_argument("range join requires operation slots and child starter");
    }
    state_ = std::allocate_shared<State>(MetadataAllocator<State>(budget), ranges, parallelism, std::move(start_child),
                                         budget);
}

BoundedRangeJoin::BoundedRangeJoin(size_t ranges, size_t parallelism, void *context, StartFunction start_child,
                                   std::shared_ptr<AllocationBudget> budget)
{
    if (parallelism == 0 || start_child == nullptr) {
        throw std::invalid_argument("range join requires operation slots and child starter");
    }
    state_ = std::allocate_shared<State>(MetadataAllocator<State>(budget), ranges, parallelism, StartChild{}, budget);
    state_->child_context  = context;
    state_->start_function = start_child;
}

BoundedRangeJoin::~BoundedRangeJoin() = default;

CallbackSender BoundedRangeJoin::sender() noexcept
{
    return {this, &BoundedRangeJoin::submit};
}

void BoundedRangeJoin::submit(void *source, CallbackComplete complete, void *context)
{
    auto state = static_cast<BoundedRangeJoin *>(source)->state_;
    if (state->submitted.test_and_set(std::memory_order_acq_rel)) {
        complete(context, CallbackSignal::kError, Status::invalid_argument("range join already started"));
        return;
    }
    state->complete = complete;
    state->context  = context;
    state->started.store(true, std::memory_order_release);
    state->drive();
}

void BoundedRangeJoin::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->drive();
}

} // namespace crowdb::tree::detail
