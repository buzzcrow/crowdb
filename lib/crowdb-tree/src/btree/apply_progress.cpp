// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_progress.h"

#include "allocation_bound.h"
#include "host_completion.h"

#include <cassert>

namespace crowdb::tree
{
struct ApplyProgress::State
{
    State(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata)
        : waiters(capacity, detail::ReservationAllocator<std::atomic<Request *>>(metadata))
    {
    }

    void notify() noexcept
    {
        if (notifications.fetch_add(1, std::memory_order_acq_rel) != 0) {
            return;
        }
        do {
            for (auto &slot : waiters) {
                auto *request = slot.load(std::memory_order_acquire);
                if (request == nullptr) {
                    continue;
                }
                const bool stopped =
                    closed.load(std::memory_order_acquire) || request->stopped_.load(std::memory_order_acquire);
                if (!stopped && sequence.load(std::memory_order_acquire) == request->observed_) {
                    continue;
                }
                slot.store(nullptr, std::memory_order_release);
                auto  callback = request->complete_;
                auto *context  = request->context_;
                request->pending_.store(false, std::memory_order_release);
                callback(context, stopped ? detail::CallbackSignal::kStopped : detail::CallbackSignal::kValue,
                         Status::Ok());
            }
        } while (notifications.fetch_sub(1, std::memory_order_acq_rel) != 1);
    }

    detail::ReservedVector<std::atomic<Request *>> waiters;
    std::atomic<uint64_t>                          sequence{0};
    std::atomic<size_t>                            notifications{0};
    std::atomic<bool>                              closed{false};
};

std::optional<size_t> ApplyProgress::allocation_bound(size_t capacity) noexcept
{
    if (capacity == 0) {
        return {};
    }
    return detail::AllocationBound(detail::shared_allocation_bound<ApplyProgress>() +
                                   detail::shared_allocation_bound<State>())
        .add_product(capacity, sizeof(std::atomic<Request *>))
        .value();
}

Status ApplyProgress::create(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata,
                             std::shared_ptr<ApplyProgress> *out)
try {
    if (out == nullptr || *out || !metadata || !allocation_bound(capacity)) {
        return Status::invalid_argument("apply progress requires bounded waiter storage and credit");
    }
    auto state = detail::reserved_shared<State>(metadata, capacity, metadata);
    *out       = detail::reserved_construct<ApplyProgress>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) ApplyProgress(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply progress waiter allocation failed");
}

uint64_t ApplyProgress::sequence() const noexcept
{
    return state_->sequence.load(std::memory_order_acquire);
}

Status ApplyProgress::wait(uint64_t observed_sequence) const
{
    struct Completion
    {
        detail::HostCompletion done;
        Status                 status;
    } completion;

    Request request(*this, observed_sequence);
    auto    callback = [](void *context, detail::CallbackSignal signal, Status status) {
        auto &result  = *static_cast<Completion *>(context);
        result.status = signal == detail::CallbackSignal::kStopped ? Status::unavailable("apply progress closed")
                                                                   : std::move(status);
        result.done.signal();
    };
    Request::submit(&request, callback, &completion);
    completion.done.wait();
    return completion.status;
}

ApplyProgress::~ApplyProgress()
{
    close();
}

void ApplyProgress::advance() noexcept
{
    auto state = state_;
    state->sequence.fetch_add(1, std::memory_order_acq_rel);
    state->notify();
}

void ApplyProgress::close() noexcept
{
    auto state = state_;
    state->closed.store(true, std::memory_order_release);
    state->notify();
}

ApplyProgress::Request::Request(const ApplyProgress &progress, uint64_t observed_sequence)
    : state_(progress.state_),
      observed_(observed_sequence)
{
}

ApplyProgress::Request::~Request()
{
    assert(!pending_.load(std::memory_order_acquire));
}

detail::CallbackSender ApplyProgress::Request::sender() noexcept
{
    return {this, &Request::submit};
}

void ApplyProgress::Request::request_stop() noexcept
{
    auto state = state_;
    stopped_.store(true, std::memory_order_release);
    state->notify();
}

void ApplyProgress::Request::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto &request = *static_cast<Request *>(source);
    if (request.submitted_.test_and_set(std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::invalid_argument());
        return;
    }
    auto state        = request.state_;
    request.complete_ = complete;
    request.context_  = context;
    request.pending_.store(true, std::memory_order_release);
    for (auto &slot : state->waiters) {
        Request *expected = nullptr;
        if (slot.compare_exchange_strong(expected, &request, std::memory_order_release, std::memory_order_relaxed)) {
            // Covers an advance racing before or after publication of this
            // waiter; the notifier may destroy the request at terminal.
            state->notify();
            return;
        }
    }
    request.pending_.store(false, std::memory_order_release);
    complete(context, detail::CallbackSignal::kError, Status::unavailable("apply progress waiter capacity exhausted"));
}
} // namespace crowdb::tree
