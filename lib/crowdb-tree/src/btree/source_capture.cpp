// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_admission_internal.h"

#include <cassert>

namespace crowdb::tree
{
void SourceCapture::State::Node::complete(void *context) noexcept
{
    static_cast<Node *>(context)->owner->one_drained();
}

void SourceCapture::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    auto previous = lifecycle.load(std::memory_order_acquire);
    do {
        if ((previous & kRunning) != 0) {
            callback(argument, detail::CallbackSignal::kError, Status::unavailable());
            return;
        }
    } while (!lifecycle.compare_exchange_weak(previous, previous | kRunning, std::memory_order_acq_rel));
    complete = callback;
    context  = argument;
    if (ready.load(std::memory_order_acquire)) {
        finish();
        return;
    }
    assert(!registered);
    registered = true;
    auto self  = weak_from_this().lock();
    assert(self);
    draining_owner = self;
    auto captured  = SourceCapturePhase::kCaptured;
    phase.compare_exchange_strong(captured, SourceCapturePhase::kDraining, std::memory_order_acq_rel);
    remaining.store(nodes.size(), std::memory_order_release);
    for (size_t i = 0; i < nodes.size(); ++i) {
        // Every node is preallocated and registered once. Close precedes start,
        // so a last writer cannot be followed by a newly admitted writer.
        bool accepted = expected->tables[i]->notify_when_frozen(nodes[i].waiter);
        assert(accepted);
        (void)accepted;
    }
}

void SourceCapture::State::one_drained() noexcept
{
    auto self = weak_from_this().lock();
    assert(self);
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) != 1) {
        return;
    }
    ready.store(true, std::memory_order_release);
    auto draining = SourceCapturePhase::kDraining;
    phase.compare_exchange_strong(draining, SourceCapturePhase::kReady, std::memory_order_acq_rel);
    draining_owner.reset();
    finish();
}

void SourceCapture::State::finish() noexcept
{
    auto       callback = complete;
    auto      *argument = context;
    const auto previous = lifecycle.exchange(0, std::memory_order_acq_rel);
    assert((previous & kRunning) != 0);
    const auto signal = (previous & kStopped) != 0 ? detail::CallbackSignal::kStopped : detail::CallbackSignal::kValue;
    callback(argument, signal, Status::Ok());
}

SourceCapture::~SourceCapture() = default;

uint64_t SourceCapture::batch_id() const noexcept
{
    return state_->batch;
}

uint64_t SourceCapture::floor() const noexcept
{
    return state_->expected->floor;
}

uint64_t SourceCapture::frontier() const noexcept
{
    return state_->frontier;
}

SourceCapturePhase SourceCapture::phase() const noexcept
{
    return state_->phase.load(std::memory_order_acquire);
}

bool SourceCapture::drained() const noexcept
{
    return state_->ready.load(std::memory_order_acquire);
}

bool SourceCapture::successor_active() const noexcept
{
    return state_->activated.load(std::memory_order_acquire);
}

const std::shared_ptr<const SourceCatalog> &SourceCapture::expected_catalog() const noexcept
{
    return state_->expected;
}

const std::shared_ptr<const SourceCatalog> &SourceCapture::next_catalog() const noexcept
{
    return state_->successor_catalog;
}

const std::shared_ptr<MemTable> &SourceCapture::successor() const noexcept
{
    return state_->successor_catalog->active;
}

const std::shared_ptr<detail::AllocationReservation> &SourceCapture::admission_owner() const noexcept
{
    return state_->admission;
}

detail::CallbackSender SourceCapture::drain_sender() noexcept
{
    return {this, &SourceCapture::submit};
}

void SourceCapture::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto state = static_cast<SourceCapture *>(source)->state_;
    state->start(complete, context);
}

void SourceCapture::request_stop() noexcept
{
    state_->lifecycle.fetch_or(State::kStopped, std::memory_order_acq_rel);
}
} // namespace crowdb::tree
