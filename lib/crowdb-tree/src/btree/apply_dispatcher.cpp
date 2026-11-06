// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_dispatcher.h"

#include "allocation_bound.h"

#include <cassert>

namespace crowdb::tree
{
ApplyDispatcher::Node::~Node()
{
    assert(!queued_.load(std::memory_order_acquire));
}

ApplyDispatcher::ApplyDispatcher(std::shared_ptr<detail::FlushRuntime> runtime, size_t steps_per_turn)
    : runtime_(std::move(runtime)),
      steps_per_turn_(steps_per_turn)
{
}

size_t ApplyDispatcher::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<ApplyDispatcher>() + detail::FlushRuntime::progress_allocation_bound();
}

Status ApplyDispatcher::create(std::shared_ptr<detail::FlushRuntime>       runtime,
                               std::shared_ptr<detail::FlushRuntime::Lane> lane, size_t steps_per_turn,
                               const std::shared_ptr<detail::AllocationReservation> &metadata,
                               std::shared_ptr<ApplyDispatcher>                     *out)
try {
    if (!runtime || !lane || !metadata || steps_per_turn == 0 || out == nullptr || *out) {
        return Status::invalid_argument("apply dispatcher requires a bounded runtime and charged storage");
    }
    auto dispatcher = detail::reserved_construct<ApplyDispatcher>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) ApplyDispatcher(std::move(runtime), steps_per_turn);
    });
    auto status     = dispatcher->runtime_->reserve_progress(std::move(lane), &ApplyDispatcher::run, dispatcher.get(),
                                                             metadata, &dispatcher->progress_);
    if (!status.ok()) {
        return status;
    }
    *out = std::move(dispatcher);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply dispatcher allocation failed");
}

void ApplyDispatcher::enqueue(Node *node) noexcept
{
    node->next_.store(nullptr, std::memory_order_relaxed);
    auto *previous = tail_.exchange(node, std::memory_order_acq_rel);
    previous->next_.store(node, std::memory_order_release);
}

void ApplyDispatcher::request(Node &node, std::shared_ptr<const void> owner) noexcept
{
    assert(owner && node.task_ != nullptr);
    if (node.queued_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    node.owner_ = std::move(owner);
    enqueue(&node);
    progress_->request(weak_from_this().lock());
}

ApplyDispatcher::Node *ApplyDispatcher::pop() noexcept
{
    auto *head = head_;
    auto *next = head->next_.load(std::memory_order_acquire);
    if (head == &stub_) {
        if (next == nullptr) {
            return nullptr;
        }
        head_ = next;
        head  = next;
        next  = next->next_.load(std::memory_order_acquire);
    }
    if (next != nullptr) {
        head_ = next;
        return head;
    }
    if (head != tail_.load(std::memory_order_acquire)) {
        // A producer has exchanged tail but has not linked its predecessor.
        // That producer requests another CPU turn after publishing the link.
        return nullptr;
    }
    enqueue(&stub_);
    next = head->next_.load(std::memory_order_acquire);
    if (next != nullptr) {
        head_ = next;
        return head;
    }
    return nullptr;
}

void ApplyDispatcher::run(void *context) noexcept
{
    auto &dispatcher = *static_cast<ApplyDispatcher *>(context);
    for (size_t step = 0; step < dispatcher.steps_per_turn_; ++step) {
        auto *node = dispatcher.pop();
        if (node == nullptr) {
            return;
        }
        auto       owner    = std::move(node->owner_);
        const auto task     = node->task_;
        auto      *argument = node->context_;
        // Coalesced producers publish operation state before observing this
        // claim. Acquire those publications before executing the node.
        node->queued_.exchange(false, std::memory_order_acq_rel);
        task(argument);
    }
    dispatcher.progress_->request(dispatcher.weak_from_this().lock());
}

void ApplyDispatcher::close_and_drain() noexcept
{
    // Internal continuations may request the next bounded turn during drain.
    // Close the registration only after that finite accepted queue empties.
    progress_->drain();
    progress_->close_and_drain();
    assert(head_ == &stub_ && tail_.load(std::memory_order_acquire) == &stub_);
}
} // namespace crowdb::tree
