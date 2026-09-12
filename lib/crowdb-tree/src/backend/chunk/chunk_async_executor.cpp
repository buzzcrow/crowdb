// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_async_executor.h"

#include "chunk_page_store.h"

#include <algorithm>
#include <utility>

namespace crowdb::tree::detail
{

ChunkAsyncExecutor::State::State(ChunkPageStore *owner, size_t queue_capacity)
    : store(owner),
      capacity(std::max<size_t>(2, queue_capacity)),
      slots(std::make_unique<Slot[]>(capacity))
{
    for (size_t index = 0; index < capacity; ++index) {
        slots[index].sequence.store(index, std::memory_order_relaxed);
        slots[index].wake_epoch = &wake_epoch;
    }
}

ChunkAsyncExecutor::ChunkAsyncExecutor(ChunkPageStore *store, size_t capacity)
    : state_(std::make_shared<State>(store, capacity))
{
    worker_ = std::thread([state = state_] { run(state); });
}

ChunkAsyncExecutor::~ChunkAsyncExecutor()
{
    uint64_t admission = state_->admission_state.fetch_or(kClosedBit, std::memory_order_acq_rel) | kClosedBit;
    while ((admission & ~kClosedBit) != 0) {
        state_->admission_state.wait(admission, std::memory_order_acquire);
        admission = state_->admission_state.load(std::memory_order_acquire);
    }
    for (size_t index = 0; index < state_->capacity; ++index) {
        const uint64_t operation_id = state_->slots[index].active_id.load(std::memory_order_acquire);
        if (operation_id != 0) {
            state_->slots[index].cancelled_id.store(operation_id, std::memory_order_release);
        }
    }
    state_->wake_epoch.fetch_add(1, std::memory_order_release);
    state_->wake_epoch.notify_one();
    if (worker_.joinable()) {
        if (worker_.get_id() == std::this_thread::get_id()) {
            state_->store.store(nullptr, std::memory_order_release);
            worker_.detach();
        }
        else {
            worker_.join();
        }
    }
}

uint64_t ChunkAsyncExecutor::submit(Task task)
{
    auto state = state_;
    if (!enter_submission(state)) {
        return 0;
    }
    uint64_t position = state->enqueue_position.load(std::memory_order_relaxed);
    for (;;) {
        Slot          &slot       = state->slots[position % state->capacity];
        const uint64_t sequence   = slot.sequence.load(std::memory_order_acquire);
        const auto     difference = static_cast<int64_t>(sequence - position);
        if (difference == 0) {
            if (!state->enqueue_position.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
                continue;
            }
            const uint64_t operation_id = position + 1;
            slot.task                   = std::move(task);
            slot.cancelled_id.store(0, std::memory_order_relaxed);
            slot.async_started.store(false, std::memory_order_relaxed);
            slot.async_ready.store(false, std::memory_order_relaxed);
            slot.async_state.reset();
            slot.active_id.store(operation_id, std::memory_order_release);
            slot.sequence.store(position + 1, std::memory_order_release);
            state->pending.fetch_add(1, std::memory_order_release);
            state->wake_epoch.fetch_add(1, std::memory_order_release);
            state->wake_epoch.notify_one();
            leave_submission(state);
            return operation_id;
        }
        if (difference < 0) {
            leave_submission(state);
            return 0;
        }
        position = state->enqueue_position.load(std::memory_order_relaxed);
    }
}

void ChunkAsyncExecutor::cancel(uint64_t operation_id)
{
    if (operation_id == 0) {
        return;
    }
    auto  state = state_;
    Slot &slot  = state->slots[(operation_id - 1) % state->capacity];
    if (slot.active_id.load(std::memory_order_acquire) == operation_id) {
        slot.cancelled_id.store(operation_id, std::memory_order_release);
    }
}

bool ChunkAsyncExecutor::enter_submission(const std::shared_ptr<State> &state)
{
    uint64_t admission = state->admission_state.load(std::memory_order_acquire);
    for (;;) {
        if ((admission & kClosedBit) != 0) {
            return false;
        }
        if (state->admission_state.compare_exchange_weak(admission, admission + 1, std::memory_order_acq_rel,
                                                         std::memory_order_acquire)) {
            return true;
        }
    }
}

void ChunkAsyncExecutor::leave_submission(const std::shared_ptr<State> &state)
{
    const uint64_t prior = state->admission_state.fetch_sub(1, std::memory_order_acq_rel);
    if ((prior & kClosedBit) != 0) {
        state->admission_state.notify_all();
    }
}

void ChunkAsyncExecutor::run(const std::shared_ptr<State> &state)
{
    for (;;) {
        uint64_t pending = state->pending.load(std::memory_order_acquire);
        if (pending == 0) {
            const uint64_t admission = state->admission_state.load(std::memory_order_acquire);
            if ((admission & kClosedBit) != 0) {
                if ((admission & ~kClosedBit) == 0) {
                    return;
                }
                state->admission_state.wait(admission, std::memory_order_acquire);
                continue;
            }
            const uint64_t epoch = state->wake_epoch.load(std::memory_order_acquire);
            if (state->pending.load(std::memory_order_acquire) == 0 &&
                (state->admission_state.load(std::memory_order_acquire) & kClosedBit) == 0) {
                state->wake_epoch.wait(epoch, std::memory_order_relaxed);
            }
            continue;
        }

        Slot          &slot              = state->slots[state->dequeue_position % state->capacity];
        const uint64_t expected_sequence = state->dequeue_position + 1;
        if (slot.sequence.load(std::memory_order_acquire) != expected_sequence) {
            std::this_thread::yield();
            continue;
        }
        if (!execute(state, &slot, state->dequeue_position)) {
            const uint64_t epoch = state->wake_epoch.load(std::memory_order_acquire);
            if (!slot.async_ready.load(std::memory_order_acquire)) {
                state->wake_epoch.wait(epoch, std::memory_order_relaxed);
            }
            continue;
        }
        ++state->dequeue_position;
        state->pending.fetch_sub(1, std::memory_order_acq_rel);
    }
}

bool ChunkAsyncExecutor::execute(const std::shared_ptr<State> &state, Slot *slot, uint64_t position)
{
    const uint64_t          operation_id = position + 1;
    const ChunkCancellation cancellation{.cancelled_id = &slot->cancelled_id, .operation_id = operation_id};
    if (slot->async_started.load(std::memory_order_acquire)) {
        if (!slot->async_ready.load(std::memory_order_acquire)) {
            return false;
        }
        Status status = std::move(slot->async_status);
        if (auto *store = state->store.load(std::memory_order_acquire); store != nullptr) {
            status = crowdb::tree::detail::ChunkPageStore::finish_sync_cancellable(slot->async_state, cancellation,
                                                                                   std::move(status));
        }
        release_slot(state, slot, position, std::move(status));
        return true;
    }
    Status status;
    if (slot->cancelled_id.load(std::memory_order_acquire) == operation_id) {
        status = Status::unavailable("chunk page operation cancelled");
    }
    else {
        switch (slot->task.kind) {
        case Kind::kRead: {
            auto *store = state->store.load(std::memory_order_acquire);
            status      = store == nullptr
                            ? Status::unavailable("chunk page store is closing")
                            : store->read_at_cancellable(slot->task.addr, static_cast<uint8_t *>(slot->task.buffer),
                                                         slot->task.length, cancellation);
            break;
        }
        case Kind::kWrite:
            if (auto *store = state->store.load(std::memory_order_acquire); store != nullptr) {
                status = store->write_at(slot->task.addr, static_cast<const uint8_t *>(slot->task.const_buffer),
                                         slot->task.length);
            }
            else {
                status = Status::unavailable("chunk page store is closing");
            }
            break;
        case Kind::kFsync:
            if (auto *store = state->store.load(std::memory_order_acquire); store != nullptr) {
                slot->async_started.store(true, std::memory_order_release);
                slot->async_state = store->start_sync_cancellable(
                    cancellation, {.context = slot, .complete_fn = &ChunkAsyncExecutor::async_complete});
                if (!slot->async_ready.load(std::memory_order_acquire)) {
                    return false;
                }
                status = std::move(slot->async_status);
                status = crowdb::tree::detail::ChunkPageStore::finish_sync_cancellable(slot->async_state, cancellation,
                                                                                       std::move(status));
            }
            else {
                status = Status::unavailable("chunk page store is closing");
            }
            break;
        }
    }
    release_slot(state, slot, position, std::move(status));
    return true;
}

void ChunkAsyncExecutor::async_complete(void *context, Status status)
{
    auto *slot         = static_cast<Slot *>(context);
    slot->async_status = std::move(status);
    slot->async_ready.store(true, std::memory_order_release);
    slot->wake_epoch->fetch_add(1, std::memory_order_release);
    slot->wake_epoch->notify_one();
}

void ChunkAsyncExecutor::release_slot(const std::shared_ptr<State> &state, Slot *slot, uint64_t position, Status status)
{
    if (auto *store = state->store.load(std::memory_order_acquire); store != nullptr) {
        store->record_completion_wakeup();
    }
    slot->task.completion.complete(std::move(status));
    slot->async_state.reset();
    slot->active_id.store(0, std::memory_order_release);
    slot->sequence.store(position + state->capacity, std::memory_order_release);
}

} // namespace crowdb::tree::detail
