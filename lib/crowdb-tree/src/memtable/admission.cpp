// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/memtable/memtable.h"

#include <cassert>

namespace crowdb::tree
{
MemTable::FrozenWaiter MemTable::frozen_sentinel_;

bool MemTable::try_enter() noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while ((state & kClosed) == 0 && state != kClosed - 1) {
        if (writers_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel)) {
            if (list_.source_ledger_) {
                list_.source_ledger_->writer_enter();
            }
            return true;
        }
    }
    return false;
}

bool MemTable::validate_open() noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while ((state & kClosed) == 0) {
        if (writers_.compare_exchange_weak(state, state, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void MemTable::leave() noexcept
{
    const auto previous = writers_.fetch_sub(1, std::memory_order_acq_rel);
#ifdef CROWDB_TREE_TEST_UTIL
    if (list_.test_hook_ != nullptr) {
        list_.test_hook_(list_.test_context_, ConcurrentSkipList::PausePoint::kBeforeLeaveAccounting, {});
    }
#endif
    if (list_.source_ledger_) {
        list_.source_ledger_->writer_leave((previous & kClosed) != 0);
    }
    assert((previous & ~kClosed) != 0);
    if (previous == kClosed + 1) {
        writers_.notify_all();
        notify_frozen();
    }
}

void MemTable::close() noexcept
{
    const auto previous = writers_.fetch_or(kClosed, std::memory_order_acq_rel);
#ifdef CROWDB_TREE_TEST_UTIL
    if (list_.test_hook_ != nullptr) {
        list_.test_hook_(list_.test_context_, ConcurrentSkipList::PausePoint::kBeforeCloseAccounting, {});
    }
#endif
    if ((previous & kClosed) == 0 && list_.source_ledger_) {
        list_.source_ledger_->table_closed(previous & ~kClosed);
    }
    writers_.notify_all();
    notify_frozen();
}

bool MemTable::notify_when_frozen(FrozenWaiter &waiter) noexcept
{
    assert(waiter.complete_ != nullptr);
    if (waiter.registered_.test_and_set(std::memory_order_acq_rel)) {
        return false;
    }
    auto *head = frozen_waiters_.load(std::memory_order_acquire);
    do {
        if (head == &frozen_sentinel_) {
            waiter.complete_(waiter.context_);
            return true;
        }
        waiter.next_ = head;
    } while (
        !frozen_waiters_.compare_exchange_weak(head, &waiter, std::memory_order_release, std::memory_order_acquire));
    notify_frozen();
    return true;
}

void MemTable::notify_frozen() noexcept
{
    if (writers_.load(std::memory_order_acquire) != kClosed) {
        return;
    }
    if (list_.owned_epoch_) {
        list_.owned_epoch_->seal_source();
    }
    if (frozen_notified_.test_and_set(std::memory_order_acq_rel)) {
        return;
    }
    if (list_.source_ledger_) {
        list_.source_ledger_->table_frozen(approx_bytes());
    }
    auto *waiter = frozen_waiters_.exchange(&frozen_sentinel_, std::memory_order_acq_rel);
    while (waiter != nullptr && waiter != &frozen_sentinel_) {
        auto *next     = waiter->next_;
        auto  complete = waiter->complete_;
        auto *context  = waiter->context_;
        complete(context);
        waiter = next;
    }
}

void MemTable::wait_frozen() const noexcept
{
    auto state = writers_.load(std::memory_order_acquire);
    while (state != kClosed) {
        writers_.wait(state, std::memory_order_acquire);
        state = writers_.load(std::memory_order_acquire);
    }
}
} // namespace crowdb::tree
