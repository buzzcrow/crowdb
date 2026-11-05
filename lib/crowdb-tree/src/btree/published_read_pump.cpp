// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_read_internal.h"

#include <cassert>

namespace crowdb::tree::published_read_detail
{
void ReadPump::start(detail::CallbackComplete complete, void *context) noexcept
{
    uint8_t expected = 0;
    if (!lifecycle_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::unavailable("read operation already running"));
        return;
    }
    complete_      = complete;
    context_       = context;
    begin_pending_ = true;
    lifecycle_.store(2, std::memory_order_release);
    wake();
}

void ReadPump::stop() noexcept
{
    stopped_.store(true, std::memory_order_release);
    wake();
}

void ReadPump::wake() noexcept
{
    auto owner = weak_from_this().lock();
    assert(owner);
    wake_.store(true, std::memory_order_release);
    if (gate_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    for (;;) {
        while (wake_.exchange(false, std::memory_order_acq_rel)) {
            drain();
        }
        // Synchronize with a waker that found the pump running. A release
        // store followed by a load can miss both sides of that notification.
        gate_.exchange(false, std::memory_order_acq_rel);
        if (!wake_.load(std::memory_order_acquire) || gate_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

void ReadPump::drain() noexcept
{
    if (lifecycle_.load(std::memory_order_acquire) != 2) {
        return;
    }
    try {
        if (begin_pending_) {
            begin_pending_ = false;
            begin();
        }
        if (stopped_.load(std::memory_order_acquire)) {
            status() = Status::unavailable("read stopped");
            finish(detail::CallbackSignal::kStopped);
            return;
        }
        if (event_) {
            if (!event_->ready.load(std::memory_order_acquire)) {
                return;
            }
            auto event = std::move(event_);
            if (!event->status.ok()) {
                status() = std::move(event->status);
            }
            else {
                loaded(*event);
            }
        }
        if (!status().ok()) {
            finish(detail::CallbackSignal::kError);
        }
        else if (advance()) {
            finish(status().ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError);
        }
    }
    catch (const std::bad_alloc &) {
        status() = Status::resource_exhausted("read operation allocation failed");
        finish(detail::CallbackSignal::kError);
    }
    catch (...) {
        status() = Status::internal_error("read operation threw");
        finish(detail::CallbackSignal::kError);
    }
}

void ReadPump::finish(detail::CallbackSignal signal) noexcept
{
    auto   callback = complete_;
    auto  *context  = context_;
    Status outcome  = status();
    lifecycle_.store(0, std::memory_order_release);
    callback(context, signal, std::move(outcome));
}

void ReadPump::load_base(BaseRef base)
{
    auto event = detail::reserved_shared<LoadEvent>(limits.metadata);
    event_     = event;
    if (!loader) {
        event->status = Status::unavailable("cold leaf requires an asynchronous loader");
        event->ready.store(true, std::memory_order_release);
        wake();
        return;
    }
    auto self = shared_from_this();
    loader->load_leaf(std::move(base), [self, event](Status status, std::shared_ptr<const LeafBase> leaf) {
        event->status = std::move(status);
        event->leaf   = std::move(leaf);
        event->ready.store(true, std::memory_order_release);
        self->wake();
    });
}

void ReadPump::load_value(const LogicalCell &cell)
{
    auto event = detail::reserved_shared<LoadEvent>(limits.metadata);
    event_     = event;
    if (!loader || cell.encoded.size() < kOverflowCellSize) {
        event->status = Status::not_supported("overflow read requires a captured backing resolver");
        event->ready.store(true, std::memory_order_release);
        wake();
        return;
    }
    const CellView encoded(cell.encoded);
    auto           self = shared_from_this();
    loader->load_overflow(cell.backing, encoded.overflow_head(), encoded.overflow_len(),
                          [self, event](Status status, std::shared_ptr<const buffer> value) {
                              event->status = std::move(status);
                              event->value  = std::move(value);
                              event->ready.store(true, std::memory_order_release);
                              self->wake();
                          });
}

void ReadPump::load_inner(std::shared_ptr<const BackingRef> backing)
{
    auto event = detail::reserved_shared<LoadEvent>(limits.metadata);
    event_     = event;
    if (!loader) {
        event->status = Status::unavailable("cold index requires an asynchronous loader");
        event->ready.store(true, std::memory_order_release);
        wake();
        return;
    }
    auto self = shared_from_this();
    loader->load_inner(std::move(backing), [self, event](Status status, std::shared_ptr<const InnerPageState> inner) {
        event->status = std::move(status);
        event->inner  = std::move(inner);
        event->ready.store(true, std::memory_order_release);
        self->wake();
    });
}

Status validate_read(const std::shared_ptr<const PublishedTreeState> &view, const PublishedReadLimits &limits)
{
    if (!view || !view->sources || limits.max_route_depth == 0 || limits.max_page_sources == 0 ||
        limits.max_work == 0 || limits.max_entries == 0 || limits.max_result_bytes == 0) {
        return Status::invalid_argument("read requires a captured view and finite positive limits");
    }
    if (view->sources->tables.size() > limits.max_memtables) {
        return Status::resource_exhausted("read MemTable source budget exhausted");
    }
    for (const auto &source : view->sources->tables) {
        if (!source) {
            return Status::corruption("captured MemTable source has no owner");
        }
    }
    return Status::Ok();
}

LogicalCell owned_cell(std::shared_ptr<const CellVersion> owner)
{
    EntryRef entry;
    entry.owner = std::move(owner);
    return LogicalCell::from_entry(entry);
}

void materialize_value(LogicalCell *cell, const std::shared_ptr<const buffer> &value)
{
    cell->owner   = value;
    cell->value   = value->slice();
    cell->encoded = {};
    cell->flags &= static_cast<uint8_t>(~kFlagOverflow);
}
} // namespace crowdb::tree::published_read_detail
