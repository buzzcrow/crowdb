// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache_internal.h"
#include "page_load_internal.h"

#include <algorithm>

namespace crowdb::tree
{
void SharedPageCache::State::process(Request *request, bool queued)
{
    for (auto &entry : entries) {
        if (entry.phase == Phase::Empty || entry.key != request->key) {
            continue;
        }
        entry.touched = ++clock;
        if (request->priority == PageLoadPriority::Read) {
            entry.priority = PageLoadPriority::Read;
        }
        if (entry.phase == Phase::Ready) {
            hits.fetch_add(1, std::memory_order_relaxed);
            record_work(request->metadata, TreeWorkCounter::CacheHits, 1,
                        request->priority == PageLoadPriority::Read ? TreeWorkCause::Read : TreeWorkCause::Flush);
            deliver(request, Status::Ok(), entry.leaf, entry.inner, entry.max_slot, entry.frame);
        }
        else {
            request->shared_wait.begin(request->metadata ? request->metadata->account()->work_metrics() : nullptr,
                                       TreeWaitClass::SharedCache, request->registered_at_ns);
            request->next = entry.waiters;
            entry.waiters = request;
            joined.fetch_add(1, std::memory_order_relaxed);
            record_work(request->metadata, TreeWorkCounter::CacheJoinedLoads, 1,
                        request->priority == PageLoadPriority::Read ? TreeWorkCause::Read : TreeWorkCause::Flush);
        }
        return;
    }
    if (!queued && (pending[0] != nullptr || pending[1] != nullptr)) {
        pend(request);
        resume_requested = true;
        return;
    }
    auto *entry = victim();
    if (entry == nullptr) {
        pend(request);
        return;
    }
    if (next_identity >= UINT64_MAX / 2) {
        rejected.fetch_add(1, std::memory_order_relaxed);
        deliver(request, Status::unavailable());
        return;
    }
    discard(*entry);
    start(entry, request);
}

void SharedPageCache::State::pend(Request *request) noexcept
{
    const auto priority = static_cast<size_t>(request->priority);
    request->next       = nullptr;
    if (pending_tail[priority] != nullptr) {
        pending_tail[priority]->next = request;
    }
    else {
        pending[priority] = request;
    }
    pending_tail[priority] = request;
}

SharedPageCache::State::Entry *SharedPageCache::State::victim() noexcept
{
    for (auto &entry : entries) {
        if (entry.phase == Phase::Empty) {
            return &entry;
        }
    }
    return ready_victim();
}

SharedPageCache::State::Entry *SharedPageCache::State::ready_victim() noexcept
{
    Entry *oldest = nullptr;
    for (auto &entry : entries) {
        if (entry.phase == Phase::Ready && (oldest == nullptr || entry.priority > oldest->priority ||
                                            (entry.priority == oldest->priority && entry.touched < oldest->touched))) {
            oldest = &entry;
        }
    }
    return oldest;
}

void SharedPageCache::State::start(Entry *entry, Request *request)
{
    if (residency) {
        auto ready       = residency->prepare_ready(request->key, request->metadata);
        auto frame       = detail::reserved_shared<PageCacheFrame>(request->metadata, residency->resources());
        entry->residency = std::move(ready);
        entry->frame     = std::move(frame);
    }
    entry->phase           = Phase::Loading;
    entry->key             = request->key;
    entry->priority        = request->priority;
    entry->touched         = ++clock;
    entry->backing         = request->base.backing;
    entry->upstream        = request->upstream;
    entry->waiters         = request;
    request->next          = nullptr;
    entry->drop_when_ready = closed.load(std::memory_order_acquire);
    const auto identity    = next_identity++;
    entry->completion.store(identity << 1, std::memory_order_release);
    misses.fetch_add(1, std::memory_order_relaxed);
    record_work(request->metadata, TreeWorkCounter::CacheMisses, 1,
                request->priority == PageLoadPriority::Read ? TreeWorkCause::Read : TreeWorkCause::Flush);
    inflight.fetch_add(1, std::memory_order_relaxed);
    auto self = shared_from_this();
    try {
        if (entry->key.kind == Kind::Leaf) {
            auto input = request->base;
            // Validate each waiter's proof against the common decoded frame,
            // so an invalid proof cannot poison other subscribers to this base.
            input.max_slot.reset();
            entry->upstream->load_leaf(std::move(input), [self, entry, identity](Status status, auto leaf) {
                self->complete(entry, identity, std::move(status), std::move(leaf), {});
            });
        }
        else {
            entry->upstream->load_inner(entry->backing, [self, entry, identity](Status status, auto inner) {
                self->complete(entry, identity, std::move(status), {}, std::move(inner));
            });
        }
    }
    catch (const std::bad_alloc &) {
        complete(entry, identity, Status::resource_exhausted(), {}, {});
    }
    catch (...) {
        complete(entry, identity, Status::io_error(), {}, {});
    }
}

void SharedPageCache::State::complete(Entry *entry, uint64_t identity, Status status,
                                      std::shared_ptr<const LeafBase>       leaf,
                                      std::shared_ptr<const InnerPageState> inner) noexcept
{
    auto expected = identity << 1;
    if (!entry->completion.compare_exchange_strong(expected, expected | 1, std::memory_order_acq_rel)) {
        record_work(entries.get_allocator().owner(), TreeWorkCounter::StaleCallbacks);
        return;
    }
    entry->status = std::move(status);
    entry->leaf   = std::move(leaf);
    entry->inner  = std::move(inner);
    enqueue(&entry->command);
}

void SharedPageCache::State::loaded(Entry *entry)
{
    resume_requested = true;
    inflight.fetch_sub(1, std::memory_order_relaxed);
    entry->upstream.reset();
    detail::AllocationBound bytes;
    if (entry->status.ok()) {
        if (entry->key.kind == Kind::Leaf && entry->leaf && !entry->inner) {
            bytes.add(entry->leaf->page_bytes());
            for (size_t i = 0; i < entry->leaf->count(); ++i) {
                entry->max_slot = std::max(entry->max_slot, CellView(entry->leaf->cell(i)).slot());
            }
        }
        else if (entry->key.kind == Kind::Inner && entry->inner && !entry->leaf) {
            bytes.add_product(entry->inner->children.capacity(), sizeof(uint64_t));
            bytes.add_product(entry->inner->separators.capacity(), sizeof(OwnedKey));
            for (const auto &key : entry->inner->separators) {
                bytes.add(key.size());
            }
        }
        else {
            entry->status = Status::corruption();
        }
    }
    const auto size = bytes.value();
    if (entry->status.ok() && !size) {
        entry->status = Status::resource_exhausted();
    }
    if (entry->status.ok() && entry->frame && size) {
        entry->frame->activate(*size, entry->leaf, entry->inner);
    }
    const bool keep = entry->status.ok() && size && *size <= limits.max_resident_bytes && !entry->drop_when_ready &&
                      !closed.load(std::memory_order_acquire);
    if (keep) {
        if (resident_bytes.load(std::memory_order_relaxed) > limits.max_resident_bytes - *size) {
            const auto low    = limits.low_resident_bytes != 0
                                  ? limits.low_resident_bytes
                                  : limits.max_resident_bytes - (limits.max_resident_bytes / 4);
            const auto target = *size < low ? low - *size : 0;
            while (resident_bytes.load(std::memory_order_relaxed) > target) {
                auto *oldest = ready_victim();
                if (oldest == nullptr) {
                    break;
                }
                discard(*oldest);
            }
        }
        entry->phase = Phase::Ready;
        entry->bytes = *size;
        resident_pages.fetch_add(1, std::memory_order_relaxed);
        resident_bytes.fetch_add(*size, std::memory_order_relaxed);
        if (entry->residency) {
            PageCacheResidency::ready(entry->residency);
        }
    }
    while (entry->waiters != nullptr) {
        auto *request  = entry->waiters;
        entry->waiters = request->next;
        deliver(request, entry->status, entry->leaf, entry->inner, entry->max_slot, entry->frame);
    }
    if (!keep) {
        discard(*entry);
    }
}

void SharedPageCache::State::deliver(Request *request, const Status &status, std::shared_ptr<const LeafBase> leaf,
                                     std::shared_ptr<const InnerPageState> inner, uint64_t max_slot,
                                     const std::shared_ptr<PageCacheFrame> &frame) noexcept
{
    Status outcome;
    try {
        outcome = status;
    }
    catch (const std::bad_alloc &) {
        outcome = Status::resource_exhausted();
    }
    if (outcome.ok() && request->key.kind == Kind::Leaf && request->base.max_slot &&
        max_slot > *request->base.max_slot) {
        outcome = Status::corruption();
    }
    if (outcome.ok() && frame) {
        try {
            auto pin = detail::reserved_shared<PageCacheFrame::Pin>(request->metadata, frame);
            if (leaf) {
                leaf = std::shared_ptr<const LeafBase>(pin, leaf.get());
            }
            if (inner) {
                inner = std::shared_ptr<const InnerPageState>(pin, inner.get());
            }
        }
        catch (const std::bad_alloc &) {
            outcome = Status::resource_exhausted();
        }
    }
    if (!outcome.ok()) {
        leaf.reset();
        inner.reset();
    }
    request->shared_wait.finish(outcome.ok() ? TreeWaitOutcome::Granted : TreeWaitOutcome::Error,
                                tree_metrics_now_ns());
    auto owner       = std::move(request->owner);
    auto reservation = std::move(request->reservation);
    auto leaf_done   = std::move(request->leaf_done);
    auto inner_done  = std::move(request->inner_done);
    request->base    = {};
    request->upstream.reset();
    request->metadata.reset();
    request->next = nullptr;
    waiters.fetch_sub(1, std::memory_order_relaxed);
    if (reservation) {
        reservation->active.store(false, std::memory_order_release);
    }
    else {
        ordinary_waiters.fetch_sub(1, std::memory_order_relaxed);
        request->occupied.store(false, std::memory_order_release);
    }
    if (leaf_done) {
        complete_page_load(leaf_done, std::move(outcome), std::move(leaf));
    }
    else {
        complete_page_load(inner_done, std::move(outcome), std::move(inner));
    }
}

void SharedPageCache::State::discard(Entry &entry) noexcept
{
    if (entry.phase == Phase::Ready) {
        if (entry.residency) {
            PageCacheResidency::evict(entry.residency);
        }
        resident_pages.fetch_sub(1, std::memory_order_relaxed);
        resident_bytes.fetch_sub(entry.bytes, std::memory_order_relaxed);
        evictions.fetch_add(1, std::memory_order_relaxed);
        record_work(entries.get_allocator().owner(), TreeWorkCounter::CacheEvictions, 1, TreeWorkCause::CachePressure);
    }
    entry.phase = Phase::Empty;
    entry.leaf.reset();
    entry.inner.reset();
    entry.frame.reset();
    entry.residency.reset();
    entry.backing.reset();
    entry.upstream.reset();
    entry.status   = {};
    entry.bytes    = 0;
    entry.max_slot = 0;
}

void SharedPageCache::State::evict_ready() noexcept
{
    resume_requested = true;
    for (auto &entry : entries) {
        if (entry.phase == Phase::Ready) {
            discard(entry);
        }
        else if (entry.phase == Phase::Loading) {
            entry.drop_when_ready = true;
        }
    }
}

void SharedPageCache::State::resume_pending()
{
    // The burst bound gives foreground reads priority without starving the
    // prepaid maintenance request which makes memory pressure progress.
    for (size_t attempts = 0; attempts < requests.size(); ++attempts) {
        if ((pending[0] == nullptr && pending[1] == nullptr) || victim() == nullptr) {
            return;
        }
        const size_t priority =
            pending[1] != nullptr && (pending[0] == nullptr || read_burst == limits.max_read_burst) ? 1 : 0;
        auto *request     = pending[priority];
        pending[priority] = request->next;
        if (pending[priority] == nullptr) {
            pending_tail[priority] = nullptr;
        }
        read_burst = priority == 1 ? 0 : std::min(read_burst + 1, limits.max_read_burst);
        try {
            process(request, true);
        }
        catch (const std::bad_alloc &) {
            deliver(request, Status::resource_exhausted());
        }
        catch (...) {
            deliver(request, Status::internal_error());
        }
    }
}
} // namespace crowdb::tree
