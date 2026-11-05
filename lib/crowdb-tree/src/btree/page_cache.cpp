// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache_internal.h"
#include "page_load_internal.h"

#include <cassert>
#include <stdexcept>

namespace crowdb::tree
{
class SharedPageCache::Binding final : public PageLoader
{
  public:
    Binding(std::shared_ptr<State> state, std::shared_ptr<PageLoader> upstream,
            std::shared_ptr<detail::AllocationReservation> metadata,
            std::shared_ptr<State::Reservation> reservation = {}, PageLoadPriority priority = PageLoadPriority::Read)
        : state_(std::move(state)),
          upstream_(std::move(upstream)),
          metadata_(std::move(metadata)),
          reservation_(std::move(reservation)),
          priority_(priority)
    {
    }

    void load_leaf(BaseRef base, LeafComplete complete) override
    {
        if (base.resident) {
            complete_page_load(complete, Status::Ok(), std::move(base.resident));
            return;
        }
        if (!base.backing || !base.backing->readable()) {
            complete_page_load(complete, Status::invalid_argument(), std::shared_ptr<const LeafBase>{});
            return;
        }
        auto *request = claim();
        if (request == nullptr) {
            complete_page_load(complete, Status::unavailable(), std::shared_ptr<const LeafBase>{});
            return;
        }
        request->priority  = priority_;
        request->base      = std::move(base);
        request->key       = State::make_key(State::Kind::Leaf, request->base);
        request->leaf_done = std::move(complete);
        request->upstream  = upstream_;
        request->metadata  = metadata_;
        state_->submit(request);
    }

    void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override
    {
        if (!backing || !backing->readable()) {
            complete_page_load(complete, Status::invalid_argument(), std::shared_ptr<const InnerPageState>{});
            return;
        }
        auto *request = claim();
        if (request == nullptr) {
            complete_page_load(complete, Status::unavailable(), std::shared_ptr<const InnerPageState>{});
            return;
        }
        request->priority     = priority_;
        request->base.backing = std::move(backing);
        request->key          = State::make_key(State::Kind::Inner, request->base);
        request->inner_done   = std::move(complete);
        request->upstream     = upstream_;
        request->metadata     = metadata_;
        state_->submit(request);
    }

    void load_overflow(uint64_t head, uint64_t length, ValueComplete complete) override
    {
        if (state_->closed.load(std::memory_order_acquire)) {
            complete_page_load(complete, Status::unavailable(), std::shared_ptr<const buffer>{});
            return;
        }
        // Overflow IDs are meaningful only in this binding's captured resolver.
        // They are streamed through its budget and never keyed by bare PID.
        upstream_->load_overflow(head, length, std::move(complete));
    }

    void load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                       ValueComplete complete) override
    {
        if (state_->closed.load(std::memory_order_acquire)) {
            complete_page_load(complete, Status::unavailable(), std::shared_ptr<const buffer>{});
            return;
        }
        upstream_->load_overflow(std::move(primary), head, length, std::move(complete));
    }

  private:
    State::Request *claim() noexcept
    {
        if (!reservation_) {
            return state_->claim();
        }
        bool idle = false;
        if (state_->closed.load(std::memory_order_acquire) ||
            !reservation_->active.compare_exchange_strong(idle, true, std::memory_order_acq_rel)) {
            state_->rejected.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }
        auto *request        = reservation_->slot;
        request->owner       = state_;
        request->reservation = reservation_;
        state_->waiters.fetch_add(1, std::memory_order_relaxed);
        return request;
    }

    std::shared_ptr<State>                         state_;
    std::shared_ptr<PageLoader>                    upstream_;
    std::shared_ptr<detail::AllocationReservation> metadata_;
    std::shared_ptr<State::Reservation>            reservation_;
    PageLoadPriority                               priority_;
};

SharedPageCache::SharedPageCache(PageCacheLimits limits, std::shared_ptr<detail::AllocationReservation> metadata,
                                 std::shared_ptr<TreeResourceMetrics> resources, detail::FlushRuntime *runtime,
                                 std::shared_ptr<detail::FlushRuntime::Lane> lane)
{
    if (!allocation_bound(limits)) {
        throw std::invalid_argument("page cache requires finite nonzero page and waiter capacities");
    }
    state_ = detail::reserved_shared<State>(metadata, limits, metadata, std::move(resources), runtime, std::move(lane));
}

SharedPageCache::~SharedPageCache()
{
    close();
}

std::optional<size_t> SharedPageCache::allocation_bound(PageCacheLimits limits) noexcept
{
    if (limits.max_pages == 0 || limits.max_waiters == 0 || limits.max_read_burst == 0 ||
        limits.low_resident_bytes > limits.max_resident_bytes) {
        return {};
    }
    return detail::AllocationBound(detail::shared_allocation_bound<State>() +
                                   detail::shared_allocation_bound<SharedPageCache>() +
                                   detail::shared_allocation_bound<Binding>() + PageCacheResidency::allocation_bound())
        .add_product(limits.max_pages, sizeof(State::Entry))
        .add_product(limits.max_waiters, sizeof(State::Request))
        .value();
}

std::shared_ptr<PageLoader> SharedPageCache::bind(std::shared_ptr<PageLoader>                    upstream,
                                                  std::shared_ptr<detail::AllocationReservation> metadata,
                                                  PageLoadPriority                               priority) const
{
    if (!upstream) {
        throw std::invalid_argument("page cache binding requires an upstream loader");
    }
    return detail::reserved_shared<Binding>(metadata, state_, std::move(upstream), metadata, nullptr, priority);
}

size_t SharedPageCache::binding_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Binding>() + detail::shared_allocation_bound<State::Reservation>();
}

size_t SharedPageCache::load_allocation_bound() noexcept
{
    return PageCacheResidency::ready_allocation_bound() + PageCacheFrame::allocation_bound() + pin_allocation_bound();
}

size_t SharedPageCache::pin_allocation_bound() noexcept
{
    return PageCacheFrame::pin_allocation_bound();
}

std::optional<size_t> SharedPageCache::load_reserve_bound(size_t pinned_frames, size_t workers) const noexcept
{
    if (!state_->residency) {
        return 0;
    }
    const auto frames =
        detail::AllocationBound(state_->limits.max_pages).add(pinned_frames).add(workers).add(1).value();
    if (!frames) {
        return {};
    }
    return detail::AllocationBound().add_product(*frames, load_allocation_bound()).value();
}

size_t SharedPageCache::retained_page_limit() const noexcept
{
    return state_->limits.max_pages;
}

Status SharedPageCache::bind_reserved(std::shared_ptr<PageLoader>                           upstream,
                                      const std::shared_ptr<detail::AllocationReservation> &metadata,
                                      std::shared_ptr<PageLoader> *out, PageLoadPriority priority) const
{
    if (!upstream || out == nullptr) {
        return Status::invalid_argument("reserved cache binding requires upstream and output");
    }
    auto *slot = state_->claim(false);
    if (slot == nullptr) {
        return Status::unavailable("cache waiter reservation unavailable");
    }
    std::shared_ptr<State::Reservation> reservation;
    try {
        reservation = detail::reserved_shared<State::Reservation>(metadata, state_, slot);
        *out = detail::reserved_shared<Binding>(metadata, state_, std::move(upstream), metadata, reservation, priority);
    }
    catch (const std::bad_alloc &) {
        if (!reservation) {
            slot->occupied.store(false, std::memory_order_release);
        }
        return Status::resource_exhausted("cache binding allocation failed");
    }
    return Status::Ok();
}

PageCacheSnapshot SharedPageCache::snapshot() const noexcept
{
    return {.hits             = state_->hits.load(std::memory_order_relaxed),
            .misses           = state_->misses.load(std::memory_order_relaxed),
            .joined           = state_->joined.load(std::memory_order_relaxed),
            .evictions        = state_->evictions.load(std::memory_order_relaxed),
            .rejected         = state_->rejected.load(std::memory_order_relaxed),
            .inflight         = state_->inflight.load(std::memory_order_relaxed),
            .waiters          = state_->waiters.load(std::memory_order_relaxed),
            .reserved_waiters = state_->reserved_waiters.load(std::memory_order_relaxed),
            .resident_pages   = state_->resident_pages.load(std::memory_order_relaxed),
            .resident_bytes   = state_->resident_bytes.load(std::memory_order_relaxed),
            .proxy_pages      = state_->residency ? state_->residency->proxy_pages() : 0,
            .request_slots    = state_->reserved_waiters.load(std::memory_order_relaxed) +
                                state_->ordinary_waiters.load(std::memory_order_relaxed)};
}

const std::shared_ptr<PageCacheResidency> &SharedPageCache::residency() const noexcept
{
    return state_->residency;
}

bool SharedPageCache::claim_publication() noexcept
{
    return state_->residency && !state_->publication_claimed.exchange(true, std::memory_order_acq_rel);
}

void SharedPageCache::evict() noexcept
{
    state_->evict_requested.store(true, std::memory_order_release);
    state_->wake();
}

void SharedPageCache::close() noexcept
{
    state_->closed.store(true, std::memory_order_release);
    evict();
}

void SharedPageCache::close_and_drain() noexcept
{
    close();
    if (state_->residency) {
        state_->residency->close_and_drain();
    }
}

SharedPageCache::State::State(PageCacheLimits bounds, const std::shared_ptr<detail::AllocationReservation> &metadata,
                              std::shared_ptr<TreeResourceMetrics> resources, detail::FlushRuntime *runtime,
                              std::shared_ptr<detail::FlushRuntime::Lane> lane)
    : limits(bounds),
      residency(resources ? detail::reserved_shared<PageCacheResidency>(metadata, std::move(resources), metadata,
                                                                        runtime, std::move(lane))
                          : nullptr),
      requests(bounds.max_waiters, detail::ReservationAllocator<Request>(metadata)),
      entries(bounds.max_pages, detail::ReservationAllocator<Entry>(metadata))
{
    for (auto &request : requests) {
        request.command.context = &request;
    }
    for (auto &entry : entries) {
        entry.command.context = &entry;
    }
}

SharedPageCache::State::Key SharedPageCache::State::make_key(Kind kind, const BaseRef &base)
{
    return page_cache_key(kind, base);
}

SharedPageCache::State::Reservation::Reservation(std::shared_ptr<State> owner, Request *slot)
    : owner(std::move(owner)),
      slot(slot)
{
    this->owner->reserved_waiters.fetch_add(1, std::memory_order_relaxed);
}

SharedPageCache::State::Reservation::~Reservation()
{
    assert(!active.load(std::memory_order_relaxed));
    owner->reserved_waiters.fetch_sub(1, std::memory_order_relaxed);
    slot->occupied.store(false, std::memory_order_release);
}

} // namespace crowdb::tree
