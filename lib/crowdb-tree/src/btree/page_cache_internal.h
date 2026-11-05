// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "page_cache.h"
#include "page_cache_frame.h"
#include "published_state.h"

namespace crowdb::tree
{
struct SharedPageCache::State : std::enable_shared_from_this<SharedPageCache::State>
{
    using Kind = PageCacheKind;
    using Key  = PageCacheKey;
    enum class Phase : uint8_t { Empty, Loading, Ready };
    enum class CommandKind : uint8_t { Request, Loaded };

    struct Command
    {
        CommandKind kind    = CommandKind::Request;
        Command    *next    = nullptr;
        void       *context = nullptr;
    };

    struct Reservation;

    struct Request
    {
        std::atomic<bool>                              occupied{false};
        Command                                        command;
        Request                                       *next     = nullptr;
        PageLoadPriority                               priority = PageLoadPriority::Read;
        std::shared_ptr<State>                         owner;
        std::shared_ptr<Reservation>                   reservation;
        std::shared_ptr<PageLoader>                    upstream;
        std::shared_ptr<detail::AllocationReservation> metadata;
        BaseRef                                        base;
        Key                                            key;
        PageLoader::LeafComplete                       leaf_done;
        PageLoader::InnerComplete                      inner_done;
        uint64_t                                       registered_at_ns = 0;
        TreeWaitSpan                                   shared_wait;
    };

    struct Reservation
    {
        Reservation(std::shared_ptr<State> owner, Request *slot);
        ~Reservation();
        std::shared_ptr<State> owner;
        Request               *slot;
        std::atomic<bool>      active{false};
    };

    struct Entry
    {
        Command                           command{.kind = CommandKind::Loaded};
        Phase                             phase = Phase::Empty;
        Key                               key;
        uint64_t                          touched         = 0;
        size_t                            bytes           = 0;
        uint64_t                          max_slot        = 0;
        bool                              drop_when_ready = false;
        PageLoadPriority                  priority        = PageLoadPriority::Maintenance;
        Request                          *waiters         = nullptr;
        std::shared_ptr<const BackingRef> backing;
        std::shared_ptr<PageLoader>       upstream;
        // One atomic identity/terminal word rejects duplicate/late callbacks,
        // including after an entry has been recycled for another generation.
        std::atomic<uint64_t>                      completion{0};
        Status                                     status;
        std::shared_ptr<const LeafBase>            leaf;
        std::shared_ptr<const InnerPageState>      inner;
        std::shared_ptr<PageCacheResidency::Ready> residency;
        std::shared_ptr<PageCacheFrame>            frame;
    };

    State(PageCacheLimits bounds, const std::shared_ptr<detail::AllocationReservation> &metadata,
          std::shared_ptr<TreeResourceMetrics> resources, detail::FlushRuntime *runtime,
          std::shared_ptr<detail::FlushRuntime::Lane> lane);
    static Key make_key(Kind kind, const BaseRef &base);
    Request   *claim(bool accept = true) noexcept;
    void       submit(Request *request) noexcept;
    void       enqueue(Command *command) noexcept;
    void       wake() noexcept;
    void       drain() noexcept;
    void       process(Request *request, bool queued = false);
    void       pend(Request *request) noexcept;
    void       loaded(Entry *entry);
    void       start(Entry *entry, Request *request);
    void       complete(Entry *entry, uint64_t identity, Status status, std::shared_ptr<const LeafBase> leaf,
                        std::shared_ptr<const InnerPageState> inner) noexcept;
    void       deliver(Request *request, const Status &status, std::shared_ptr<const LeafBase> leaf = {},
                       std::shared_ptr<const InnerPageState> inner = {}, uint64_t max_slot = 0,
                       const std::shared_ptr<PageCacheFrame> &frame = {}) noexcept;
    void       discard(Entry &entry) noexcept;
    Entry     *victim() noexcept;
    Entry     *ready_victim() noexcept;
    void       evict_ready() noexcept;
    size_t     trim_ready(PageCacheKind kind, size_t max_pages) noexcept;
    void       drain_trims() noexcept;
    void       resume_pending();

    const PageCacheLimits                     limits;
    const std::shared_ptr<PageCacheResidency> residency;
    detail::ReservedVector<Request>           requests;
    detail::ReservedVector<Entry>             entries;
    std::atomic<size_t>                       next_request{0};
    std::atomic<Command *>                    incoming{nullptr};
    std::atomic<PageCacheTrim *>              trims{nullptr};
    std::atomic<bool>                         running{false};
    std::atomic<bool>                         closed{false};
    std::atomic<bool>                         evict_requested{false};
    std::atomic<bool>                         publication_claimed{false};
    uint64_t                                  clock         = 0;
    uint64_t                                  next_identity = 1;
    std::array<Request *, 2>                  pending{};
    std::array<Request *, 2>                  pending_tail{};
    size_t                                    read_burst       = 0;
    bool                                      resume_requested = false;
    std::atomic<uint64_t>                     hits{0}, misses{0}, joined{0}, evictions{0}, rejected{0};
    std::atomic<uint64_t>                     ordinary_waiters{0};
    std::atomic<uint64_t> inflight{0}, waiters{0}, reserved_waiters{0}, resident_pages{0}, resident_bytes{0};
};
} // namespace crowdb::tree
