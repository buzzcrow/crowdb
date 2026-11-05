// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "page_cache_residency.h"
#include "page_load.h"

namespace crowdb::tree
{
enum class PageLoadPriority : uint8_t { Read, Maintenance };

struct PageCacheLimits
{
    size_t max_pages   = 256;
    size_t max_waiters = 1024;
    // Decoded frame/array/key bytes referenced by the cache. The allocation
    // ledger separately accounts actual capacity, control objects and pins.
    size_t max_resident_bytes = 64 * 1024 * 1024;
    // Zero selects 75% of the high watermark. A frame larger than the low
    // watermark may remain alone if it fits the high watermark.
    size_t low_resident_bytes = 0;
    size_t max_read_burst     = 8;
};

struct PageCacheSnapshot
{
    uint64_t hits             = 0;
    uint64_t misses           = 0;
    uint64_t joined           = 0;
    uint64_t evictions        = 0;
    uint64_t rejected         = 0;
    uint64_t inflight         = 0;
    uint64_t waiters          = 0;
    uint64_t reserved_waiters = 0;
    uint64_t resident_pages   = 0;
    uint64_t resident_bytes   = 0;
    uint64_t proxy_pages      = 0;
    uint64_t request_slots    = 0;
};

// Intrusive one-shot command owned by its admitted caller through completion.
// It only drops cache references; independent reader pins stay valid and charged.
struct PageCacheTrim
{
    PageCacheKind kind                        = PageCacheKind::Leaf;
    size_t        max_pages                   = 0;
    void (*complete)(void *, size_t) noexcept = nullptr;
    void          *context                    = nullptr;
    PageCacheTrim *next                       = nullptr;
};

// One cache may serve independently budgeted loaders for the same backend.
// Nonzero backend_id must uniquely name that backend. Legacy zero IDs also
// compare backing owner addresses, preventing unrelated stores from aliasing.
// It never changes live mapping/overlays: returned owners serve captured views.
class SharedPageCache
{
    struct State;
    class Binding;

  public:
    // Residency metrics require a host-owned runtime which outlives cache
    // owners and is drained after their release. No runtime ownership is held.
    explicit SharedPageCache(PageCacheLimits limits = {}, std::shared_ptr<detail::AllocationReservation> metadata = {},
                             std::shared_ptr<TreeResourceMetrics>        resources = {},
                             detail::FlushRuntime                       *runtime   = nullptr,
                             std::shared_ptr<detail::FlushRuntime::Lane> lane      = {});
    ~SharedPageCache();
    SharedPageCache(const SharedPageCache &)            = delete;
    SharedPageCache &operator=(const SharedPageCache &) = delete;

    // The miss uses the first caller's upstream, including its held load/I/O
    // credit. Joined callers never acquire a second I/O permit for that base.
    [[nodiscard]] std::shared_ptr<PageLoader> bind(std::shared_ptr<PageLoader>                    upstream,
                                                   std::shared_ptr<detail::AllocationReservation> metadata = {},
                                                   PageLoadPriority priority = PageLoadPriority::Read) const;
    // Reserve one request slot before closing flush sources. This binding has
    // one serial outstanding load and reuses its slot across routing steps.
    [[nodiscard]] Status        bind_reserved(std::shared_ptr<PageLoader>                           upstream,
                                              const std::shared_ptr<detail::AllocationReservation> &metadata,
                                              std::shared_ptr<PageLoader>                          *out,
                                              PageLoadPriority priority = PageLoadPriority::Read) const;
    [[nodiscard]] static size_t binding_allocation_bound() noexcept;
    // Additional per-load progress credit when residency metrics are enabled.
    // Each subscriber needs pin_allocation_bound(), including cache hits.
    [[nodiscard]] static size_t                load_allocation_bound() noexcept;
    [[nodiscard]] static size_t                pin_allocation_bound() noexcept;
    [[nodiscard]] std::optional<size_t>        load_reserve_bound(size_t pinned_frames, size_t workers) const noexcept;
    [[nodiscard]] size_t                       retained_page_limit() const noexcept;
    [[nodiscard]] static std::optional<size_t> allocation_bound(PageCacheLimits limits) noexcept;
    [[nodiscard]] PageCacheSnapshot            snapshot() const noexcept;
    [[nodiscard]] const std::shared_ptr<PageCacheResidency> &residency() const noexcept;
    // One live PID namespace per cache; independently budgeted load bindings
    // can share it throughout that publication lifetime.
    [[nodiscard]] bool claim_publication() noexcept;
    // Nonblocking metadata commands. Accepted loads still complete; their
    // ready results are not retained if eviction/close preceded completion.
    void evict() noexcept;
    void trim(PageCacheTrim &request) noexcept;
    void close() noexcept;
    // Close admission and detach residency progress; the caller separately
    // drains accepted load/I/O owners before destroying its backend.
    void close_and_drain() noexcept;

  private:
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
