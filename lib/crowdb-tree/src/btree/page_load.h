// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/flush_runtime.h"
#include "btree/inline_function.h"
#include "btree/logical_page.h"
#include "btree/tree_work_counters.h"
#include "crowdb-tree/backend/async_page_store.h"

#include <functional>

namespace crowdb::tree
{
struct InnerPageState;
class TreeWorkMetrics;
class ApplyDispatcher;

class PageLoader
{
  public:
    using InnerComplete   = detail::InlineFunction<void(Status, std::shared_ptr<const InnerPageState>)>;
    using LeafComplete    = detail::InlineFunction<void(Status, std::shared_ptr<const LeafBase>)>;
    using ValueComplete   = detail::InlineFunction<void(Status, std::shared_ptr<const buffer>)>;
    virtual ~PageLoader() = default;
    // Exactly one nonthrowing callback, possibly inline. No blocking fallback.
    virtual void load_leaf(BaseRef base, LeafComplete complete) = 0;
    virtual void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete);
    virtual void load_overflow(uint64_t head, uint64_t length, ValueComplete complete);
    virtual void load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                               ValueComplete complete);
};

struct PageLoadLimits
{
    size_t max_inflight        = 32;
    size_t max_blob_bytes      = 1024 * 1024;
    size_t max_frame_bytes     = 1024 * 1024;
    size_t max_value_bytes     = 64 * 1024 * 1024;
    size_t max_overflow_pages  = 65536;
    size_t max_inner_children  = 4096;
    size_t max_inner_key_bytes = 256 * 1024;
    size_t max_leaf_entries    = 4096;
    size_t max_leaf_key_bytes  = 256 * 1024;
    // Zero disables typed Chunk I/O. The extra framing window is included in
    // every admitted read bound when a Chunk domain is selected.
    size_t max_chunk_blob_bytes = 0;
};

// Supplied before a flush closes sources. The loader spends this credit
// directly; it never waits for a second host reservation while holding it.
struct PageLoadBudget
{
    std::shared_ptr<detail::AllocationReservation> bytes;
    std::shared_ptr<detail::AllocationReservation> io;
    std::shared_ptr<TreeWorkMetrics>               work_metrics = nullptr;
    std::shared_ptr<AllocationBudget>              backend      = nullptr;
    std::shared_ptr<ApplyDispatcher>               cpu_progress = nullptr;
    TreeWorkCause                                  cause        = TreeWorkCause::Other;
};

// The resolver must be bound to the caller's captured backing/mapping view and
// return independently owned extents. The loader never consults a mutable root.
using OverflowBackingResolver = detail::InlineFunction<Status(uint64_t, std::shared_ptr<const BackingRef> *)>;

class AsyncStorePageLoader final : public PageLoader
{
    struct State;

  public:
    // The host retains and drains the runtime off its workers. I/O completion
    // only schedules decoding; CPU work never runs on the transport thread.
    AsyncStorePageLoader(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadLimits limits = {},
                         OverflowBackingResolver overflow = {}, PageLoadBudget budget = {});
    void load_leaf(BaseRef base, LeafComplete complete) override;
    void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override;
    void load_overflow(uint64_t head, uint64_t length, ValueComplete complete) override;
    void load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                       ValueComplete complete) override;
    // Serial routing can retain one path plus completed decoder steps that
    // have handed their result to a continuation but not yet returned.
    [[nodiscard]] static std::optional<size_t> routing_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                                     size_t workers, size_t cached_pages = 0) noexcept;
    // Primary-page maintenance preserves overflow references without loading
    // their payloads. Includes inner paths, leaf normalization, and decoder
    // callback tails, but no overflow chain or materialized value storage.
    [[nodiscard]] static std::optional<size_t>
    primary_reserve_bound(const PageLoadLimits &limits, size_t depth, size_t workers, size_t retained_leaf_pages,
                          size_t cached_pages = 0, std::optional<size_t> cached_inner_pages = {}) noexcept;
    // Serial captured reads retain their current leaf sources and previously
    // emitted frame owners. value_bytes bounds all simultaneous materialized
    // overflow payloads, including results retained across decoder callbacks.
    [[nodiscard]] static std::optional<size_t>
    read_reserve_bound(const PageLoadLimits &limits, size_t depth, size_t workers, size_t retained_leaf_pages,
                       size_t retained_values, size_t value_bytes, size_t cached_pages = 0,
                       std::optional<size_t> cached_inner_pages = {}) noexcept;

  private:
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
