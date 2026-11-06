// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_load.h"
#include "btree/published_state.h"
#include "stdexec_adapter.h"

namespace crowdb::tree
{
struct PublishedReadLimits
{
    size_t max_memtables    = 64;
    size_t max_page_sources = 64;
    size_t max_key_bytes    = 256 * 1024;
    size_t max_route_depth  = 64;
    size_t max_work         = 4096;
    size_t max_entries      = 128;
    size_t max_result_bytes = 1024 * 1024;
    // Captured mapping shape, independent of the cursor's source work window.
    size_t                            page_metadata_bytes = 0;
    std::shared_ptr<AllocationBudget> metadata            = nullptr;
};

struct PublishedGetResult
{
    Status      status;
    bool        found = false;
    LogicalCell cell;
};

struct PublishedScanEntry
{
    OwnedKey    key;
    LogicalCell cell;
};

struct PublishedScanResult
{
    Status                                     status;
    detail::ReservedVector<PublishedScanEntry> entries;
    bool                                       done = false;
    // An eligible candidate is retained because it cannot fit the byte budget.
    bool   byte_limit_reached = false;
    size_t work               = 0;
};

struct PublishedScanOptions
{
    std::optional<OwnedKey> start                 = std::nullopt;
    bool                    start_inclusive       = false;
    bool                    keys_only             = false;
    bool                    include_tombstones    = false;
    bool                    allow_oversized_first = false;
    // When absent, each connection applies max_result_bytes independently.
    // A supplied budget spans every work window; SIZE_MAX means no public cap.
    std::optional<size_t> byte_budget = std::nullopt;
};

class PublishedGet
{
    struct State;

  public:
    static Status create(std::shared_ptr<const PublishedTreeState> view, std::shared_ptr<PageLoader> loader, Slice key,
                         PublishedReadLimits limits, std::shared_ptr<PublishedGet> *out);
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] static std::optional<size_t> allocation_bound(const PublishedReadLimits &limits,
                                                                size_t                     workers) noexcept;
    [[nodiscard]] const PublishedGetResult    &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit PublishedGet(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};

// This object is the continuation: mapping/catalog/floor and source positions
// stay fixed across windows. Each next_sender() connection produces one bounded
// window; an empty nonterminal window is valid after skipped/covered entries.
// Values own their bytes independently; no epoch guard crosses a callback.
class PublishedScan
{
    struct State;

  public:
    static Status create(std::shared_ptr<const PublishedTreeState> view, std::shared_ptr<PageLoader> loader,
                         PageKeyRange range, bool reverse, PublishedReadLimits limits,
                         std::shared_ptr<PublishedScan> *out, PublishedScanOptions options = {});
    [[nodiscard]] detail::CallbackSender       next_sender() noexcept;
    [[nodiscard]] static std::optional<size_t> allocation_bound(const PublishedReadLimits &limits,
                                                                size_t                     workers) noexcept;
    [[nodiscard]] const PublishedScanResult   &result() const noexcept;
    [[nodiscard]] uint64_t                     skipped_subtrees() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit PublishedScan(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
