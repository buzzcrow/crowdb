// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_writeback.h"
#include "snapshot/captured_frames.h"
#include "snapshot/readable_page_catalog.h"

#include <map>

namespace crowdb::tree
{
// Only independently reloadable durable descriptors enter this type. The
// recovery adapter verifies that promise while validating the graph.
struct CapturedDurablePages : ReadablePageCatalog
{
    using ReadablePageCatalog::ReadablePageCatalog;
};

struct CapturedWriteLimits
{
    CapturedFrameLimits frames{};
    PageWriteLimits     write{};
    size_t              max_frames           = 1024 * 1024;
    size_t              max_dependency_steps = 1024 * 1024;
};

struct CapturedWriteResult
{
    Status                                      status;
    std::shared_ptr<const CapturedDurablePages> pages;
    uint64_t                                    written_frames = 0;
    uint64_t                                    written_bytes  = 0;
    uint64_t                                    reused_frames  = 0;
};

// Materializes a captured view into protected, independently readable page
// references. It does not write a directory/anchor or advance durable coverage.
// Existing overflow cells retain verified dependencies through their primary
// backing. The inherited mapping supports older callers without typed backing
// descriptors; missing or mismatched dependencies fail explicitly.
class CapturedWriteOperation
{
    struct State;

  public:
    // Sink controls and retained descriptors only. Add the stream, loader and
    // one PageWriteOperation window separately. Counts come from layout preflight.
    [[nodiscard]] static std::optional<size_t> allocation_bound(CapturedWriteLimits limits, size_t frame_count_upper,
                                                                size_t dependency_entries,
                                                                size_t reference_heads) noexcept;
    static Status create(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> loader,
                         std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedWriteLimits limits,
                         PageLoadBudget budget, CapturedFrameStream::ReserveOverflow reserve_overflow,
                         PageWriteOperation::ReserveExtent           reserve_extent,
                         std::shared_ptr<const CapturedDurablePages> inherited,
                         std::shared_ptr<CapturedWriteOperation>    *out);
    [[nodiscard]] detail::CallbackSender     sender() noexcept;
    [[nodiscard]] const CapturedWriteResult &result() const noexcept;
    void                                     request_stop() noexcept;

  private:
    explicit CapturedWriteOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
