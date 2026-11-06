// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/compressor.h"
#include "page_encode.h"
#include "page_load.h"

namespace crowdb::tree
{
struct PageWriteLimits
{
    size_t        max_frame_bytes  = 64 * 1024;
    uint32_t      iu_size          = 4096;
    size_t        buffer_alignment = 4096;
    compress_algo compression      = compress_algo::kLz4;
    bool          chunk_backed     = false;
};

struct PageWriteInput
{
    EncodedFrameInfo            info;
    std::span<const uint8_t>    frame;
    std::shared_ptr<const void> owner;
    // Protects actual referenced overflow extents and their captured resolver.
    // The encoder's PID reservation alone does not protect those byte ranges.
    // This is retained by the resulting backing, independently of raw input.
    std::shared_ptr<const void> reload_dependencies;
};

struct ReservedWriteExtent
{
    uint64_t backend_id = 0;
    uint64_t address    = 0;
    uint64_t length     = 0;
    uint64_t content_id = 0;
    uint64_t generation = 0;
    // Held before submission, including on error/unknown write outcome, until
    // the final transport callback and every published backing release it.
    std::shared_ptr<const void>        owner;
    std::weak_ptr<BackingMetricExtent> physical_metrics;
    std::weak_ptr<ChunkPackBacking>    chunk;
};

struct PageWriteResult
{
    Status                            status;
    std::shared_ptr<const BackingRef> backing;
    uint32_t                          logical_bytes  = 0;
    uint64_t                          physical_bytes = 0;
};

class PageWriteOperation
{
    struct State;

  public:
    using ExtentComplete = detail::InlineFunction<void(Status, ReservedWriteExtent), 32>;
    using ReserveExtent  = detail::InlineFunction<void(uint64_t, ExtentComplete)>;
    static Status create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageWriteInput input, PageWriteLimits limits,
                         PageLoadBudget budget, ReserveExtent reserve, std::shared_ptr<PageWriteOperation> *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(PageWriteLimits limits) noexcept;
    [[nodiscard]] static size_t                retained_allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const PageWriteResult       &result() const noexcept;
    // In-flight I/O is drained; stop never releases/reuses its buffer early.
    void request_stop() noexcept;

  private:
    explicit PageWriteOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
