// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_encode.h"
#include "btree/page_load.h"
#include "btree/resource_observation.h"

namespace crowdb::tree
{
struct PrimaryReuseOutput;
class WritebackMetrics;

struct CapturedFrameLimits
{
    PageEncodeLimits encode{};
    size_t           max_depth = 64;
    // Derived from captured MappingView copy bounds before admission. A work
    // window limits cursor state, not the source arrays retained by a leaf.
    size_t page_metadata_bytes = 0;
    // Maximum typed range bases in a captured leaf, derived before admission.
    uint64_t max_pages = MappingView::kPageLimit;
    // Traverse and prepare final layouts without copying payload or reserving
    // overflow PIDs. Each result owns the returned dependency descriptors.
    bool plan_only = false;
    // Staged writeback can materialize one primary while retaining the complete
    // captured mapping for routing and immutable overflow ownership proofs.
    std::optional<uint64_t> selected_page = std::nullopt;
    // Checkpoint-only: reuse verified clean primaries in this exact backend.
    uint64_t                          reuse_backend_id = 0;
    BackingExclusion                  exclusion{};
    std::shared_ptr<WritebackMetrics> writeback_metrics = nullptr;
    std::shared_ptr<ApplyDispatcher>  cpu_progress      = nullptr;
};

struct CapturedFrameBuffer
{
    explicit CapturedFrameBuffer(const PageEncodeLimits &limits)
        : resources(limits.metadata, TreeResourceMetric::MaterializeInflightBytes),
          bytes(limits.frame_bytes, detail::ReservationAllocator<uint8_t, AllocationKind::kFrame>(limits.metadata))
    {
        resources.begin(bytes.capacity());
    }

    ResourceObservation                                     resources;
    detail::ReservedVector<uint8_t, AllocationKind::kFrame> bytes;
};

struct CapturedFrame
{
    EncodedFrameInfo                           info;
    std::shared_ptr<const CapturedFrameBuffer> buffer;
    // Primary-frame dependencies keep this span and referenced old overflow
    // chains alive. The sink must carry them into its durable mapping/owner.
    std::span<const ReusedOverflowDependency> reused_overflow;
    std::optional<FinalPageSummary>           summary = std::nullopt;
    std::shared_ptr<const PrimaryReuseOutput> reuse   = nullptr;
};

struct CapturedFrameResult
{
    Status                       status;
    std::optional<CapturedFrame> frame;
    bool                         complete = false;

    struct Plan
    {
        uint64_t                                  page_id = kInvalidPageId;
        FinalPageSummary                          summary;
        std::span<const ReusedOverflowDependency> reused_overflow;
        std::shared_ptr<const void>               owner;
        bool                                      reused_primary = false;
    };

    std::optional<Plan> plan = std::nullopt;
    // Fatal immutable-layout result. The caller may split a still-current L1
    // representation, then build a new layout proof before capture.
    std::optional<uint64_t> needs_split = std::nullopt;
};

struct CapturedFrameStats
{
    uint64_t           pages     = 0;
    uint64_t           frames    = 0;
    uint64_t           loads     = 0;
    uint64_t           cpu_steps = 0;
    PageEncodeCounters encoding;
};

// Postorder materialization of one immutable captured L1 mapping. Each sender
// invocation yields at most one raw frame; callers write/release that window
// before requesting another. Existing overflow references are reported to the
// sink, not silently omitted from its persistence dependencies. This is not a
// self-contained native export until that sink also emits the reused chains.
class CapturedFrameStream
{
    struct State;

  public:
    using ReserveOverflow = detail::InlineFunction<Status(uint64_t, ReservedOverflowIds *)>;

    static Status create(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> loader,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedFrameLimits limits,
                         ReserveOverflow reserve_overflow, std::shared_ptr<CapturedFrameStream> *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(const CapturedFrameLimits &limits) noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const CapturedFrameResult   &result() const noexcept;
    [[nodiscard]] const CapturedFrameStats    &stats() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit CapturedFrameStream(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
