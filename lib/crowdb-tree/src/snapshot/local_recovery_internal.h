// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/overflow_backing.h"
#include "crowdb-tree/maptable/mapping_geometry.h"
#include "snapshot/local_recovery.h"
#include "snapshot/metadata_decode.h"

namespace crowdb::tree
{
struct LocalRecoveryOperation::State : std::enable_shared_from_this<State>
{
    enum class Stage : uint8_t {
        NextAnchor,
        AnchorRead,
        DirectoryProtected,
        DirectoryRead,
        Segment,
        SegmentProtected,
        SegmentRead,
        PageExtent,
        PageProtected,
        NextFrame,
        FrameRead,
        Dependencies,
        Activated
    };

    struct Window
    {
        Window(size_t blob_bytes, size_t frame_bytes, size_t alignment,
               const std::shared_ptr<detail::AllocationReservation> &metadata);
        detail::ReservedVector<uint8_t, AllocationKind::kIo>      storage;
        detail::ReservedVector<uint8_t, AllocationKind::kScratch> frame;
        std::span<uint8_t>                                        bytes;
    };

    struct ReadStep
    {
        explicit ReadStep(const PageLoadBudget &budget) : credit(budget.io, 1)
        {
        }

        static void             completed(void *context, Status status) noexcept;
        void                    returned(Status status) noexcept;
        void                    publish() noexcept;
        std::shared_ptr<State>  owner;
        std::shared_ptr<Window> window;
        AllocationLease         credit;
        Status                  completion_status;
        Status                  submission_status;
        std::atomic<unsigned>   events{0};
        std::atomic<bool>       claimed{false};
        std::atomic<bool>       reported{false};
    };

    struct LeafReference
    {
        uint64_t page_id;
        uint64_t head;
        uint64_t bytes;
    };

    struct LeafProtection
    {
        bool retire_step() noexcept
        {
            for (size_t i = 0; i < 64 && !overflow.empty(); ++i) {
                overflow.pop_back();
            }
            return overflow.empty();
        }

        explicit LeafProtection(const std::shared_ptr<detail::AllocationReservation> &budget)
            : overflow(detail::ReservationAllocator<std::shared_ptr<const void>>(budget))
        {
        }

        std::shared_ptr<const void>                         primary;
        detail::ReservedVector<std::shared_ptr<const void>> overflow;
    };

    explicit State(PageLoadBudget credits);
    void        start(detail::CallbackComplete callback, void *argument) noexcept;
    void        wake() noexcept;
    static void run(void *context) noexcept;
    void        pump();
    void        next_anchor();
    void        anchor_read();
    void        read_directory();
    void        directory_read();
    void        segment();
    void        segment_read();
    void        page_extent();
    void        page_protected();
    bool        valid_extent(uint64_t address, uint64_t bytes) const noexcept;
    void        protect(uint64_t address, uint64_t bytes, Stage next);
    void        read(uint64_t address, size_t bytes, Stage next);
    void        event(Status status) noexcept;
    void        finish(Status status) noexcept;

    // Frame/bootstrap implementation lives in local_recovery_frames.cpp.
    void   next_frame();
    void   frame_read();
    Status collect_cell(uint64_t page_id, Slice raw);
    void   dependencies();
    void   image_complete();

    PageLoadBudget                                            budget;
    LocalRecoveryLimits                                       limits;
    ExtentLedgerLimits                                        domain;
    uint64_t                                                  readable_bytes = 0;
    std::shared_ptr<AsyncPageStore>                           store;
    std::shared_ptr<detail::FlushRuntime>                     runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>               lane;
    std::shared_ptr<ExtentLedger>                             ledger;
    std::shared_ptr<Window>                                   window;
    std::array<std::shared_ptr<const ProtectedCheckpoint>, 2> anchors;
    std::shared_ptr<ProtectedCheckpoint>                      image;
    std::shared_ptr<CapturedDurablePages>                     pages;
    detail::ReservedVector<DirEntry>                          directory;
    std::optional<snapshot::DirectoryDecoder>                 decoder;
    std::array<uint64_t, kMappingSegmentSize>                 words{};
    ProtectedExtent                                           recovered;
    size_t                                                    slot             = 0;
    size_t                                                    directory_offset = 0;
    size_t                                                    segment_index    = 0;
    size_t                                                    word_index       = 0;
    size_t                                                    read_bytes       = 0;
    size_t                                                    metadata_window  = 0;
    CapturedDurablePages::Frames::iterator                    frame_position;
    detail::ReservedVector<LeafReference>                     references;
    size_t                                                    reference_index  = 0;
    uint64_t                                                  chain_pid        = kInvalidPageId;
    uint64_t                                                  chain_bytes      = 0;
    size_t                                                    dependency_steps = 0;
    uint64_t                                                  protected_leaf   = kInvalidPageId;
    std::shared_ptr<LeafProtection>                           protection;
    std::shared_ptr<OverflowBackingSet>                       overflow_backing;
    uint64_t                                                  leaf_count  = 0;
    uint64_t                                                  inner_count = 0;
    Stage                                                     stage       = Stage::NextAnchor;
    bool                                                      waiting     = false;
    Status                                                    event_status;
    Status                                                    dispatch_failure;
    std::atomic<bool>                                         event_ready{false};
    std::atomic<bool>                                         stopped{false};
    std::atomic<bool>                                         scheduled{false};
    std::atomic<bool>                                         wake_requested{false};
    std::atomic<uint8_t>                                      lifecycle{0};
    std::shared_ptr<State>                                    queued_owner;
    detail::CallbackComplete                                  complete = nullptr;
    void                                                     *context  = nullptr;
    std::shared_ptr<LocalRecoveryResult>                      report;
};
} // namespace crowdb::tree
