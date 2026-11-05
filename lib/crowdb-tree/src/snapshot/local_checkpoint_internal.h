// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/mapping_geometry.h"
#include "snapshot/local_checkpoint.h"
#include "snapshot/metadata_codec.h"

namespace crowdb::tree
{
struct LocalCheckpointOperation::State : std::enable_shared_from_this<State>
{
    enum class Stage : uint8_t {
        Begin,
        Segment,
        SegmentReserved,
        SegmentWritten,
        DirectoryReserved,
        DirectoryWrite,
        DirectoryWritten,
        BeforeAnchor,
        AnchorWritten,
        Commit
    };

    struct Window
    {
        Window(size_t size, size_t alignment, const std::shared_ptr<detail::AllocationReservation> &budget);
        detail::ReservedVector<uint8_t, AllocationKind::kIo> storage;
        std::span<uint8_t>                                   bytes;
    };

    // Each submission has its own identity and retains its buffer/extent even
    // when submission throws after transport acceptance. An inline callback
    // cannot advance the state machine until submit has returned successfully.
    struct IoStep
    {
        explicit IoStep(const PageLoadBudget &budget) : credit(budget.io, 1)
        {
        }

        static void                                completed(void *context, Status status) noexcept;
        void                                       returned(Status status) noexcept;
        void                                       publish() noexcept;
        std::shared_ptr<State>                     owner;
        std::shared_ptr<Window>                    window;
        std::shared_ptr<const ProtectedCheckpoint> checkpoint;
        AllocationLease                            credit;
        Status                                     completion_status;
        Status                                     submission_status;
        std::atomic<unsigned>                      events{0};
        std::atomic<bool>                          claimed{false};
        std::atomic<bool>                          reported{false};
    };

    explicit State(PageLoadBudget credits);
    void        start(detail::CallbackComplete callback, void *argument) noexcept;
    void        wake() noexcept;
    static void run(void *context) noexcept;
    void        pump();
    void        begin();
    void        segment();
    Status      pack_frame(const DurableCapturedFrame &frame, uint64_t *word);
    void        directory_window();
    void        anchor();
    void        reserve(uint64_t length, Stage next);
    void        io(bool barrier, uint64_t address, size_t bytes, Stage next, bool anchor = false);
    void        event(Status status) noexcept;
    void        finish(Status status) noexcept;

    PageLoadBudget                               budget;
    LocalCheckpointLimits                        limits;
    std::shared_ptr<LocalCheckpointRegistry>     registry;
    std::shared_ptr<AsyncPageStore>              store;
    std::shared_ptr<detail::FlushRuntime>        runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>  lane;
    std::shared_ptr<ProtectedCheckpoint>         image;
    std::shared_ptr<Window>                      window;
    detail::ReservedVector<DirEntry>             directory;
    std::optional<snapshot::DirectoryEncoder>    encoder;
    std::array<uint64_t, kMappingSegmentSize>    words{};
    CapturedDurablePages::Frames::const_iterator position;
    DirEntry                                     segment_entry;
    ProtectedExtent                              reserved;
    uint64_t                                     directory_address = 0;
    uint64_t                                     directory_offset  = 0;
    size_t                                       segment_bytes     = 0;
    uint32_t                                     iu                = 0;
    Stage                                        stage             = Stage::Begin;
    bool                                         admitted          = false;
    bool                                         anchor_touched    = false;
    bool                                         waiting           = false;
    Status                                       event_status;
    Status                                       dispatch_failure;
    std::atomic<bool>                            event_ready{false};
    std::atomic<bool>                            stopped{false};
    std::atomic<bool>                            scheduled{false};
    std::atomic<bool>                            wake_requested{false};
    std::atomic<uint8_t>                         lifecycle{0};
    std::shared_ptr<State>                       queued_owner;
    detail::CallbackComplete                     complete = nullptr;
    void                                        *context  = nullptr;
    LocalCheckpointResult                        report;
};
} // namespace crowdb::tree
