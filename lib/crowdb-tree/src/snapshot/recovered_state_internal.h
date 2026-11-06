// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/recovered_state.h"

#include <set>

namespace crowdb::tree
{
struct RecoveredStateOperation::State : std::enable_shared_from_this<State>
{
    enum class Phase : uint8_t { Load, Inner, Children, Leaf, Dependencies, Finish };

    struct Bounds
    {
        std::optional<OwnedKey> lower;
        std::optional<OwnedKey> upper;
        uint64_t                lower_leaf = kInvalidPageId;
        uint64_t                upper_leaf = kInvalidPageId;
    };

    struct Path
    {
        uint64_t                              pid        = kInvalidPageId;
        const DurableCapturedFrame           *descriptor = nullptr;
        PageKeyRange                          range;
        Bounds                                bounds;
        std::shared_ptr<const InnerPageState> inner;
        std::shared_ptr<const LeafBase>       leaf;
        PageMetricsContribution               metrics;
        FinalPageSummary                      summary;
        std::optional<uint32_t>               child_level;
        size_t                                index       = 0;
        size_t                                key_bytes   = 0;
        size_t                                dependency  = 0;
        uint64_t                              chain_pid   = kInvalidPageId;
        uint64_t                              chain_bytes = 0;
        Phase                                 phase       = Phase::Load;
    };

    struct Load
    {
        static constexpr unsigned             kReturned = 1;
        static constexpr unsigned             kCallback = 2;
        static constexpr unsigned             kFailed   = 4;
        Status                                status;
        Status                                submission;
        std::shared_ptr<const LeafBase>       leaf;
        std::shared_ptr<const InnerPageState> inner;
        std::atomic<bool>                     claimed{false};
        std::atomic<unsigned>                 events{0};
    };

    using Pids = std::set<uint64_t, std::less<>, detail::ReservationAllocator<uint64_t>>;
    explicit State(std::shared_ptr<detail::AllocationReservation> credit);
    void        start(detail::CallbackComplete callback, void *argument) noexcept;
    void        wake() noexcept;
    static void run(void *context) noexcept;
    void        pump();
    void        push(uint64_t pid, PageKeyRange range);
    void        begin_load();
    void        loaded();
    void        inner_step();
    void        child_step();
    void        leaf_step();
    void        dependency_step();
    void        complete_page();
    bool        install_page(uint32_t level);
    void        verify_reachable();
    void        finish(Status status) noexcept;

    std::shared_ptr<detail::AllocationReservation>  metadata;
    std::shared_ptr<const ReadablePageCatalog>      captured;
    std::shared_ptr<PageLoader>                     loader;
    std::shared_ptr<detail::FlushRuntime>           runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>     lane;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    RecoveredStateLimits                            limits;
    std::shared_ptr<PublishedTreeState>             building;
    detail::ReservedVector<Path>                    path;
    Pids                                            reached;
    std::shared_ptr<Load>                           load;
    ReadablePageCatalog::Frames::const_iterator     position;
    uint64_t                                        previous_sibling = kInvalidPageId;
    bool                                            have_leaf        = false;
    bool                                            initialized      = false;
    bool                                            require_durable  = false;
    size_t                                          dependency_steps = 0;
    std::atomic<uint8_t>                            lifecycle{0};
    std::atomic<bool>                               stopped{false};
    detail::CallbackComplete                        complete = nullptr;
    void                                           *context  = nullptr;
    RecoveredStateResult                            report;
};
} // namespace crowdb::tree
