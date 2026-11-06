// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "btree/writeback_metrics.h"
#include "snapshot/captured_rebase.h"

namespace crowdb::tree
{
struct CapturedRebaseOperation::State : std::enable_shared_from_this<State>
{
    explicit State(PublicationCoordinator &source) : publisher(source)
    {
    }

    static void                      run(void *context) noexcept;
    void                             wake() noexcept;
    void                             step();
    void                             prepare_batch();
    void                             add_replacement(uint64_t pid, std::shared_ptr<const PublishedPage> current,
                                                     std::shared_ptr<const PublishedPage> replacement);
    void                             defer(uint64_t pid, Status status);
    void                             finish_batch(TreeOutcome outcome);
    void                             summary_step();
    void                             finish(Status status) noexcept;
    ApplyDispatcher::Node            cpu_node{&State::run, this};
    std::shared_ptr<ApplyDispatcher> cpu_progress;
    std::shared_ptr<PublicationLane> publication;
    PublicationCoordinator          &publisher;
    std::shared_ptr<const PublishedTreeState>       captured;
    std::shared_ptr<const CapturedDurablePages>     pages;
    CapturedDurablePages::Frames::const_iterator    next;
    std::shared_ptr<detail::FlushRuntime>           runtime;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    std::shared_ptr<detail::AllocationReservation>  metadata;
    std::shared_ptr<PublicationRequest>             request;
    std::shared_ptr<WritebackMetrics>               writeback_metrics;
    size_t                                          max_sources     = 0;
    bool                                            waiting         = false;
    bool                                            summarizing     = false;
    bool                                            summary_pending = false;
    std::shared_ptr<const PublishedTreeState>       summary_view;
    TreeSummary                                     summary;
    Status                                          publication_status;
    std::atomic<bool>                               ready{false};
    std::atomic<bool>                               stopped{false};
    std::atomic<uint8_t>                            lifecycle{0};
    detail::CallbackComplete                        complete = nullptr;
    void                                           *context  = nullptr;
    CapturedRebaseResult                            report;
    TreeWorkCause                                   cause = TreeWorkCause::Checkpoint;
};
} // namespace crowdb::tree
