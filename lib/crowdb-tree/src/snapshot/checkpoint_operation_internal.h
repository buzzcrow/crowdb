// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/checkpoint_backend.h"
#include "btree/apply_dispatcher.h"
#include "snapshot/captured_rebase.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
struct CheckpointOperation::State : std::enable_shared_from_this<State>
{
    enum class Stage : uint8_t {
        Begin,
        Handoff,
        PrepareLayout,
        PrepareWrite,
        PlanPage,
        PlanDependencies,
        Structure,
        Capture,
        Write,
        Checkpoint,
        Reconcile,
        Rebase,
        FinishMetrics
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;
        void                   set_value() noexcept;
        void                   set_value(FlushResult report) noexcept;
        void                   set_error(Status status) noexcept;
        void                   set_stopped() noexcept;
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        Connected connected;
    };

    struct HandoffAttempt
    {
        HandoffAttempt(FlushReportSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        stdexec::connect_result_t<FlushReportSender, Receiver> connected;
    };

    explicit State(PublicationCoordinator &coordinator) : publisher(coordinator)
    {
    }

    void                   start(detail::CallbackComplete callback, void *argument) noexcept;
    void                   wake() noexcept;
    static void            run(void *context) noexcept;
    void                   pump();
    void                   begin_child(detail::CallbackSender sender, Stage next);
    void                   begin_handoff();
    void                   child_done(Status status) noexcept;
    void                   cancel_child();
    void                   preflight();
    void                   begin_structure(uint64_t pid);
    void                   structured();
    void                   prepare_phase(bool writing);
    void                   phase_ready(bool writing);
    void                   begin_layout();
    void                   begin_materialization();
    void                   planned_page();
    void                   plan_dependencies();
    void                   planned();
    void                   capture();
    void                   captured();
    void                   written();
    void                   checkpointed();
    void                   begin_chunk_checkpoint();
    [[nodiscard]] bool     backend_needs_recovery() const noexcept;
    [[nodiscard]] uint64_t backend_identity() const noexcept;
    [[nodiscard]] std::shared_ptr<const CapturedDurablePages> inherited_pages() const;
    void                                                      reconcile(Status status);
    void                                                      begin_rebase();
    void                                                      record_rebase_result() noexcept;
    void                                                      finish_rebase(Status status);
    void                                                      fail(Status status) noexcept;
    void                                                      finish(Status status) noexcept;

    ApplyDispatcher::Node                           cpu_node{&State::run, this};
    std::shared_ptr<ApplyDispatcher>                cpu_progress;
    PrepareWorkAsync                                prepare_work_async;
    PrepareCleanup                                  on_terminal;
    PrepareCleanup                                  cancel_work;
    CheckpointWork                                  prepared_work;
    PublicationCoordinator                         &publisher;
    std::shared_ptr<const void>                     owner;
    std::shared_ptr<LocalBackendState>              backend;
    std::shared_ptr<CheckpointBackend>              chunk;
    uint64_t                                        chunk_wal_offset = 0;
    std::shared_ptr<BackendCheckpoint>              chunk_checkpoint;
    std::shared_ptr<AsyncPageStore>                 store;
    std::shared_ptr<detail::FlushRuntime>           runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>     lane;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    CheckpointOptions                               options;
    size_t                                          max_layout_depth = 64;
    PageLoadBudget                                  budget;
    std::shared_ptr<detail::AllocationReservation>  finish_metadata;
    std::shared_ptr<PageLoader>                     loader;
    PrepareHandoff                                  prepare_handoff;
    PrepareStructure                                prepare_structure;
    std::shared_ptr<PageSplitOperation>             structure;
    std::optional<uint64_t>                         fixed_coverage;
    std::optional<uint64_t>                         fixed_generation;
    std::optional<uint64_t>                         structure_l1_seq;
    PrepareWork                                     prepare_work;
    PrepareCleanup                                  prepare_cleanup;
    std::shared_ptr<const void>                     work_owner;
    std::shared_ptr<detail::AllocationReservation>  control_metadata;
    std::shared_ptr<CapturedFrameStream>            planning_stream;
    detail::ReservedUnique<CapturedLayoutPlanner>   planner;
    CapturedWritePlan                               plan;
    bool                                            last_plan      = false;
    uint64_t                                        overflow_begin = 0;
    std::shared_ptr<PublicationRequest>             capture_request;
    std::shared_ptr<PublicationRequest>             finish_request;
    PublicationResult                               publication;
    std::shared_ptr<const PublishedTreeState>       view;
    std::shared_ptr<FlushOperation>                 handoff;
    std::shared_ptr<CapturedWriteOperation>         write;
    std::shared_ptr<LocalCheckpointOperation>       checkpoint;
    std::shared_ptr<const CapturedDurablePages>     written_pages;
    std::shared_ptr<SnapshotBaseline>               baseline;
    std::shared_ptr<CapturedRebaseOperation>        rebase;
    std::optional<Attempt>                          attempt;
    std::optional<HandoffAttempt>                   handoff_attempt;
    Stage                                           stage            = Stage::Begin;
    bool                                            waiting          = false;
    bool                                            capture_accepted = false;
    Status                                          child_status;
    Status                                          outcome;
    std::atomic<bool>                               child_ready{false};
    std::atomic<bool>                               stopped{false};
    std::atomic<uint8_t>                            lifecycle{0};
    detail::CallbackComplete                        complete = nullptr;
    void                                           *context  = nullptr;
    CheckpointResult                                report;
    std::shared_ptr<WritebackMetrics>               writeback_metrics;
    Status                                          final_status;
    TreeOutcome                                     final_outcome = TreeOutcome::Cancelled;
    std::optional<TreeMetricsOperation>             capture_metrics;
    std::optional<TreeMetricsOperation>             commit_metrics;
};
} // namespace crowdb::tree
