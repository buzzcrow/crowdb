// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/local_backend_state.h"
#include "btree/flush_operation.h"
#include "btree/page_split_operation.h"
#include "snapshot/captured_plan.h"

namespace crowdb::tree
{
class CheckpointBackend;

enum class CheckpointMode : uint8_t { Durable, Writeback };

struct CheckpointDiagnostics
{
    std::atomic<uint64_t> written_frames{0};
    std::atomic<uint64_t> total_written_frames{0};
};

class ApplyProgress;

struct CheckpointOptions
{
    std::shared_ptr<ApplyProgress> handoff_progress = nullptr;
    const bool                    *handoff_blocked  = nullptr;
    CheckpointMode                 mode             = CheckpointMode::Durable;
    // Core's accepted ticket retains this state through terminal. Only durable
    // commits update it, while their publication capture token is still held.
    CheckpointDiagnostics *diagnostics = nullptr;
    // Select a bounded host window from the immutable view after handoff.
    bool                             select_oldest_page = false;
    CapturedWriteLimits              write;
    LocalCheckpointLimits            metadata;
    std::shared_ptr<PublicationLane> publication = nullptr;
    TreeWorkCause                    cause       = TreeWorkCause::Checkpoint;
};

struct CheckpointWork
{
    PageLoadBudget              budget;
    std::shared_ptr<PageLoader> loader;
    std::shared_ptr<const void> owner;
};

struct CheckpointResult
{
    Status   status;
    uint64_t durable_coverage    = 0;
    uint64_t snapshot_sequence   = 0;
    uint64_t checkpoint_token    = 0;
    uint64_t captured_coverage   = 0;
    uint64_t primary_frames      = 0;
    uint64_t structural_windows  = 0;
    uint64_t written_frames      = 0;
    uint64_t written_bytes       = 0;
    uint64_t reused_frames       = 0;
    bool     durable_committed   = false;
    bool     writeback_completed = false;
    uint64_t rebased_pages       = 0;
    uint64_t rebase_deferred     = 0;
};

// Serializes a finite handoff, immutable publication capture, protected page
// materialization, local anchor commit and publication reconciliation. The
// finish partition and CPU continuation are reserved before closing sources.
// A successful durable commit must be reconciled even after cancellation.
// Writeback mode stops before anchor commit, installs reloadable
// representations and aborts the capture token without advancing the durable
// frontier.
class CheckpointOperation
{
    struct State;

  public:
    using PrepareHandoff = detail::InlineFunction<Status(std::shared_ptr<FlushOperation> *)>;
    // Called after handoff for a read/layout window, then after that window
    // drains for the observed materialization bound. A null plan means preflight.
    using PrepareWork =
        detail::InlineFunction<Status(const PublishedTreeState &, const CapturedWritePlan *, CheckpointWork *)>;

    // The sender owns one complete phase grant and writes the output before terminal.
    // The captured view, plan and output stay stable through that terminal.
    using PrepareWorkAsync = detail::InlineFunction<detail::CallbackSender(
        const PublishedTreeState &, const CapturedWritePlan *, CheckpointWork *)>;
    using PrepareCleanup   = detail::InlineFunction<void()>;
    using AfterFlush       = detail::InlineFunction<std::optional<CheckpointResult>()>;
    using PrepareStructure = detail::InlineFunction<Status(std::shared_ptr<const PublishedTreeState>, uint64_t,
                                                           std::shared_ptr<PageSplitOperation> *)>;

    static Status create(std::shared_ptr<const void> owner, PublicationCoordinator &publisher,
                         std::shared_ptr<LocalBackendState> backend, std::shared_ptr<AsyncPageStore> operation_store,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, CheckpointOptions options,
                         PageLoadBudget budget, std::shared_ptr<detail::AllocationReservation> finish_metadata,
                         std::shared_ptr<PageLoader> loader, PrepareHandoff prepare_handoff, PrepareWork prepare_work,
                         std::shared_ptr<CheckpointOperation> *out, PrepareCleanup prepare_cleanup = {},
                         PrepareWorkAsync prepare_work_async = {}, PrepareCleanup on_terminal = {},
                         PrepareCleanup cancel_work = {}, std::shared_ptr<CheckpointBackend> chunk = {},
                         PrepareStructure prepare_structure = {}, AfterFlush flush_result = {});
    [[nodiscard]] static size_t           control_allocation_bound() noexcept;
    [[nodiscard]] static size_t           finish_allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender  sender() noexcept;
    [[nodiscard]] const CheckpointResult &result() const noexcept;
    void                                  request_stop() noexcept;

  private:
    explicit CheckpointOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
