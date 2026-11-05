// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "admitted_batch.h"
#include "apply_dispatcher.h"
#include "apply_progress.h"
#include "apply_waiters.h"
#include "flush_operation.h"
#include "replay_controls.h"
#include "source_rotation.h"
#include "tree_apply_operation.h"

namespace crowdb::tree
{
struct TreeApplyOperation::State : std::enable_shared_from_this<State>
{
    enum class Phase : uint8_t { kPrepared, kArming, kArmed, kStarted, kDone };
    enum class Wait : uint8_t { kNone, kProgress, kCredit, kApplyCredit, kFlush };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        State *owner;
        void   set_value() const noexcept;
        void   set_value(FlushResult result) const noexcept;
        void   set_error(Status status) const noexcept;
        void   set_stopped() const noexcept;
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct ProgressAttempt
    {
        ProgressAttempt(State *state, const ApplyProgress &progress, uint64_t sequence)
            : request(progress, sequence),
              connected(stdexec::connect(request.sender(), Receiver{state}))
        {
        }

        ApplyProgress::Request request;
        Connected              connected;
    };

    struct CreditAttempt
    {
        CreditAttempt(State *state, std::shared_ptr<detail::ResourceBudget::Account> account,
                      detail::BudgetClass category, size_t amount, std::shared_ptr<TreeWorkMetrics> work)
            : request(std::move(account), category, amount, state->waiters->get(category)),
              metrics(std::move(work), TreeStage::BudgetWait, tree_metrics_now_ns()),
              connected(stdexec::connect(request.sender(), Receiver{state}))
        {
        }

        detail::ResourceBudget::Request request;
        TreeMetricsOperation            metrics;
        Connected                       connected;
    };

    struct FlushAttempt
    {
        using Connected = stdexec::connect_result_t<FlushReportSender, Receiver>;

        FlushAttempt(State *state, std::shared_ptr<FlushOperation> flush)
            : operation(std::move(flush)),
              connected(stdexec::connect(operation->report_sender(), Receiver{state}))
        {
        }

        std::shared_ptr<FlushOperation> operation;
        Connected                       connected;
    };

    struct ReplayInput
    {
        size_t                      count = 0;
        CoreWriteReader             read;
        std::shared_ptr<const void> caller_owner;
        SourceWriteOptions          options;
        AdmittedBatch::Plan         bound;
    };

    State(std::weak_ptr<TreeCore> core, uint64_t generation, std::shared_ptr<AdmittedBatch> batch,
          std::shared_ptr<detail::AllocationReservation> metadata);
    Status      arm();
    void        start(uint64_t chosen_slot, detail::CallbackComplete callback, void *argument) noexcept;
    static void run(void *context) noexcept;
    void        step();
    void        start_flush();
    void        await_progress(uint64_t sequence);
    void        await_credit();
    void        credit_ready();
    bool        prepare_replay_batch();
    void        apply_credit_ready();
    void        start_pressure();
    void        bind_replay_batch(detail::ResourceBudget::Credit grant);
    void        connect_flush(std::shared_ptr<FlushOperation> operation);
    void        wake(Status status) noexcept;
    void        finish(Status status) noexcept;

    std::shared_ptr<ApplyWaiters>                  waiters;
    bool                                           owns_replay_waiters = false;
    ReplayControls::Lease                          replay_control;
    std::optional<ReplayInput>                     replay_input;
    uint64_t                                       replay_slot    = 0;
    uint64_t                                       replay_flushed = UINT64_MAX;
    std::weak_ptr<TreeCore>                        weak_core;
    const uint64_t                                 generation;
    std::shared_ptr<AdmittedBatch>                 batch;
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<TreeOperationLifetime::Ticket> apply_ticket;
    std::shared_ptr<TreeOperationLifetime::Ticket> backend_ticket;
    std::shared_ptr<SourceRotation>                source_rotation;
    std::shared_ptr<ApplyProgress>                 progress;
    std::shared_ptr<ApplyDispatcher>               dispatcher;
    std::shared_ptr<TreeWorkMetrics>               work;
    SourceAdmission                               *sources  = nullptr;
    ApplyFrontier                                 *frontier = nullptr;
    FlushController                               *flush    = nullptr;
    FlushPreparationOptions                        preparation{};
    ApplyDispatcher::Node                          node;
    std::atomic<Phase>                             phase{Phase::kPrepared};
    std::shared_ptr<State>                         self;
    uint64_t                                       slot     = 0;
    uint64_t                                       observed = 0;
    Wait                                           waiting  = Wait::kNone;
    Status                                         resumed;
    bool                                           flush_rejected = false;
    detail::CallbackComplete                       complete       = nullptr;
    void                                          *context        = nullptr;
    FlushReservePlan                               plan{};
    detail::FlushCredits                           credits;
    size_t                                         next_credit = 0;
    std::optional<TreeMetricsOperation>            admission_metrics;
    std::optional<ProgressAttempt>                 progress_attempt;
    std::optional<CreditAttempt>                   credit_attempt;
    std::optional<FlushAttempt>                    flush_attempt;
};
} // namespace crowdb::tree
