// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "apply_dispatcher.h"
#include "apply_progress.h"
#include "flush_operation.h"

namespace crowdb::tree
{
struct SourceRotationOptions
{
    size_t bytes   = 0;
    size_t entries = 0;
    size_t tables  = 2;
    // Actual reader and flush source capacity, including inherited tables.
    size_t source_window = 64;
};

// A Tree owns this controller through the last accepted rotation and finite
// handoff. Publication and writer catalogs advance together; no new source is
// writable before readers can find it. One dispatcher node serves all writers.
class SourceRotation : public std::enable_shared_from_this<SourceRotation>
{
  public:
    using PrepareFlush = detail::InlineFunction<Status(std::shared_ptr<FlushOperation> *)>;
    SourceRotation(SourceAdmission &sources, PublicationCoordinator &publisher,
                   std::shared_ptr<ApplyDispatcher> dispatcher, std::shared_ptr<ApplyProgress> progress,
                   std::shared_ptr<detail::ResourceBudget::Account> account, SourceRotationOptions options);
    using CheckRotation =
        detail::InlineFunction<Status(const PublishedTreeState &, const SourceCatalog &, WriteFootprint)>;
    void set_flush(PrepareFlush prepare, CheckRotation check = {});
    using PrepareOptions = detail::InlineFunction<Status(FlushPreparationOptions *)>;
    void set_async_flush(FlushController &controller, PrepareOptions prepare);
    // Zero means no threshold was crossed. Success of the write is independent
    // of this best-effort soft maintenance admission.
    [[nodiscard]] uint64_t request() noexcept;
    void                   wait(uint64_t sequence) noexcept;
    void                   drain() noexcept;
    // Fence only unaccepted soft maintenance; accepted flushes still finish.
    void stop() noexcept;
    void restart() noexcept;
    // Host-only, after old generation ingress, operations and this controller drain.
    void rebind(SourceAdmission &sources, FlushController &controller) noexcept;

  private:
    enum class Phase : uint8_t {
        Idle,
        Pausing,
        Paused,
        Publishing,
        Acknowledge,
        Flushing,
        FlushReady,
        FlushPlanning,
        CreditWaiting,
        CreditReady,
        ProgressWaiting,
        ProgressReady
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        SourceRotation *owner;
        void            set_value(FlushResult report) const noexcept;
        void            set_error(Status status) const noexcept;
        void            set_stopped() const noexcept;
    };

    enum class FlushRetry : uint8_t { None, Progress, Replan };
    using Connected = stdexec::connect_result_t<FlushReportSender, Receiver>;
    bool        eligible(const SourceCatalog &catalog) const noexcept;
    static void run(void *context) noexcept;
    void        schedule() noexcept;
    void        step();
    static void paused(void *context) noexcept;
    void        resume() noexcept;
    void        prepare(const std::shared_ptr<const SourceCatalog> &catalog);
    void        flush();
    void        flush_done() noexcept;
    void        plan_flush();
    void        await_credit();
    void        credit_ready();
    void        await_progress(uint64_t observed);
    bool        wait_step(Phase phase);
    void        start_flush();
    void        finish() noexcept;

    SourceAdmission                                 *sources_;
    PublicationCoordinator                          &publisher_;
    std::shared_ptr<ApplyDispatcher>                 dispatcher_;
    std::shared_ptr<ApplyProgress>                   progress_;
    std::shared_ptr<detail::ResourceBudget::Account> account_;
    const SourceRotationOptions                      options_;
    PrepareFlush                                     prepare_flush_;
    CheckRotation                                    check_rotation_;
    PrepareOptions                                   prepare_options_;
    FlushController                                 *flush_controller_ = nullptr;
    FlushPreparationOptions                          preparation_;
    FlushReservePlan                                 plan_;
    detail::FlushCredits                             credits_;
    size_t                                           next_credit_ = 0;
    std::array<detail::ResourceBudget::Registration, detail::FlushReserveBounds::categories.size()> registrations_;
    Status                                                                                          resumed_;
    FlushRetry                                     flush_retry_ = FlushRetry::None;
    std::atomic<bool>                              stopping_{false};
    uint64_t                                       observed_ = 0;
    std::shared_ptr<const SourceCatalog>           paused_catalog_;
    std::shared_ptr<SourceRotation>                paused_owner_;
    ApplyDispatcher::Node                          node_;
    std::atomic<uint64_t>                          requested_{0};
    std::atomic<uint64_t>                          completed_{0};
    uint64_t                                       target_ = 0;
    std::atomic<Phase>                             phase_{Phase::Idle};
    bool                                           committed_ = false;
    std::shared_ptr<SourceAdmission::Rotation>     rotation_;
    std::shared_ptr<detail::AllocationReservation> metadata_;
    std::shared_ptr<FlushOperation>                flush_;

    struct Connection
    {
        Connection(FlushReportSender sender, Receiver receiver) : operation(stdexec::connect(sender, receiver))
        {
        }

        Connected operation;
    };

    std::optional<Connection> connected_;

    struct WaitReceiver
    {
        using receiver_concept = stdexec::receiver_tag;
        SourceRotation *owner;
        void            set_value() const noexcept;
        void            set_error(Status status) const noexcept;
        void            set_stopped() const noexcept;
    };

    using WaitConnected = stdexec::connect_result_t<detail::CallbackSender, WaitReceiver>;

    struct CreditWait
    {
        CreditWait(SourceRotation &owner, size_t index)
            : request(owner.account_, detail::FlushReserveBounds::categories[index], owner.plan_.bounds.amounts[index],
                      &owner.registrations_[index], detail::ResourceBudget::Request::WaitPolicy::ReservationsOnly),
              metrics(owner.publisher_.work_metrics(), TreeStage::BudgetWait, tree_metrics_now_ns()),
              connected(stdexec::connect(request.sender(), WaitReceiver{&owner}))
        {
        }

        detail::ResourceBudget::Request request;
        TreeMetricsOperation            metrics;
        WaitConnected                   connected;
    };

    struct ProgressWait
    {
        ProgressWait(SourceRotation &owner, uint64_t observed)
            : request(*owner.progress_, observed),
              connected(stdexec::connect(request.sender(), WaitReceiver{&owner}))
        {
        }

        ApplyProgress::Request request;
        WaitConnected          connected;
    };

    std::optional<CreditWait>           credit_wait_;
    std::optional<ProgressWait>         progress_wait_;
    std::optional<TreeMetricsOperation> admission_metrics_;
};
} // namespace crowdb::tree
