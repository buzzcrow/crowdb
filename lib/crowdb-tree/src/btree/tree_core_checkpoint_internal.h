// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/block_account.h"
#include "btree/apply_dispatcher.h"
#include "btree/apply_progress.h"
#include "btree/apply_waiters.h"
#include "btree/tree_core.h"
#include "snapshot/checkpoint_operation.h"

namespace crowdb::tree::checkpoint_detail
{
struct Credits
{
    PageLoadBudget                                 budget;
    std::shared_ptr<detail::AllocationReservation> snapshot;
    bool                                           owns_io = true;
    ~Credits();
};

struct WorkPlan
{
    size_t                         bytes         = 0;
    size_t                         backend_bytes = 0;
    PageLoadLimits                 loads;
    detail::BackendOperationLimits backend;
};

struct Preparation
{
    bool                                           handoff_blocked = false;
    TreeCore                                      *core            = nullptr;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<detail::AllocationReservation> control;
    std::shared_ptr<detail::AllocationReservation> finish;
    CheckpointOptions                              options;
    PageMaintenanceOperationOptions                structure_options;
    Status                           structure(std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                                               std::shared_ptr<PageSplitOperation> *out);
    std::shared_ptr<ApplyDispatcher> cpu_progress;
    std::shared_ptr<ApplyWaiters>    waiters;
    ~Preparation();
    Status plan_work(const PublishedTreeState &view, const CapturedWritePlan *plan, WorkPlan *out,
                     const CapturedWriteLimits *write_limits = nullptr) const;
    Status bind_work(const WorkPlan &plan, detail::ResourceBudget::Credit bytes, detail::ResourceBudget::Credit backend,
                     detail::ResourceBudget::Credit io, CheckpointWork *out,
                     std::shared_ptr<detail::AllocationReservation> shared_io = {});
    Status work(const PublishedTreeState &view, const CapturedWritePlan *plan, CheckpointWork *out,
                size_t extra_bytes = 0, std::shared_ptr<detail::AllocationReservation> shared_io = {},
                const CapturedWriteLimits *write_limits = nullptr);
};

// One phase at a time; the checkpoint owns its captured input and retains this
// control until the grant callback and its final CPU continuation have drained.
class AsyncWork : public std::enable_shared_from_this<AsyncWork>
{
  public:
    explicit AsyncWork(std::shared_ptr<Preparation> preparation);
    [[nodiscard]] detail::CallbackSender prepare(const PublishedTreeState &view, const CapturedWritePlan *plan,
                                                 CheckpointWork *out);
    void                                 cancel() noexcept;

  private:
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        AsyncWork *owner;
        void       set_value() const noexcept;
        void       set_error(Status status) const noexcept;
        void       set_stopped() const noexcept;
    };

    struct Attempt
    {
        Attempt(AsyncWork *owner, detail::BudgetClass category, size_t bytes);
        detail::ResourceBudget::Request                             request;
        TreeMetricsOperation                                        metrics;
        stdexec::connect_result_t<detail::CallbackSender, Receiver> connected;
    };

    static void                    submit(void *source, detail::CallbackComplete complete, void *context);
    static void                    run(void *context) noexcept;
    void                           wake(Status status) noexcept;
    void                           step();
    void                           finish(Status status) noexcept;
    std::shared_ptr<Preparation>   preparation_;
    ApplyDispatcher::Node          node_{&AsyncWork::run, this};
    WorkPlan                       plan_;
    CheckpointWork                *out_ = nullptr;
    std::optional<Attempt>         attempt_;
    detail::ResourceBudget::Credit bytes_;
    std::atomic<bool>              stopped_{false};
    enum class Phase : uint8_t { kRouting, kReclaim, kIo };
    Phase                          phase_ = Phase::kRouting;
    detail::ResourceBudget::Credit backend_;
    Status                         status_;
    detail::CallbackComplete       complete_ = nullptr;
    void                          *context_  = nullptr;
};

CheckpointOptions defaults(const TreeCore &core);
void              reclaim_free_blocks(TreeCore &core, std::shared_ptr<detail::AllocationReservation> metadata = {},
                                      std::shared_ptr<AllocationBudget> backend = {}) noexcept;
void              schedule_free_blocks(TreeCore &core, std::shared_ptr<TreeOperationLifetime::Ticket> ticket) noexcept;
} // namespace crowdb::tree::checkpoint_detail
