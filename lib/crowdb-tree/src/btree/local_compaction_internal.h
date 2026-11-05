// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/local/local_sparse_gc.h"
#include "btree/local_compaction_operation.h"
#include "btree/tree_core_checkpoint_internal.h"

namespace crowdb::tree
{
struct LocalCompactionOperation::State : std::enable_shared_from_this<State>
{
    enum class Phase : uint8_t { kBegin, kSelected, kCheckpoint, kCleaned, kDone };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;
        void                   set_value() noexcept;
        void                   set_error(Status status) noexcept;
        void                   set_stopped() noexcept;
    };

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{std::move(owner)}))
        {
        }

        stdexec::connect_result_t<detail::CallbackSender, Receiver> connected;
    };

    ~State();
    void        start(detail::CallbackComplete callback, void *argument) noexcept;
    static void cpu_run(void *context) noexcept;
    static void backend_run(void *context) noexcept;
    void        pump();
    void        backend_step();
    void        checkpoint();
    void        child_done(Status status) noexcept;
    void        finish(Status status) noexcept;
    void        wake() noexcept;

    std::shared_ptr<detail::AllocationReservation>         credit;
    std::shared_ptr<TreeOperationLifetime::Ticket>         ticket;
    std::shared_ptr<AllocationBudget>                      backend;
    TreeCore                                              *core  = nullptr;
    BlockPageStore                                        *store = nullptr;
    std::shared_ptr<LocalBackendState>                     local;
    ApplyDispatcher::Node                                  cpu_node{&State::cpu_run, this};
    ApplyDispatcher::Node                                  backend_node{&State::backend_run, this};
    std::shared_ptr<ApplyDispatcher>                       cpu_progress;
    std::shared_ptr<ApplyDispatcher>                       backend_progress;
    std::shared_ptr<CheckpointOperation>                   child;
    std::optional<Attempt>                                 attempt;
    LocalSparseBlock                                       candidate;
    MergeGcStats                                           report;
    detail::InlineFunction<void(const MergeGcStats &), 32> observer;
    LocalCompactionOptions                                 options;
    Status                                                 child_status;
    std::atomic<bool>                                      child_ready{false};
    std::atomic<bool>                                      stopped{false};
    std::atomic<bool>                                      started{false};
    std::atomic<bool>                                      callback_ready{false};
    Phase                                                  phase     = Phase::kBegin;
    size_t                                                 rotations = 0;
    bool                                                   gate_held = false;
    detail::CallbackComplete                               complete  = nullptr;
    void                                                  *context   = nullptr;
};
} // namespace crowdb::tree
