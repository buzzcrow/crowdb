// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "btree/tree_core_internal.h"
#include "crowdb-common/log.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
Status TreeCore::prepare_checkpoint(const CheckpointOptions &options, std::shared_ptr<CheckpointOperation> *out)
{
    return prepare_checkpoint(options, out, {});
}

Status TreeCore::prepare_checkpoint(const CheckpointOptions &options, std::shared_ptr<CheckpointOperation> *out,
                                    std::shared_ptr<TreeOperationLifetime::Ticket> accepted)
try {
    State::Ingress admission(*state_);
    if ((!accepted && admission.owner == nullptr) || (!local_backend() && !chunk_backend()) ||
        async_backend() == nullptr) {
        return Status::unavailable("checkpoint requires an open reloadable backend");
    }
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    using namespace detail;
    const auto bound = AllocationBound(CheckpointOperation::control_allocation_bound())
                           .add(shared_allocation_bound<checkpoint_detail::Preparation>())
                           .add_product(2, shared_allocation_bound<checkpoint_detail::Credits>())
                           .add(shared_allocation_bound<AllocationReservation>())
                           .add(FlushPersistence::control_allocation_bound(binding()->cpu()->worker_count()))
                           .add(TreeOperationLifetime::ticket_allocation_bound())
                           .value();
    if (!bound) {
        return Status::resource_exhausted();
    }
    auto control = ResourceBudget::try_reserve(binding()->account(), BudgetClass::kReclaim, *bound);
    auto finish  = ResourceBudget::try_reserve(
        binding()->account(), BudgetClass::kReclaim,
        CheckpointOperation::finish_allocation_bound() + CheckpointOperation::finish_allocation_bound() +
            shared_allocation_bound<SnapshotBaseline>() + shared_allocation_bound<AllocationReservation>());
    if (!control || !finish) {
        return Status::resource_exhausted("checkpoint finish credit unavailable");
    }
    auto credit                           = AllocationReservation::create(std::move(*control));
    auto preparation                      = reserved_shared<checkpoint_detail::Preparation>(credit);
    preparation->control                  = credit;
    preparation->finish                   = AllocationReservation::create(std::move(*finish));
    preparation->core                     = this;
    preparation->options                  = options;
    preparation->structure_options        = state_->preparation.maintenance;
    preparation->options.diagnostics      = &state_->checkpoint_diagnostics;
    preparation->options.handoff_progress = state_->apply_progress;
    preparation->options.handoff_blocked  = &preparation->handoff_blocked;
    preparation->cpu_progress             = state_->apply_dispatcher;
    if (accepted) {
        // A finite child shares its parent's accepted lifetime; no second
        // per-Tree operation slot is consumed.
        preparation->ticket = std::move(accepted);
    }
    else {
        auto status = operation_lifetime().acquire(credit, &preparation->ticket);
        if (!status.ok()) {
            return status;
        }
    }
    std::shared_ptr<FlushPersistence> persistence;
    if (options.mode == CheckpointMode::Durable) {
        persistence = reserved_shared<FlushPersistence>(credit, preparation);
    }
    return CheckpointOperation::create(
        preparation, publisher(), local_backend(), preparation->ticket->backend(async_backend()), binding()->cpu(),
        binding()->lane(), preparation->options, {.bytes = credit, .io = {}, .cpu_progress = state_->apply_dispatcher},
        preparation->finish, {},
        [preparation, persistence, controller = state_->flush.get(),
         initial_options = &state_->preparation](std::shared_ptr<FlushOperation> *handoff) {
            if (persistence) {
                auto flush_options  = *initial_options;
                flush_options.store = preparation->ticket->backend(preparation->core->async_backend());
                persistence->configure(flush_options);
                return controller->prepare(flush_options, handoff, &preparation->handoff_blocked);
            }
            return preparation->core->prepare_flush(handoff, preparation->ticket);
        },
        [preparation](const PublishedTreeState &view, const CapturedWritePlan *plan, CheckpointWork *work) {
            return preparation->work(view, plan, work);
        },
        out, [preparation] { checkpoint_detail::schedule_free_blocks(*preparation->core, preparation->ticket); }, {},
        [progress = state_->apply_progress] { progress->advance(); }, {}, chunk_backend(),
        [preparation](std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                      std::shared_ptr<PageSplitOperation> *operation) {
            return preparation->structure(std::move(view), pid, operation);
        },
        persistence ? CheckpointOperation::AfterFlush{[persistence] { return persistence->result(); }}
                    : CheckpointOperation::AfterFlush{});
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status TreeCore::prepare_writeback(std::shared_ptr<CheckpointOperation> *out)
{
    auto options               = checkpoint_detail::defaults(*this);
    options.mode               = CheckpointMode::Writeback;
    options.cause              = TreeWorkCause::CachePressure;
    options.select_oldest_page = true;
    return prepare_checkpoint(options, out);
}

Status TreeCore::checkpoint(uint64_t *coverage, uint64_t *sequence)
{
    std::shared_ptr<CheckpointOperation> operation;
    auto                                 status = prepare_checkpoint(checkpoint_detail::defaults(*this), &operation);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(operation);
    }
    if (status.ok()) {
        if (coverage != nullptr) {
            *coverage = operation->result().durable_coverage;
        }
        if (sequence != nullptr) {
            *sequence = operation->result().snapshot_sequence;
        }
        operation.reset();
    }
    return status;
}
} // namespace crowdb::tree
