// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "tree_apply_operation_internal.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
TreeApplyOperation::State::State(std::weak_ptr<TreeCore> core, uint64_t generation,
                                 std::shared_ptr<AdmittedBatch>                 batch,
                                 std::shared_ptr<detail::AllocationReservation> metadata)
    : weak_core(std::move(core)),
      generation(generation),
      batch(std::move(batch)),
      metadata(std::move(metadata)),
      node(&State::run, this)
{
}

std::optional<size_t> TreeApplyOperation::allocation_bound(size_t retained_metadata_bytes) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<State>())
        .add(detail::shared_allocation_bound<TreeApplyOperation>())
        .add(detail::shared_allocation_bound<ApplyWaiters>())
        .add(detail::shared_allocation_bound<detail::AllocationReservation>())
        .add_product(2, TreeOperationLifetime::ticket_allocation_bound())
        .add(retained_metadata_bytes)
        .value();
}

Status TreeApplyOperation::bind_prepaid(std::shared_ptr<TreeCore> core, std::shared_ptr<AdmittedBatch> batch,
                                        detail::ResourceBudget::Credit credit, std::shared_ptr<TreeApplyOperation> *out)
{
    auto metadata = detail::AllocationReservation::create(std::move(credit));
    auto state =
        detail::reserved_shared<State>(metadata, core, core->capture()->generation, std::move(batch), metadata);
    auto status = ApplyWaiters::create(core->binding()->account(), metadata, &state->waiters);
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<TreeApplyOperation>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) TreeApplyOperation(std::move(state)); });
    return Status::Ok();
}

Status TreeApplyOperation::State::arm()
{
    Phase expected = Phase::kPrepared;
    if (!phase.compare_exchange_strong(expected, Phase::kArming, std::memory_order_acq_rel)) {
        return Status::invalid_argument("apply permit must be armed exactly once");
    }

    struct Rollback
    {
        State &state;

        ~Rollback()
        {
            if (state.phase.load(std::memory_order_acquire) == Phase::kArming) {
                state.apply_ticket.reset();
                state.backend_ticket.reset();
                state.phase.store(Phase::kPrepared, std::memory_order_release);
            }
        }
    } rollback{*this};

    auto core = weak_core.lock();
    if (!core) {
        return Status::unavailable("apply Tree already closed");
    }
    TreeCore::State::Ingress ingress(*core->state_);
    if (ingress.owner == nullptr || core->capture()->generation != generation) {
        return Status::unavailable("apply permit generation closed or replaced");
    }
    auto &state  = *core->state_;
    auto  status = Status::Ok();
    if (replay_control) {
        status         = state.replay_lifetime->acquire(metadata, &apply_ticket);
        backend_ticket = apply_ticket;
    }
    else {
        status = state.apply_lifetime->try_acquire(metadata, &apply_ticket);
        if (status.ok()) {
            status = state.lifetime->try_acquire(metadata, &backend_ticket);
        }
    }
    if (!status.ok()) {
        return status;
    }
    sources           = state.sources.get();
    frontier          = state.frontier.get();
    flush             = state.flush.get();
    dispatcher        = state.apply_dispatcher;
    progress          = state.apply_progress;
    source_rotation   = state.source_rotation;
    work              = state.work;
    preparation       = state.preparation;
    preparation.store = backend_ticket->backend(core->async_backend());
    if (!replay_control) {
        metadata->seal();
    }
    phase.store(Phase::kArmed, std::memory_order_release);
    return Status::Ok();
}

TreeApplyOperation::~TreeApplyOperation() = default;

Status TreeApplyOperation::arm()
try {
    return state_->arm();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply arm allocation failed");
}

void TreeApplyOperation::start(uint64_t slot, detail::CallbackComplete complete, void *context) noexcept
{
    auto state = state_;
    state->start(slot, complete, context);
}

const std::shared_ptr<detail::AllocationReservation> &TreeApplyOperation::allocation_budget() const noexcept
{
    return state_->metadata;
}
} // namespace crowdb::tree
