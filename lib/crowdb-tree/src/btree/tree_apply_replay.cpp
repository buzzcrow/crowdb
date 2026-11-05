// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "tree_apply_operation_internal.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
Status TreeApplyOperation::bind_replay(std::shared_ptr<TreeCore> core, uint64_t slot, size_t count,
                                       CoreWriteReader read, TreeCore::ReplayOwnerFactory retain,
                                       size_t retained_metadata_bytes, std::shared_ptr<TreeApplyOperation> *out)
{
    if (out == nullptr || *out || (count != 0 && !retain) || retained_metadata_bytes > kReplayCallerControlBytes) {
        return Status::invalid_argument("replay needs an immutable caller owner and bounded controls");
    }
    const auto          sources = core->state_->sources->catalog();
    const auto          limits  = sources->active_budget->options();
    AdmittedBatch::Plan plan;
    auto status = AdmittedBatch::plan(core->config(), limits, core->load_limits().max_value_bytes, count, read, &plan);
    if (!status.ok()) {
        return status;
    }
    const auto complete = detail::AllocationBound(plan.staging_bytes)
                              .add(plan.source_bytes)
                              .add(allocation_bound(retained_metadata_bytes))
                              .value();
    if (!complete || *complete > core->binding()->options().limits.gap_repair_bytes ||
        plan.footprint.entries > limits.max_gap_entries || plan.footprint.key_bytes > limits.max_gap_key_bytes) {
        return Status::invalid_argument("chosen batch exceeds the configured replay progress bound");
    }
    const auto completed   = core->contiguous_slot();
    const bool current_gap = slot <= completed || slot - completed == 1;
    auto       controls    = core->state_->replay_controls->acquire(current_gap);
    if (!controls) {
        return Status::unavailable("caller exceeded the bounded replay registration window");
    }
    auto metadata     = controls.budget();
    auto caller_owner = retain ? retain(metadata) : std::shared_ptr<const void>{};
    if (count != 0 && !caller_owner) {
        return Status::invalid_argument("replay input owner is absent");
    }
    auto state =
        detail::reserved_shared<State>(metadata, core, sources->generation, std::shared_ptr<AdmittedBatch>{}, metadata);
    state->waiters        = core->state_->replay_waiters;
    state->replay_control = std::move(controls);
    state->replay_slot    = slot;
    state->replay_input.emplace(State::ReplayInput{.count        = count,
                                                   .read         = std::move(read),
                                                   .caller_owner = std::move(caller_owner),
                                                   .options      = limits,
                                                   .bound        = plan});
    auto result = detail::reserved_construct<TreeApplyOperation>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) TreeApplyOperation(state); });
    status = result->arm();
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}

Status TreeCore::prepare_replay(uint64_t slot, size_t count, detail::InlineFunction<CoreWriteInput(size_t)> read,
                                std::shared_ptr<const void> caller_owner, size_t retained_metadata_bytes,
                                std::shared_ptr<TreeApplyOperation> *out)
{
    return prepare_replay_adopt(
        slot, count, std::move(read),
        [owner = std::move(caller_owner)](const std::shared_ptr<detail::AllocationReservation> &) { return owner; },
        retained_metadata_bytes, out);
}

Status TreeCore::prepare_replay_adopt(uint64_t slot, size_t count, CoreWriteReader read, ReplayOwnerFactory retain,
                                      size_t retained_metadata_bytes, std::shared_ptr<TreeApplyOperation> *out)
try {
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("replay ingress closed or replacing");
    }
    return TreeApplyOperation::bind_replay(shared_from_this(), slot, count, std::move(read), std::move(retain),
                                           retained_metadata_bytes, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("prepaid replay control allocation failed");
}

bool TreeApplyOperation::State::prepare_replay_batch()
{
    const auto completed = frontier->contiguous().load(std::memory_order_acquire);
    if (slot <= completed) {
        finish(Status::Ok());
        return false;
    }
    if (slot - completed != 1) {
        await_progress(observed);
        return false;
    }
    if (!owns_replay_waiters) {
        if (!waiters->try_acquire()) {
            await_progress(observed);
            return false;
        }
        owns_replay_waiters = true;
    }
    if (!replay_input) {
        finish(Status::internal_error("replay continuation lost its caller input"));
        return false;
    }
    const auto amount = replay_input->bound.staging_bytes + replay_input->bound.source_bytes;
    auto grant = detail::ResourceBudget::try_reserve(metadata->account(), detail::BudgetClass::kGapRepair, amount);
    if (grant) {
        bind_replay_batch(std::move(*grant));
        return static_cast<bool>(batch);
    }
    if (completed > sources->catalog()->floor && replay_flushed != completed) {
        replay_flushed = completed;
        start_flush();
        return false;
    }
    start_pressure();
    return false;
}

void TreeApplyOperation::State::apply_credit_ready()
{
    if (!resumed.ok()) {
        finish(std::move(resumed));
        return;
    }
    if (!credit_attempt) {
        finish(Status::internal_error("replay grant has no waiter"));
        return;
    }
    auto grant = credit_attempt->request.take_credit();
    credit_attempt.reset();
    waiting = Wait::kNone;
    if (!grant) {
        finish(Status::internal_error("replay waiter completed without its grant"));
        return;
    }
    bind_replay_batch(std::move(*grant));
    if (batch) {
        dispatcher->request(node, self);
    }
}

void TreeApplyOperation::State::bind_replay_batch(detail::ResourceBudget::Credit grant)
{
    if (!replay_input) {
        finish(Status::internal_error("replay grant has no input plan"));
        return;
    }
    const auto &input   = *replay_input;
    auto        staging = grant.split(input.bound.staging_bytes);
    auto        source  = input.bound.source_bytes != 0 ? grant.split(input.bound.source_bytes) : std::nullopt;
    if (!staging || (input.bound.source_bytes != 0 && !source)) {
        finish(Status::internal_error("replay complete grant split failed"));
        return;
    }
    auto status = AdmittedBatch::bind_prepaid(input.options, input.count, input.read, detail::BudgetClass::kGapRepair,
                                              input.bound, std::move(*staging),
                                              source ? std::move(*source) : detail::ResourceBudget::Credit{}, &batch);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    replay_input.reset();
}
} // namespace crowdb::tree
