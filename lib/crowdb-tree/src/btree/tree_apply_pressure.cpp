// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_apply_operation_internal.h"
#include "tree_core_checkpoint_internal.h"

namespace crowdb::tree
{
void TreeApplyOperation::State::start_pressure()
{
    auto core = weak_core.lock();
    if (!core || (!core->local_backend() && !core->chunk_backend()) || !replay_input) {
        finish(Status::invalid_argument("chosen replay pressure requires an owned local backend"));
        return;
    }
    const auto view     = core->publisher().capture();
    const auto selected = view->mapping.oldest_writeback_leaf();
    if (!selected) {
        // The maintained candidate index is empty. Retained old readers or
        // completed retirement tails may still own the paid gap payload.
        waiting = Wait::kApplyCredit;
        credit_attempt.emplace(this, metadata->account(), detail::BudgetClass::kGapRepair,
                               replay_input->bound.staging_bytes + replay_input->bound.source_bytes, work);
        stdexec::start(credit_attempt->connected);
        return;
    }
    pressure_pid     = *selected;
    const auto bytes = checkpoint_detail::pressure_control_bound();
    if (!bytes) {
        finish(Status::resource_exhausted("pressure control bound overflow"));
        return;
    }
    waiting = Wait::kPressureCredit;
    credit_attempt.emplace(this, metadata->account(), detail::BudgetClass::kReclaim, *bytes, work);
    stdexec::start(credit_attempt->connected);
}

void TreeApplyOperation::State::pressure_credit_ready()
{
    if (!resumed.ok()) {
        finish(std::move(resumed));
        return;
    }
    if (!credit_attempt) {
        finish(Status::internal_error("pressure control has no grant waiter"));
        return;
    }
    auto credit = credit_attempt->request.take_credit();
    credit_attempt.reset();
    waiting   = Wait::kNone;
    auto core = weak_core.lock();
    if (!credit || !core) {
        finish(Status::internal_error("pressure control lost its accepted owner"));
        return;
    }
    std::shared_ptr<CheckpointOperation> operation;
    auto status = checkpoint_detail::prepare_pressure(*core, backend_ticket, pressure_pid, std::move(*credit),
                                                      dispatcher, preparation.publication, progress, waiters,
                                                      preparation.maintenance, &operation);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    observed = progress->sequence();
    waiting  = Wait::kWriteback;
    writeback_attempt.emplace(this, std::move(operation));
    stdexec::start(writeback_attempt->connected);
}

void TreeApplyOperation::State::writeback_ready()
{
    writeback_attempt.reset();
    waiting = Wait::kNone;
    if (!resumed.ok()) {
        if (resumed.code() == Code::kUnavailable) {
            await_progress(observed);
        }
        else {
            finish(std::move(resumed));
        }
        return;
    }
    if (auto core = weak_core.lock()) {
        core->publisher().reclaim();
    }
    dispatcher->request(node, self);
}
} // namespace crowdb::tree
