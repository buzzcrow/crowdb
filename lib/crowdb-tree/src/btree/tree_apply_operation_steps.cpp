// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "tree_apply_operation_internal.h"

#include <cassert>

namespace crowdb::tree
{
void TreeApplyOperation::State::start(uint64_t chosen_slot, detail::CallbackComplete callback, void *argument) noexcept
{
    assert(callback != nullptr);
    Phase expected = Phase::kArmed;
    if (!phase.compare_exchange_strong(expected, Phase::kStarted, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::invalid_argument());
        return;
    }
    if (replay_input && chosen_slot != replay_slot) {
        phase.store(Phase::kArmed, std::memory_order_release);
        callback(argument, detail::CallbackSignal::kError,
                 Status::invalid_argument("replay slot differs from its accepted input"));
        return;
    }
    slot     = chosen_slot;
    complete = callback;
    context  = argument;
    self     = weak_from_this().lock();
    dispatcher->request(node, self);
}

void TreeApplyOperation::State::run(void *context) noexcept
{
    auto &state = *static_cast<State *>(context);
    try {
        state.step();
    }
    catch (const std::bad_alloc &) {
        state.finish(Status::resource_exhausted());
    }
    catch (...) {
        state.finish(Status::internal_error());
    }
}

void TreeApplyOperation::State::wake(Status status) noexcept
{
    resumed = std::move(status);
    dispatcher->request(node, self);
}

void TreeApplyOperation::State::step()
{
    if (waiting == Wait::kPressureCredit) {
        pressure_credit_ready();
        return;
    }
    if (waiting == Wait::kWriteback) {
        writeback_ready();
        return;
    }
    if (waiting == Wait::kApplyCredit) {
        apply_credit_ready();
        return;
    }
    if (waiting == Wait::kCredit) {
        credit_ready();
        return;
    }
    if (waiting == Wait::kFlush) {
        flush_attempt.reset();
        waiting = Wait::kNone;
        if (flush_rejected && resumed.code() == Code::kUnavailable) {
            await_progress(observed);
            return;
        }
        if (!resumed.ok()) {
            finish(std::move(resumed));
            return;
        }
    }
    else if (waiting == Wait::kProgress) {
        progress_attempt.reset();
        waiting = Wait::kNone;
        if (!resumed.ok()) {
            finish(std::move(resumed));
            return;
        }
    }
    if (sources->catalog()->generation != generation) {
        finish(Status::internal_error("armed apply lost its pinned generation"));
        return;
    }
    observed = progress->sequence();
    if (!frontier->check_slot(slot).ok()) {
        await_progress(observed);
        return;
    }
    if (replay_input && !prepare_replay_batch()) {
        return;
    }
    auto status = batch->try_commit(*sources, *frontier, slot);
    if (status.ok()) {
        progress->advance();
        (void)source_rotation->request();
        finish(Status::Ok());
        return;
    }
    if (status.code() != Code::kUnavailable || batch->consumed()) {
        finish(std::move(status));
        return;
    }
    if (frontier->contiguous().load(std::memory_order_acquire) <= sources->catalog()->floor) {
        await_progress(observed);
        return;
    }
    start_flush();
}

void TreeApplyOperation::State::start_flush()
{
    plan = {};
    std::shared_ptr<FlushOperation> operation;
    auto                            status = flush->prepare_or_plan(preparation, &operation, &plan);
    if (!status.ok()) {
        if (status.code() == Code::kUnavailable) {
            await_progress(observed);
        }
        else {
            finish(std::move(status));
        }
        return;
    }
    if (operation) {
        connect_flush(std::move(operation));
        return;
    }
    next_credit = 0;
    admission_metrics.emplace(work, TreeStage::FlushAdmissionWait, tree_metrics_now_ns());
    await_credit();
}

void TreeApplyOperation::State::connect_flush(std::shared_ptr<FlushOperation> operation)
{
    // A concurrent connection may own this repair. Observe after the planning
    // gate released so our own planning wakeup cannot become a retry loop.
    observed       = progress->sequence();
    flush_rejected = false;
    waiting        = Wait::kFlush;
    flush_attempt.emplace(this, std::move(operation));
    stdexec::start(flush_attempt->connected);
}

void TreeApplyOperation::State::finish(Status status) noexcept
{
    if (phase.exchange(Phase::kDone, std::memory_order_acq_rel) == Phase::kDone) {
        return;
    }
    auto retained = std::move(self);
    if (admission_metrics) {
        (void)admission_metrics->finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
    }
    progress_attempt.reset();
    credit_attempt.reset();
    flush_attempt.reset();
    writeback_attempt.reset();
    credits = {};
    plan    = {};
    batch.reset();
    replay_input.reset();
    if (owns_replay_waiters) {
        owns_replay_waiters = false;
        waiters->release();
        progress->advance();
    }
    waiters.reset();
    preparation.store.reset();
    backend_ticket.reset();
    apply_ticket.reset();
    try {
        complete(context, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                 std::move(status));
    }
    catch (...) {
        CRB_LOG_ERROR("Tree accepted apply receiver threw after terminal");
    }
}
} // namespace crowdb::tree
