// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_apply_operation_internal.h"

namespace crowdb::tree
{
void TreeApplyOperation::State::Receiver::set_value() const noexcept
{
    if ((owner->waiting == Wait::kCredit || owner->waiting == Wait::kApplyCredit) && owner->credit_attempt) {
        (void)owner->credit_attempt->metrics.finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    owner->wake(Status::Ok());
}

void TreeApplyOperation::State::Receiver::set_value(FlushResult result) const noexcept
{
    owner->wake(result.handoff_status.ok() ? std::move(result.maintenance_status) : std::move(result.handoff_status));
}

void TreeApplyOperation::State::Receiver::set_error(Status status) const noexcept
{
    if ((owner->waiting == Wait::kCredit || owner->waiting == Wait::kApplyCredit) && owner->credit_attempt) {
        (void)owner->credit_attempt->metrics.finish(TreeOutcome::Error, tree_metrics_now_ns());
    }
    else if (owner->waiting == Wait::kFlush) {
        owner->flush_rejected = true;
    }
    owner->wake(std::move(status));
}

void TreeApplyOperation::State::Receiver::set_stopped() const noexcept
{
    if ((owner->waiting == Wait::kCredit || owner->waiting == Wait::kApplyCredit) && owner->credit_attempt) {
        (void)owner->credit_attempt->metrics.finish(TreeOutcome::Cancelled, tree_metrics_now_ns());
    }
    owner->wake(Status::unavailable("accepted apply progress stopped"));
}

void TreeApplyOperation::State::await_progress(uint64_t sequence)
{
    waiting = Wait::kProgress;
    progress_attempt.emplace(this, *progress, sequence);
    stdexec::start(progress_attempt->connected);
}

void TreeApplyOperation::State::await_credit()
{
    waiting = Wait::kCredit;
    credit_attempt.emplace(this, metadata->account(), detail::FlushReserveBounds::categories[next_credit],
                           plan.bounds.amounts[next_credit], work);
    stdexec::start(credit_attempt->connected);
}

void TreeApplyOperation::State::credit_ready()
{
    if (!resumed.ok()) {
        finish(std::move(resumed));
        return;
    }
    if (!credit_attempt) {
        finish(Status::internal_error("apply budget terminal has no registered request"));
        return;
    }
    auto paid = credit_attempt->request.take_credit();
    credit_attempt.reset();
    waiting = Wait::kNone;
    if (!paid) {
        finish(Status::internal_error("completed apply budget request has no grant"));
        return;
    }
    credits[next_credit++] = std::move(*paid);
    if (next_credit != credits.size()) {
        await_credit();
        return;
    }
    std::shared_ptr<FlushOperation> operation;
    auto                            status = flush->prepare_prepaid(preparation, plan, std::move(credits), &operation);
    if (admission_metrics) {
        auto outcome = TreeOutcome::Error;
        if (status.ok()) {
            outcome = TreeOutcome::Success;
        }
        else if (status.code() == Code::kUnavailable) {
            outcome = TreeOutcome::Stale;
        }
        (void)admission_metrics->finish(outcome, tree_metrics_now_ns());
        admission_metrics.reset();
    }
    plan = {};
    if (!status.ok()) {
        if (status.code() == Code::kUnavailable) {
            // A changed immutable plan is a concrete publication transition.
            // Re-enter once through the bounded dispatcher to capture its new bound.
            dispatcher->request(node, self);
        }
        else {
            finish(std::move(status));
        }
        return;
    }
    connect_flush(std::move(operation));
}
} // namespace crowdb::tree
