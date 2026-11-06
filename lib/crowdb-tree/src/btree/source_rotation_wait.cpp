// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "source_rotation.h"

namespace crowdb::tree
{
void SourceRotation::set_async_flush(FlushController &controller, PrepareOptions prepare)
{
    flush_controller_ = &controller;
    prepare_options_  = std::move(prepare);
}

void SourceRotation::plan_flush()
{
    if (stopping_.load(std::memory_order_acquire)) {
        finish();
        return;
    }
    plan_    = {};
    credits_ = {};
    if (!preparation_.store) {
        auto status = prepare_options_(&preparation_);
        if (!status.ok()) {
            CRB_LOG_ERROR("automatic flush preparation failed; ordinary writes remain paused: {}", status.to_string());
            finish();
            return;
        }
    }
    observed_   = progress_->sequence();
    auto status = flush_controller_->prepare_or_plan(preparation_, &flush_, &plan_);
    if (!status.ok()) {
        if (status.code() == Code::kUnavailable) {
            await_progress(observed_);
        }
        else {
            finish();
        }
        return;
    }
    if (flush_) {
        start_flush();
        return;
    }
    admission_metrics_.emplace(publisher_.work_metrics(), TreeStage::FlushAdmissionWait, tree_metrics_now_ns());
    next_credit_ = 0;
    await_credit();
}

void SourceRotation::await_credit()
{
    phase_.store(Phase::CreditWaiting, std::memory_order_release);
    credit_wait_.emplace(*this, next_credit_);
    stdexec::start(credit_wait_->connected);
}

void SourceRotation::credit_ready()
{
    if (!resumed_.ok() || stopping_.load(std::memory_order_acquire)) {
        finish();
        return;
    }
    if (!credit_wait_) {
        std::terminate();
    }
    auto credit = credit_wait_->request.take_credit();
    credit_wait_.reset();
    if (!credit) {
        finish();
        return;
    }
    credits_[next_credit_++] = std::move(*credit);
    if (next_credit_ != credits_.size()) {
        await_credit();
        return;
    }
    auto status = flush_controller_->prepare_prepaid(preparation_, plan_, std::move(credits_), &flush_);
    if (admission_metrics_) {
        auto outcome = TreeOutcome::Error;
        if (status.ok()) {
            outcome = TreeOutcome::Success;
        }
        else if (status.code() == Code::kUnavailable) {
            outcome = TreeOutcome::Stale;
        }
        (void)admission_metrics_->finish(outcome, tree_metrics_now_ns());
        admission_metrics_.reset();
    }
    plan_ = {};
    if (!status.ok()) {
        if (status.code() == Code::kUnavailable) {
            // The captured immutable plan changed. Replan once through the
            // dispatcher; no source was closed by a rejected binding.
            preparation_ = {};
            phase_.store(Phase::FlushPlanning, std::memory_order_release);
            schedule();
        }
        else {
            finish();
        }
        return;
    }
    start_flush();
}

void SourceRotation::await_progress(uint64_t observed)
{
    phase_.store(Phase::ProgressWaiting, std::memory_order_release);
    progress_wait_.emplace(*this, observed);
    stdexec::start(progress_wait_->connected);
}

bool SourceRotation::wait_step(Phase phase)
{
    if (phase == Phase::CreditWaiting || phase == Phase::ProgressWaiting) {
        if (stopping_.load(std::memory_order_acquire)) {
            if (phase == Phase::CreditWaiting) {
                if (!credit_wait_) {
                    std::terminate();
                }
                credit_wait_->request.request_stop();
            }
            else {
                if (!progress_wait_) {
                    std::terminate();
                }
                progress_wait_->request.request_stop();
            }
        }
        return true;
    }
    if (phase == Phase::CreditReady) {
        credit_ready();
        return true;
    }
    if (phase == Phase::ProgressReady) {
        progress_wait_.reset();
        if (!resumed_.ok()) {
            finish();
        }
        else {
            plan_flush();
        }
        return true;
    }
    if (phase == Phase::FlushPlanning) {
        plan_flush();
        return true;
    }
    return false;
}

void SourceRotation::WaitReceiver::set_value() const noexcept
{
    if (owner->phase_.load(std::memory_order_relaxed) == Phase::CreditWaiting) {
        if (!owner->credit_wait_) {
            std::terminate();
        }
        (void)owner->credit_wait_->metrics.finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    owner->resumed_  = Status::Ok();
    const auto ready = owner->phase_.load(std::memory_order_relaxed) == Phase::CreditWaiting ? Phase::CreditReady
                                                                                             : Phase::ProgressReady;
    owner->phase_.store(ready, std::memory_order_release);
    owner->schedule();
}

void SourceRotation::WaitReceiver::set_error(Status status) const noexcept
{
    if (owner->phase_.load(std::memory_order_relaxed) == Phase::CreditWaiting) {
        if (!owner->credit_wait_) {
            std::terminate();
        }
        const auto outcome =
            owner->stopping_.load(std::memory_order_acquire) ? TreeOutcome::Cancelled : TreeOutcome::Error;
        (void)owner->credit_wait_->metrics.finish(outcome, tree_metrics_now_ns());
    }
    owner->resumed_  = std::move(status);
    const auto ready = owner->phase_.load(std::memory_order_relaxed) == Phase::CreditWaiting ? Phase::CreditReady
                                                                                             : Phase::ProgressReady;
    owner->phase_.store(ready, std::memory_order_release);
    owner->schedule();
}

void SourceRotation::WaitReceiver::set_stopped() const noexcept
{
    set_error(Status::unavailable("soft source maintenance stopped"));
}
} // namespace crowdb::tree
