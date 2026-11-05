// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "tree_core_checkpoint_internal.h"

namespace crowdb::tree::checkpoint_detail
{
AsyncWork::AsyncWork(std::shared_ptr<Preparation> preparation) : preparation_(std::move(preparation))
{
}

AsyncWork::Attempt::Attempt(AsyncWork *owner, detail::BudgetClass category, size_t bytes)
    : request(owner->preparation_->core->binding()->account(), category, bytes,
              owner->preparation_->waiters ? owner->preparation_->waiters->get(category) : nullptr),
      metrics(owner->preparation_->core->publisher().work_metrics(), TreeStage::BudgetWait, tree_metrics_now_ns()),
      connected(stdexec::connect(request.sender(), Receiver{owner}))
{
}

void AsyncWork::Receiver::set_value() const noexcept
{
    owner->wake(Status::Ok());
}

void AsyncWork::Receiver::set_error(Status status) const noexcept
{
    owner->wake(std::move(status));
}

void AsyncWork::Receiver::set_stopped() const noexcept
{
    owner->wake(Status::unavailable("checkpoint grant cancelled"));
}

detail::CallbackSender AsyncWork::prepare(const PublishedTreeState &view, const CapturedWritePlan *plan,
                                          CheckpointWork *out)
{
    out_    = out;
    phase_  = Phase::kRouting;
    status_ = preparation_->plan_work(view, plan, &plan_);
    return {this, &AsyncWork::submit};
}

void AsyncWork::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto &self     = *static_cast<AsyncWork *>(source);
    self.complete_ = complete;
    self.context_  = context;
    if (!self.status_.ok()) {
        self.finish(std::move(self.status_));
        return;
    }
    self.attempt_.emplace(&self, detail::BudgetClass::kRouting, self.plan_.backend_bytes);
    if (self.stopped_.load(std::memory_order_acquire)) {
        self.attempt_->request.request_stop();
    }
    stdexec::start(self.attempt_->connected);
}

void AsyncWork::cancel() noexcept
{
    stopped_.store(true, std::memory_order_release);
    if (attempt_) {
        attempt_->request.request_stop();
    }
}

void AsyncWork::wake(Status status) noexcept
{
    status_ = std::move(status);
    preparation_->cpu_progress->request(node_, weak_from_this().lock());
}

void AsyncWork::run(void *context) noexcept
{
    auto &self = *static_cast<AsyncWork *>(context);
    try {
        self.step();
    }
    catch (const std::bad_alloc &) {
        self.finish(Status::resource_exhausted());
    }
    catch (...) {
        self.finish(Status::internal_error());
    }
}

void AsyncWork::step()
{
    if (!attempt_) {
        finish(Status::internal_error("checkpoint grant lost its request"));
        return;
    }
    auto outcome = status_.ok() ? TreeOutcome::Success : TreeOutcome::Error;
    if (stopped_.load(std::memory_order_acquire)) {
        outcome = TreeOutcome::Cancelled;
    }
    (void)attempt_->metrics.finish(outcome, tree_metrics_now_ns());
    if (!status_.ok() || stopped_.load(std::memory_order_acquire)) {
        finish(status_.ok() ? Status::unavailable("checkpoint grant cancelled") : std::move(status_));
        return;
    }
    auto credit = attempt_->request.take_credit();
    attempt_.reset();
    if (!credit) {
        finish(Status::internal_error("checkpoint grant completed without credit"));
        return;
    }
    if (phase_ == Phase::kRouting) {
        backend_ = std::move(*credit);
        phase_   = Phase::kReclaim;
        attempt_.emplace(this, detail::BudgetClass::kReclaim, plan_.bytes);
        stdexec::start(attempt_->connected);
        return;
    }
    if (phase_ == Phase::kReclaim) {
        bytes_ = std::move(*credit);
        phase_ = Phase::kIo;
        attempt_.emplace(this, detail::BudgetClass::kIo, 1);
        stdexec::start(attempt_->connected);
        return;
    }
    finish(preparation_->bind_work(plan_, std::move(bytes_), std::move(backend_), std::move(*credit), out_));
}

void AsyncWork::finish(Status status) noexcept
{
    attempt_.reset();
    bytes_   = {};
    backend_ = {};
    out_     = nullptr;
    complete_(context_, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
              std::move(status));
}
} // namespace crowdb::tree::checkpoint_detail
