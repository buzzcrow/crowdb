// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_rotation.h"

#include <algorithm>
#include <cassert>

namespace crowdb::tree
{
SourceRotation::SourceRotation(SourceAdmission &sources, PublicationCoordinator &publisher,
                               std::shared_ptr<ApplyDispatcher> dispatcher, std::shared_ptr<ApplyProgress> progress,
                               std::shared_ptr<detail::ResourceBudget::Account> account, SourceRotationOptions options)
    : sources_(&sources),
      publisher_(publisher),
      dispatcher_(std::move(dispatcher)),
      progress_(std::move(progress)),
      account_(std::move(account)),
      options_(options),
      node_(&SourceRotation::run, this)
{
    for (size_t index = 0; index < registrations_.size(); ++index) {
        auto registration =
            detail::ResourceBudget::try_register(account_, detail::FlushReserveBounds::categories[index]);
        if (!registration) {
            throw std::bad_alloc();
        }
        registrations_[index] = std::move(*registration);
    }
}

void SourceRotation::set_flush(PrepareFlush prepare, CheckRotation check)
{
    prepare_flush_  = std::move(prepare);
    check_rotation_ = std::move(check);
}

void SourceRotation::paused(void *context) noexcept
{
    auto &self = *static_cast<SourceRotation *>(context);
    self.phase_.store(Phase::Paused, std::memory_order_release);
    self.schedule();
}

void SourceRotation::resume() noexcept
{
    if (paused_catalog_) {
        // Permanently closed sources keep their exact admitted bound. A failed
        // rotation reopens ordinary admission without changing membership.
        if (!paused_catalog_->active->closed()) {
            paused_catalog_->active_budget->resume();
        }
        paused_catalog_.reset();
        paused_owner_.reset();
        progress_->advance();
    }
}

bool SourceRotation::eligible(const SourceCatalog &catalog) const noexcept
{
    return catalog.active->count() != 0 &&
           (catalog.active->count() >= options_.entries || catalog.active->approx_bytes() >= options_.bytes ||
            (catalog.active_budget && catalog.active_budget->full()));
}

uint64_t SourceRotation::request() noexcept
try {
    if (!eligible(*sources_->catalog())) {
        return 0;
    }
    const auto sequence = requested_.fetch_add(1, std::memory_order_acq_rel) + 1;
    schedule();
    return sequence;
}
catch (...) {
    return 0;
}

void SourceRotation::wait(uint64_t sequence) noexcept
{
    if (detail::FlushRuntime::on_worker_thread()) {
        return;
    }
    auto value = completed_.load(std::memory_order_acquire);
    while (value < sequence) {
        completed_.wait(value, std::memory_order_acquire);
        value = completed_.load(std::memory_order_acquire);
    }
}

void SourceRotation::stop() noexcept
{
    stopping_.store(true, std::memory_order_release);
    schedule();
}

void SourceRotation::restart() noexcept
{
    stopping_.store(false, std::memory_order_release);
}

void SourceRotation::drain() noexcept
{
    stop();
    wait(requested_.load(std::memory_order_acquire));
}

void SourceRotation::rebind(SourceAdmission &sources, FlushController &controller) noexcept
{
    assert(completed_.load() == requested_.load());
    sources_          = &sources;
    flush_controller_ = &controller;
    stopping_.store(false, std::memory_order_release);
}

void SourceRotation::schedule() noexcept
{
    auto self = weak_from_this().lock();
    if (!self) {
        std::terminate();
    }
    dispatcher_->request(node_, std::move(self));
}

void SourceRotation::run(void *context) noexcept
{
    auto &self = *static_cast<SourceRotation *>(context);
    try {
        self.step();
    }
    catch (...) {
        self.committed_ = false;
        self.phase_.store(Phase::Acknowledge, std::memory_order_release);
        self.schedule();
    }
}

void SourceRotation::step()
{
    const auto phase = phase_.load(std::memory_order_acquire);
    if (wait_step(phase)) {
        return;
    }
    if (phase == Phase::Publishing || phase == Phase::Flushing || phase == Phase::Pausing) {
        return;
    }
    if (phase == Phase::Paused) {
        const auto catalog = paused_catalog_;
        const auto view    = publisher_.capture();
        if (sources_->catalog() != catalog || !view->sources || view->sources->inheritance) {
            finish();
            return;
        }
        auto status =
            check_rotation_ ? check_rotation_(*view, *catalog, catalog->active_budget->paused_bound()) : Status::Ok();
        if (status.ok()) {
            prepare(catalog);
        }
        else {
            resume();
            if (status.code() == Code::kResourceExhausted) {
                flush();
            }
            else {
                finish();
            }
        }
        return;
    }
    if (phase == Phase::FlushReady) {
        connected_.reset();
        flush_.reset();
        if (flush_retry_ != FlushRetry::None && flush_controller_ != nullptr &&
            !stopping_.load(std::memory_order_acquire)) {
            if (flush_retry_ == FlushRetry::Replan) {
                phase_.store(Phase::FlushPlanning, std::memory_order_release);
                schedule();
            }
            else {
                await_progress(observed_);
            }
        }
        else {
            finish();
        }
        return;
    }
    if (phase == Phase::Acknowledge) {
        auto status = Status::Ok();
        if (rotation_) {
            status = committed_ ? sources_->activate_rotation(rotation_) : sources_->abort_rotation(rotation_);
        }
        if (status.code() == Code::kUnavailable) {
            schedule();
            return;
        }
        assert(status.ok());
        finish();
        return;
    }
    target_ = requested_.load(std::memory_order_acquire);
    if (target_ == completed_.load(std::memory_order_acquire)) {
        return;
    }
    if (stopping_.load(std::memory_order_acquire)) {
        finish();
        return;
    }
    const auto catalog = sources_->catalog();
    const auto view    = publisher_.capture();
    // A captured prefix installs one successor and may be inherited by a
    // child with its own active table. Keep both slots before rotating.
    const auto source_limit = options_.source_window > 2 ? options_.source_window - 2 : 0;
    const bool window_full  = view->sources && view->sources->tables.size() >= source_limit;
    if (!eligible(*catalog) || (view->sources && view->sources->inheritance)) {
        // Inherited catalogs have their own fixed-frontier handoff. They must
        // publish coverage before ordinary source maintenance can run.
        finish();
    }
    else if (window_full || catalog->tables.size() >= std::max<size_t>(2, options_.tables) ||
             (catalog->active_budget && catalog->active_budget->full())) {
        flush();
    }
    else if (catalog->active_budget && check_rotation_) {
        paused_catalog_ = catalog;
        paused_owner_   = shared_from_this();
        phase_.store(Phase::Pausing, std::memory_order_release);
        if (!catalog->active_budget->pause(&SourceRotation::paused, this)) {
            paused_catalog_.reset();
            paused_owner_.reset();
            finish();
        }
    }
    else {
        prepare(catalog);
    }
}

void SourceRotation::flush()
{
    if (flush_controller_ != nullptr) {
        plan_flush();
        return;
    }
    if (!prepare_flush_ || !prepare_flush_(&flush_).ok()) {
        finish();
        return;
    }
    start_flush();
}

void SourceRotation::start_flush()
{
    observed_    = progress_->sequence();
    resumed_     = Status::Ok();
    flush_retry_ = FlushRetry::None;
    connected_.emplace(flush_->report_sender(), Receiver{this});
    phase_.store(Phase::Flushing, std::memory_order_release);
    stdexec::start(connected_->operation);
}

void SourceRotation::Receiver::set_value(FlushResult report) const noexcept
{
    owner->resumed_ =
        report.handoff_status.ok() ? std::move(report.maintenance_status) : std::move(report.handoff_status);
    // Repair and maintenance failures retain their controller-owned batch for
    // a later request. Its own terminal progress notification is not a retry.
    owner->flush_retry_ = report.admission_stale ? FlushRetry::Replan : FlushRetry::None;
    owner->flush_done();
}

void SourceRotation::Receiver::set_error(Status status) const noexcept
{
    owner->resumed_     = std::move(status);
    owner->flush_retry_ = owner->resumed_.code() == Code::kUnavailable ? FlushRetry::Progress : FlushRetry::None;
    owner->flush_done();
}

void SourceRotation::Receiver::set_stopped() const noexcept
{
    owner->resumed_     = Status::unavailable();
    owner->flush_retry_ = FlushRetry::None;
    owner->flush_done();
}

void SourceRotation::flush_done() noexcept
{
    phase_.store(Phase::FlushReady, std::memory_order_release);
    schedule();
}

void SourceRotation::finish() noexcept
{
    if (admission_metrics_) {
        const auto outcome = stopping_.load(std::memory_order_acquire) ? TreeOutcome::Cancelled : TreeOutcome::Error;
        (void)admission_metrics_->finish(outcome, tree_metrics_now_ns());
        admission_metrics_.reset();
    }
    resume();
    credit_wait_.reset();
    progress_wait_.reset();
    credits_     = {};
    plan_        = {};
    preparation_ = {};
    resumed_     = Status::Ok();
    rotation_.reset();
    if (metadata_) {
        metadata_->seal();
    }
    metadata_.reset();
    phase_.store(Phase::Idle, std::memory_order_release);
    completed_.store(target_, std::memory_order_release);
    completed_.notify_all();
    progress_->advance();
    if (requested_.load(std::memory_order_acquire) != target_) {
        schedule();
    }
}
} // namespace crowdb::tree
