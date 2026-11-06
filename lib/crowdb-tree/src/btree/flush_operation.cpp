// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_progress.h"
#include "crowdb-common/log.h"
#include "flush_operation_internal.h"
#include "source_pause.h"

#include <algorithm>

namespace crowdb::tree
{
FlushOperation::~FlushOperation() = default;

detail::CallbackSender FlushOperation::sender() noexcept
{
    return {this, &FlushOperation::submit};
}

FlushReportSender FlushOperation::report_sender() noexcept
{
    return {this, &FlushOperation::submit_report};
}

FlushReportSender FlushOperation::capture_sender() noexcept
{
    return {this, &FlushOperation::submit_capture};
}

FrozenSourceView FlushOperation::frozen_sources() const noexcept
{
    return state_->frozen;
}

void FlushOperation::submit_capture(void *source, FlushReportComplete complete, void *context)
{
    auto state = static_cast<FlushOperation *>(source)->state_;
    state->start(nullptr, context, complete, true);
}

void FlushOperation::submit_report(void *source, FlushReportComplete complete, void *context)
{
    auto state = static_cast<FlushOperation *>(source)->state_;
    state->start(nullptr, context, complete);
}

const FlushResult &FlushOperation::result() const noexcept
{
    return state_->report;
}

void FlushOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto state = static_cast<FlushOperation *>(source)->state_;
    state->start(complete, context);
}

void FlushOperation::request_stop() noexcept
{
    auto state = state_;
    state->lifecycle.fetch_or(State::kStopped, std::memory_order_acq_rel);
    if (auto *batch = state->cancel_batch.load(std::memory_order_acquire)) {
        batch->request_stop();
    }
}

bool FlushOperation::State::stopped() const noexcept
{
    return (lifecycle.load(std::memory_order_acquire) & kStopped) != 0;
}

bool FlushOperation::State::start(detail::CallbackComplete callback, void *argument,
                                  FlushReportComplete report_callback, bool only_capture, bool reject_running) noexcept
{
    auto previous = lifecycle.load(std::memory_order_acquire);
    do {
        if ((previous & kRunning) != 0) {
            if (!reject_running) {
                return false;
            }
            if (report_callback != nullptr) {
                report_callback(argument, detail::CallbackSignal::kError, Status::unavailable(), {});
            }
            else {
                callback(argument, detail::CallbackSignal::kError, Status::unavailable());
            }
            return false;
        }
    } while (!lifecycle.compare_exchange_weak(previous, previous | kRunning, std::memory_order_acq_rel));
    started_at_ns = tree_metrics_now_ns();
    batch_metrics.reset();
    if (!only_capture) {
        batch_metrics.emplace(publisher.work_metrics(), TreeStage::FlushBatch, started_at_ns);
        CRB_LOG_INFO("flush begin: tree={} start_ns={} persistent={}", static_cast<const void *>(&publisher),
                     started_at_ns, options.persistent);
    }
    capture_metrics.reset();
    drain_metrics.reset();
    complete               = callback;
    report_complete        = report_callback;
    context                = argument;
    capture_only           = only_capture;
    report.admission_stale = false;
    covered_at_start       = complete_success.load(std::memory_order_acquire) ||
                             (report.handoff_status.ok() &&
                              (report.phase == FlushPhase::kCovered || report.phase == FlushPhase::kMaintenance ||
                               report.phase == FlushPhase::kComplete));
    replaceable.store(false, std::memory_order_release);
    dispatch(CpuStage::kBegin);
    return true;
}

void FlushOperation::State::dispatch(CpuStage stage) noexcept
{
    queued_stage = stage;
    queued_owner = weak_from_this().lock();
    try {
        auto status = runtime.try_submit(lane, &State::run_cpu, this);
        if (status.ok()) {
            return;
        }
        if (options.cpu_progress) {
            options.cpu_progress->request(cpu_node, queued_owner);
            return;
        }
        auto self = std::move(queued_owner);
        fail(std::move(status));
    }
    catch (const std::bad_alloc &) {
        if (options.cpu_progress) {
            options.cpu_progress->request(cpu_node, queued_owner);
            return;
        }
        auto self = std::move(queued_owner);
        fail(Status::resource_exhausted());
    }
}

void FlushOperation::State::run_cpu(void *context) noexcept
{
    auto      *raw   = static_cast<State *>(context);
    auto       self  = std::move(raw->queued_owner);
    const auto stage = self->queued_stage;
    try {
        if (stage == CpuStage::kBegin) {
            self->begin();
        }
        else if (stage == CpuStage::kDrain) {
            self->drain_sources();
        }
        else {
            self->start_batch();
        }
    }
    catch (const std::bad_alloc &) {
        self->fail(Status::resource_exhausted());
    }
    catch (const std::exception &) {
        self->fail(Status::internal_error());
    }
}

void FlushOperation::State::begin()
{

    if (capture_only && !frozen.base && covered_at_start) {
        // A normal connection already handed off this source set. Pair the
        // newly requested capture with that complete source-free L1, not its
        // former catalog floor. Existing capture pairs never change.
        frozen.base               = publisher.capture_complete();
        frozen.frontier           = frozen.base->coverage;
        report.floor              = frozen.frontier;
        report.frontier           = frozen.frontier;
        report.published_coverage = frozen.frontier;
        report.handoff_status     = Status::Ok();
        report.phase              = FlushPhase::kCaptured;
        finish();
        return;
    }
    if (capture_only && (frozen.base || (capture && capture->drained()))) {
        if (!frozen.base) {
            frozen = {.catalog  = capture->expected_catalog(),
                      .base     = publisher.capture_complete(),
                      .frontier = capture->frontier(),
                      .batch_id = capture->batch_id()};
        }
        report.handoff_status = Status::Ok();
        report.phase          = FlushPhase::kCaptured;
        finish();
        return;
    }
    if (complete_success.load(std::memory_order_acquire)) {
        report.phase = FlushPhase::kComplete;
        finish();
        return;
    }
    if (admission_released) {
        fail(Status::unavailable("released admission requires a newly prepared flush"));
        return;
    }
    if (!capture && !batch) {
        const auto view           = publisher.capture();
        report.floor              = view->coverage;
        report.frontier           = view->coverage;
        report.published_coverage = view->coverage;
    }
    if (stopped()) {
        fail(Status::unavailable("flush stopped before dispatch"));
        return;
    }
    if (batch) {
        start_batch();
        return;
    }
    if (!capture) {
        auto       source_view = sources.catalog();
        const auto tree_view   = publisher.capture();
        if (tree_view->sources->inheritance) {
            fail(Status::unavailable("split inheritance must publish its fixed "
                                     "prefix before ordinary flush"));
            return;
        }
        if (expected_catalog && source_view != expected_catalog) {
            fail(Status::unavailable("source catalog changed after reserve preparation"));
            return;
        }
        if (tree_view->coverage != source_view->floor || tree_view->sources->floor != source_view->floor ||
            tree_view->sources->tables.size() != source_view->tables.size() ||
            !std::equal(tree_view->sources->tables.begin(), tree_view->sources->tables.end(),
                        source_view->tables.begin())) {
            fail(Status::unavailable("Tree and write source catalogs differ"));
            return;
        }
        report.floor              = source_view->floor;
        report.frontier           = source_view->floor;
        report.published_coverage = source_view->floor;
        if (!admitted && !options.persistent && contiguous.load(std::memory_order_acquire) <= source_view->floor) {
            if (capture_only) {
                frozen.base     = tree_view;
                frozen.frontier = source_view->floor;
            }
            report.handoff_status     = Status::Ok();
            report.maintenance_status = Status::Ok();
            report.deferred_pages     = tree_view->stats.dirty_pages;
            report.phase              = FlushPhase::kComplete;
            metadata->seal();
            release_metadata->seal();
            complete_success.store(true, std::memory_order_release);
            finish();
            return;
        }
        if (!admitted) {
            acquire_admission();
            return;
        }
        if (options.persistent && contiguous.load(std::memory_order_acquire) <= source_view->floor) {
            source_pause.reset();
            report.batch_id = admission_owner;
            dispatch(CpuStage::kBatch);
            return;
        }
#ifdef CROWDB_TREE_TEST_UTIL
        if (options.before_capture_for_tests) {
            options.before_capture_for_tests();
        }
#endif
        capture_metrics.emplace(publisher.work_metrics(), TreeStage::FlushCapture, tree_metrics_now_ns());
        auto status = sources.try_capture(contiguous, metadata, &capture, expected_catalog);
        if (!status.ok()) {
            fail(std::move(status));
            return;
        }
        source_pause.reset();
        captured_at_ns = tree_metrics_now_ns();
        release_metadata->seal();
        expected_catalog.reset();
        release_metadata.reset();
        release_request.reset();
        report.batch_id  = capture->batch_id();
        report.floor     = capture->floor();
        report.frontier  = capture->frontier();
        expected_sources = tree_view->sources;
    }
    if (!capture->successor_active()) {
        if (!capture_metrics) {
            capture_metrics.emplace(publisher.work_metrics(), TreeStage::FlushCapture, tree_metrics_now_ns());
        }
        publish_successor();
        return;
    }
    drain_sources();
}

void FlushOperation::State::fail(Status status) noexcept
{
    if (admitted && !capture && !batch) {
        try {
            release_admission(std::move(status));
        }
        catch (const std::bad_alloc &) {
            fail_terminal(Status::resource_exhausted("flush admission release allocation failed"));
        }
        catch (const std::exception &) {
            fail_terminal(Status::internal_error("flush admission release failed"));
        }
        return;
    }
    fail_terminal(std::move(status));
}

void FlushOperation::State::fail_terminal(Status status) noexcept
{
    report.admission_stale = status.code() == Code::kUnavailable && !admitted && !admission_released && !capture &&
                             !batch && !stopped() && expected_commit_seq != publisher.capture()->commit_seq;
    if (report.published_coverage == report.frontier && batch) {
        report.maintenance_status = std::move(status);
        report.phase              = FlushPhase::kComplete;
    }
    else {
        report.handoff_status = std::move(status);
        report.phase          = capture ? FlushPhase::kRepairRequired : FlushPhase::kCaptured;
        if (capture) {
            (void)sources.mark_repair(capture, capture->batch_id(), report.floor);
        }
    }
    finish();
}

void FlushOperation::State::finish() noexcept
{
    if ((!report.handoff_status.ok() || !report.maintenance_status.ok()) && !report.admission_stale) {
        CRB_LOG_ERROR("flush failed: batch={} frontier={} handoff={} maintenance={}", report.batch_id, report.frontier,
                      report.handoff_status.to_string(), report.maintenance_status.to_string());
    }

    auto  self            = weak_from_this().lock();
    auto  callback        = complete;
    auto *argument        = context;
    auto  report_callback = report_complete;
    if (!admitted && !capture && !batch) {
        source_pause.reset();
        options.loader.reset();
        options.consolidation_loader.reset();
        metadata->seal();
        if (release_metadata) {
            release_metadata->seal();
        }
        reservations.seal();
    }
    else if (report.phase == FlushPhase::kComplete) {
        options.loader.reset();
        options.consolidation_loader.reset();
        reservations.seal();
        if (report.handoff_status.ok() && report.maintenance_status.ok()) {
            expected_catalog.reset();
            options.persistence.reset();
            maintenance = {};
        }
    }
    auto outcome = report.handoff_status.ok() ? TreeOutcome::Success : TreeOutcome::Error;
    if (stopped()) {
        outcome = TreeOutcome::Cancelled;
    }
    const auto now = tree_metrics_now_ns();
    if (!capture_only) {
        CRB_LOG_INFO("flush end: tree={} start_ns={} end_ns={} persistent={} batch={} floor={} frontier={} "
                     "coverage={} handoff_ok={} maintenance_ok={} admission_stale={} phase={}",
                     static_cast<const void *>(&publisher), started_at_ns, now, options.persistent, report.batch_id,
                     report.floor, report.frontier, report.published_coverage, report.handoff_status.ok(),
                     report.maintenance_status.ok(), report.admission_stale, static_cast<unsigned>(report.phase));
    }
    if (batch_metrics) {
        const auto batch_outcome =
            outcome == TreeOutcome::Success && !report.maintenance_status.ok() ? TreeOutcome::Error : outcome;
        (void)batch_metrics->finish(batch_outcome, now);
    }
    if (capture_metrics) {
        (void)capture_metrics->finish(outcome, now);
    }
    if (drain_metrics) {
        (void)drain_metrics->finish(outcome, now);
    }
    if (capture && report.phase == FlushPhase::kRepairRequired && !repair_metrics) {
        repair_metrics.emplace(publisher.work_metrics(), TreeStage::FlushRepair, captured_at_ns);
    }
    const bool covered      = report.handoff_status.ok() &&
                              (report.phase == FlushPhase::kCovered || report.phase == FlushPhase::kMaintenance ||
                               report.phase == FlushPhase::kComplete);
    report.handoff_advanced = !covered_at_start && covered && report.frontier > report.floor;
    // Freeze before reopening the lifecycle. A concurrent reconnect may now
    // mutate the operation report without changing this terminal value.
    FlushResult completed{.batch_id           = report.batch_id,
                          .floor              = report.floor,
                          .frontier           = report.frontier,
                          .published_coverage = report.published_coverage,
                          .handoff_status     = report.handoff_status.without_message(),
                          .maintenance_status = report.maintenance_status.without_message(),
                          .deferred_pages     = report.deferred_pages,
                          .phase              = report.phase,
                          .handoff_advanced   = report.handoff_advanced,
                          .admission_stale    = report.admission_stale};
    replaceable.store(!admitted && !capture && !batch, std::memory_order_release);
    lifecycle.exchange(0, std::memory_order_acq_rel);
    lifecycle.notify_all();
    if (progress) {
        progress->advance();
    }
    if (report_callback != nullptr) {
        report_callback(argument, detail::CallbackSignal::kValue, Status::Ok(), std::move(completed));
    }
    else {
        callback(argument, detail::CallbackSignal::kValue, Status::Ok());
    }
}
} // namespace crowdb::tree
