// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/extent_writeback.h"
#include "snapshot/checkpoint_operation_internal.h"

namespace crowdb::tree
{
void CheckpointOperation::State::Receiver::set_value() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::Ok());
}

void CheckpointOperation::State::Receiver::set_value(FlushResult report) noexcept
{
    auto self            = std::move(owner);
    self->fixed_coverage = report.frontier;
    if (self->flush_result && report.admission_stale) {
        self->child_done(Status::Ok());
        return;
    }
    self->child_done(report.handoff_status.ok() ? std::move(report.maintenance_status)
                                                : std::move(report.handoff_status));
}

void CheckpointOperation::State::Receiver::set_error(Status status) noexcept
{
    auto self              = std::move(owner);
    self->handoff_rejected = self->stage == Stage::Handoff && status.code() == Code::kUnavailable;
    self->child_done(std::move(status));
}

void CheckpointOperation::State::Receiver::set_stopped() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::unavailable());
}

void CheckpointOperation::State::child_done(Status status) noexcept
{
    child_status = std::move(status);
    child_ready.store(true, std::memory_order_release);
    wake();
}

void CheckpointOperation::State::begin_child(detail::CallbackSender sender, Stage next)
{
    stage   = next;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    attempt.emplace(sender, shared_from_this());
    stdexec::start(attempt->connected);
}

void CheckpointOperation::State::begin_handoff()
{
    stage   = Stage::Handoff;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    handoff_attempt.emplace(handoff->report_sender(), shared_from_this());
    stdexec::start(handoff_attempt->connected);
}

void CheckpointOperation::State::wait_handoff()
{
    handoff.reset();
    handoff_wait.emplace(*options.handoff_progress, handoff_observed);
    begin_child(handoff_wait->sender(), Stage::Begin);
}

void CheckpointOperation::State::cancel_child()
{
    if (stage == Stage::Begin && handoff_wait) {
        handoff_wait->request_stop();
    }
    else if ((stage == Stage::PrepareLayout || stage == Stage::PrepareWrite) && cancel_work) {
        cancel_work();
    }
    else if (stage == Stage::Structure && structure) {
        structure->request_stop();
    }
    else if (stage == Stage::Handoff && handoff) {
        handoff->request_stop();
    }
    else if (stage == Stage::PlanPage && planning_stream) {
        planning_stream->request_stop();
    }
    else if (stage == Stage::Rebase && rebase) {
        rebase->request_stop();
    }
    else if (stage == Stage::Write && write) {
        write->request_stop();
    }
    else if (stage == Stage::Checkpoint && chunk_checkpoint) {
        chunk_checkpoint->request_stop();
    }
    else if (stage == Stage::Checkpoint && checkpoint) {
        checkpoint->request_stop();
    }
}

void CheckpointOperation::State::pump()
{
    if (stage == Stage::FinishMetrics) {
        finish(Status::Ok());
        return;
    }
    if (waiting) {
        if (stopped.load(std::memory_order_acquire)) {
            cancel_child();
        }
        if (!child_ready.load(std::memory_order_acquire)) {
            return;
        }
        waiting = false;
        attempt.reset();
        handoff_attempt.reset();
        handoff_wait.reset();
        if (std::exchange(handoff_rejected, false) && flush_result && options.handoff_progress &&
            !stopped.load(std::memory_order_acquire)) {
            wait_handoff();
            return;
        }
        if (!child_status.ok() && stage == Stage::Rebase) {
            finish_rebase(child_status.without_message());
            return;
        }
        if (!child_status.ok() && stage == Stage::PlanPage && planning_stream && prepare_structure &&
            planning_stream->result().needs_split && !stopped.load(std::memory_order_acquire)) {
            begin_structure(*planning_stream->result().needs_split);
            return;
        }
        if (!child_status.ok()) {
            fail(child_status.without_message());
            return;
        }
    }
    if (stopped.load(std::memory_order_acquire) && stage != Stage::Capture && stage != Stage::Checkpoint &&
        stage != Stage::Reconcile && stage != Stage::Rebase) {
        fail(Status::unavailable());
        return;
    }
    switch (stage) {
    case Stage::Begin:
        if (prepare_handoff) {
            handoff_observed = options.handoff_progress ? options.handoff_progress->sequence() : 0;
            auto status      = prepare_handoff(&handoff);
            if (!status.ok()) {
                if (status.code() == Code::kUnavailable && options.handoff_progress &&
                    options.handoff_blocked != nullptr && *options.handoff_blocked) {
                    wait_handoff();
                }
                else {
                    finish(std::move(status));
                }
                return;
            }
            if (!handoff) {
                finish(Status::internal_error());
                return;
            }
            begin_handoff();
        }
        else {
            preflight();
        }
        break;
    case Stage::Handoff:
        handoff.reset();
        if (flush_result) {
            if (auto completed = flush_result()) {
                report = std::move(*completed);
                finish(report.status);
            }
            else {
                stage = Stage::Begin;
                wake();
            }
            break;
        }
        preflight();
        break;
    case Stage::PrepareLayout:
        phase_ready(false);
        break;
    case Stage::PrepareWrite:
        phase_ready(true);
        break;
    case Stage::PlanPage:
        planned_page();
        break;
    case Stage::PlanDependencies:
        plan_dependencies();
        break;
    case Stage::Structure:
        structured();
        break;
    case Stage::Capture:
        captured();
        break;
    case Stage::Write:
        written();
        break;
    case Stage::Checkpoint:
        checkpointed();
        break;
    case Stage::Reconcile:
        if (options.mode == CheckpointMode::Durable && report.durable_committed) {
            begin_rebase();
        }
        else {
            finish(outcome);
        }
        break;
    case Stage::Rebase:
        finish_rebase(Status::Ok());
        break;
    case Stage::FinishMetrics:
        finish(Status::Ok());
        break;
    }
}

void CheckpointOperation::State::capture()
{
    capture_metrics.emplace(publisher.work_metrics(), TreeStage::SnapshotCapture, tree_metrics_now_ns());
    if (view->pending_handoff || view->flush_admission_owner != 0 || backend_needs_recovery()) {
        finish(Status::unavailable());
        return;
    }
    auto &transaction                 = capture_request->transaction;
    transaction.expected_coverage     = view->coverage;
    transaction.expected_l1_seq       = view->l1_seq;
    transaction.expected_next_page_id = view->next_page_id;
    transaction.next_page_id          = view->next_page_id + plan.new_overflow_frames;
    overflow_begin                    = view->next_page_id;
    stage                             = Stage::Capture;
    waiting                           = true;
    child_ready.store(false, std::memory_order_release);
    capture_request->on_done = [self = shared_from_this()](PublicationResult result) {
        const auto status = result.status.without_message();
        self->publication = std::move(result);
        self->child_done(status);
    };
    const auto submitted = options.publication ? publisher.try_submit(capture_request, options.publication)
                                               : publisher.try_submit(capture_request);
    if (!submitted) {
        waiting = false;
        finish(Status::unavailable());
    }
}

void CheckpointOperation::State::captured()
{
    capture_accepted = true;
    view             = std::move(publication.checkpoint_view);
    capture_request.reset();
    if (!view) {
        reconcile(Status::internal_error());
        return;
    }
    if (capture_metrics) {
        (void)capture_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    if (options.mode == CheckpointMode::Durable) {
        commit_metrics.emplace(publisher.work_metrics(), TreeStage::SnapshotCommit, tree_metrics_now_ns());
    }
    report.captured_coverage = view->coverage;
    report.primary_frames    = plan.primary_frames;
    if (stopped.load(std::memory_order_acquire)) {
        reconcile(Status::unavailable());
        return;
    }
    auto artifact          = detail::reserved_shared<PublishedTreeState>(control_metadata, *view);
    artifact->next_page_id = overflow_begin;
    view                   = std::move(artifact);
    const auto end         = overflow_begin + plan.new_overflow_frames;
    // The publication owns the advanced high-water mark. A tiny independent
    // owner, rather than this operation, protects the emitted interval.
    auto lease   = detail::reserved_shared<ReservedOverflowIds>(budget.bytes);
    lease->first = overflow_begin;
    lease->count = plan.new_overflow_frames;
    auto reserve = [lease, next = overflow_begin, end](uint64_t count, ReservedOverflowIds *out) mutable {
        if (count > end - next) {
            return Status::corruption("encoded overflow exceeds its planned lease");
        }
        *out = {.first = next, .count = count, .owner = lease};
        next += count;
        return Status::Ok();
    };
    auto status = CapturedWriteOperation::create(
        view, loader, store, runtime, lane, options.write, budget, std::move(reserve),
        chunk ? chunk->reserve(budget.bytes) : extent_write_reservation(backend->ledger, budget.bytes),
        inherited_pages(), &write);
    if (!status.ok()) {
        reconcile(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::Write);
}

void CheckpointOperation::State::written()
{
    written_pages              = write->result().pages;
    report.written_frames      = write->result().written_frames;
    report.reused_frames       = write->result().reused_frames;
    report.written_bytes       = write->result().written_bytes;
    report.writeback_completed = true;
    if (options.mode == CheckpointMode::Writeback) {
        write.reset();
        begin_rebase();
        return;
    }
    if (chunk) {
        begin_chunk_checkpoint();
        return;
    }
    auto metadata                   = options.metadata;
    metadata.exclusion              = options.write.frames.exclusion;
    metadata.retire_previous_anchor = metadata.retire_previous_anchor || metadata.exclusion.length != 0;
    auto status = LocalCheckpointOperation::create(written_pages, backend->checkpoints, store, runtime, lane, metadata,
                                                   budget, &checkpoint);
    if (!status.ok()) {
        reconcile(std::move(status));
        return;
    }
    write.reset();
    begin_child(checkpoint->sender(), Stage::Checkpoint);
}

void CheckpointOperation::State::checkpointed()
{
    if (chunk) {
        chunk_checkpoint->completed();
        if (!chunk_checkpoint->committed()) {
            reconcile(chunk_checkpoint->status());
            return;
        }
    }
    else {
        written_pages = checkpoint->result().checkpoint->pages;
    }
    report.durable_committed = true;
    if (commit_metrics) {
        (void)commit_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    reconcile(Status::Ok());
}

void CheckpointOperation::State::reconcile(Status status)
{
    if (report.durable_committed) {
        // The capture token excludes a later checkpoint until reconciliation.
        // Record this checkpoint's pair before releasing that serialization.
        report.durable_coverage =
            chunk ? chunk->durable_coverage.load(std::memory_order_acquire) : backend->checkpoints->durable_coverage();
        report.snapshot_sequence    = chunk ? chunk->snapshot_sequence.load(std::memory_order_acquire)
                                            : backend->checkpoints->snapshot_sequence();
        baseline->generation        = view->generation;
        baseline->capture_token     = report.checkpoint_token;
        baseline->coverage          = report.durable_coverage;
        baseline->snapshot_sequence = report.snapshot_sequence;
        baseline->backend_id        = backend_identity();
        baseline->gc_floor          = view->gc_floor;
        baseline->wal_offset        = chunk_wal_offset;
        baseline->pages             = written_pages;
        if (options.diagnostics != nullptr) {
            options.diagnostics->written_frames.store(report.written_frames, std::memory_order_release);
            options.diagnostics->total_written_frames.fetch_add(report.written_frames, std::memory_order_relaxed);
        }
    }
    outcome = std::move(status);
    stage   = Stage::Reconcile;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    finish_request->transaction.checkpoint = {.action   = report.durable_committed ? CheckpointAction::Commit
                                                                                   : CheckpointAction::Abort,
                                              .token    = report.checkpoint_token,
                                              .baseline = report.durable_committed ? baseline : nullptr};
    finish_request->on_done                = [self = shared_from_this()](PublicationResult result) {
        self->child_done(result.status.without_message());
    };
    if (!publisher.try_submit_checkpoint_completion(finish_request)) {
        waiting = false;
        finish(Status::unavailable());
    }
}

void CheckpointOperation::State::begin_rebase()
{
    if (stopped.load(std::memory_order_acquire)) {
        finish_rebase(Status::unavailable());
        return;
    }
    auto status = CapturedRebaseOperation::create(publisher, view, written_pages, runtime, lane, budget.bytes, &rebase,
                                                  options.write.frames.encode.max_sources, writeback_metrics,
                                                  cpu_progress, options.publication, options.cause);
    if (!status.ok()) {
        finish_rebase(std::move(status));
        return;
    }
    begin_child(rebase->sender(), Stage::Rebase);
}

void CheckpointOperation::State::record_rebase_result() noexcept
{
    report.rebased_pages   = rebase ? rebase->result().installed : 0;
    const auto reused      = rebase ? rebase->result().reused : 0;
    report.rebase_deferred = plan.primary_frames - report.rebased_pages - reused;
}

void CheckpointOperation::State::finish_rebase(Status status)
{
    record_rebase_result();
    if (options.mode == CheckpointMode::Writeback) {
        // The protected receipts stay alive through the last publication. Abort
        // retains checkpoint-pending changes; staged data is not a WAL cutoff.
        reconcile(std::move(status));
    }
    else {
        finish(Status::Ok());
    }
}

void CheckpointOperation::State::fail(Status status) noexcept
{
    if (chunk && stage == Stage::Checkpoint && chunk_checkpoint) {
        chunk_checkpoint->completed();
        if (chunk_checkpoint->committed()) {
            report.durable_committed = true;
            status                   = Status::Ok();
        }
    }
    if (report.durable_committed && stage == Stage::Rebase) {
        record_rebase_result();
        finish(Status::Ok());
    }
    else if (capture_accepted && stage != Stage::Reconcile) {
        try {
            reconcile(std::move(status));
        }
        catch (...) {
            finish(Status::internal_error());
        }
    }
    else {
        finish(std::move(status));
    }
}
} // namespace crowdb::tree
