// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/writeback_metrics.h"
#include "crowdb-common/log.h"
#include "snapshot/checkpoint_operation_internal.h"

#include <cassert>

namespace crowdb::tree
{
Status CheckpointOperation::create(
    std::shared_ptr<const void> owner, PublicationCoordinator &publisher, std::shared_ptr<LocalBackendState> backend,
    std::shared_ptr<AsyncPageStore> operation_store, std::shared_ptr<detail::FlushRuntime> runtime,
    std::shared_ptr<detail::FlushRuntime::Lane> lane, CheckpointOptions options, PageLoadBudget budget,
    std::shared_ptr<detail::AllocationReservation> finish_metadata, std::shared_ptr<PageLoader> loader,
    PrepareHandoff prepare_handoff, PrepareWork prepare_work, std::shared_ptr<CheckpointOperation> *out,
    PrepareCleanup prepare_cleanup, PrepareWorkAsync prepare_work_async, PrepareCleanup on_terminal,
    PrepareCleanup cancel_work, std::shared_ptr<CheckpointBackend> chunk, PrepareStructure prepare_structure,
    CheckpointOperation::AfterFlush flush_result)
{
    const bool local_valid = backend && backend->ledger && backend->checkpoints;
    const bool chunk_valid = chunk && chunk->supported();
    if ((options.mode != CheckpointMode::Durable && options.mode != CheckpointMode::Writeback) ||
        local_valid == chunk_valid || !operation_store || !runtime || !lane ||
        (!loader && !prepare_work && !prepare_work_async) || out == nullptr ||
        (!chunk_valid && !operation_store->write_completion_is_reloadable()) ||
        static_cast<bool>(budget.bytes) != static_cast<bool>(finish_metadata) ||
        (finish_metadata && (finish_metadata == budget.bytes || finish_metadata->account() != budget.bytes->account() ||
                             finish_metadata->category() == detail::BudgetClass::kIo))) {
        return Status::invalid_argument();
    }
    if ((options.mode == CheckpointMode::Durable &&
         (options.write.frames.selected_page || options.select_oldest_page)) ||
        (options.select_oldest_page && options.write.frames.selected_page) ||
        options.write.write.iu_size != (chunk ? chunk->iu_size : backend->ledger->limits().iu_size) ||
        options.write.write.chunk_backed != chunk_valid || !PageWriteOperation::allocation_bound(options.write.write) ||
        options.write.frames.plan_only ||
        options.write.frames.encode.frame_bytes > options.write.write.max_frame_bytes ||
        options.write.frames.encode.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize ||
        options.write.max_frames == 0 || options.write.max_dependency_steps == 0 ||
        options.metadata.max_segments == 0) {
        return Status::invalid_argument("checkpoint requires matching bounded page geometry");
    }
    if (chunk ? chunk->needs_recovery.load(std::memory_order_acquire) : backend->checkpoints->needs_recovery()) {
        return Status::unavailable();
    }
    try {
        if (finish_metadata && finish_metadata->available() < finish_allocation_bound()) {
            return Status::resource_exhausted();
        }
        auto state                = detail::reserved_shared<State>(budget.bytes, publisher);
        state->owner              = std::move(owner);
        state->backend            = std::move(backend);
        state->chunk              = std::move(chunk);
        state->chunk_wal_offset   = state->chunk ? state->chunk->requested_wal_offset() : 0;
        state->store              = std::move(operation_store);
        state->runtime            = std::move(runtime);
        state->lane               = std::move(lane);
        state->options            = std::move(options);
        state->max_layout_depth   = state->options.write.frames.max_depth;
        state->budget             = std::move(budget);
        state->finish_metadata    = std::move(finish_metadata);
        state->loader             = std::move(loader);
        state->prepare_handoff    = std::move(prepare_handoff);
        state->flush_result       = std::move(flush_result);
        state->prepare_structure  = std::move(prepare_structure);
        state->prepare_work       = std::move(prepare_work);
        state->prepare_cleanup    = std::move(prepare_cleanup);
        state->prepare_work_async = std::move(prepare_work_async);
        state->on_terminal        = std::move(on_terminal);
        state->cancel_work        = std::move(cancel_work);
        state->cpu_progress       = state->budget.cpu_progress;
        state->control_metadata   = state->budget.bytes;
        state->capture_request = detail::reserved_shared<PublicationRequest>(state->budget.bytes, state->budget.bytes);
        state->finish_request =
            detail::reserved_shared<PublicationRequest>(state->finish_metadata, state->finish_metadata);
        if (state->options.mode == CheckpointMode::Durable) {
            state->baseline = detail::reserved_shared<SnapshotBaseline>(state->budget.bytes);
        }
        if (!state->cpu_progress) {
            auto status = state->runtime->reserve_progress(state->lane, &State::run, state.get(),
                                                           state->finish_metadata, &state->progress);
            if (!status.ok()) {
                return status;
            }
        }
        *out = detail::reserved_construct<CheckpointOperation>(state->budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) CheckpointOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

size_t CheckpointOperation::control_allocation_bound() noexcept
{
    using detail::shared_allocation_bound;
    return shared_allocation_bound<State>() + shared_allocation_bound<CheckpointOperation>() +
           shared_allocation_bound<PublicationRequest>() + shared_allocation_bound<InstallRecord>() +
           (4 * shared_allocation_bound<PublishedTreeState>()) + (3 * PublishedSources::allocation_bound()) +
           PublicationCoordinator::empty_commit_reserve_bytes() + shared_allocation_bound<SnapshotBaseline>();
}

size_t CheckpointOperation::finish_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<PublicationRequest>() + detail::shared_allocation_bound<InstallRecord>() +
           detail::FlushRuntime::progress_allocation_bound() + PublicationCoordinator::empty_commit_reserve_bytes();
}

detail::CallbackSender CheckpointOperation::sender() noexcept
{
    return {this, &CheckpointOperation::submit};
}

const CheckpointResult &CheckpointOperation::result() const noexcept
{
    return state_->report;
}

void CheckpointOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void CheckpointOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<CheckpointOperation *>(source)->state_->start(complete, context);
}

void CheckpointOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    uint8_t idle = 0;
    if (!lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete = callback;
    context  = argument;
    lifecycle.store(2, std::memory_order_release);
    wake();
}

void CheckpointOperation::State::wake() noexcept
{
    if (lifecycle.load(std::memory_order_acquire) == 2) {
        auto self = weak_from_this().lock();
        assert(self);
        if (cpu_progress) {
            cpu_progress->request(cpu_node, std::move(self));
        }
        else {
            progress->request(std::move(self));
        }
    }
}

void CheckpointOperation::State::run(void *context) noexcept
{
    auto *self = static_cast<State *>(context);
    if (self->lifecycle.load(std::memory_order_acquire) != 2) {
        return;
    }
    try {
        self->pump();
    }
    catch (const std::bad_alloc &) {
        self->fail(Status::resource_exhausted());
    }
    catch (...) {
        self->fail(Status::internal_error());
    }
}

void CheckpointOperation::State::finish(Status status) noexcept
{
    if (stage != Stage::FinishMetrics) {
        final_status  = std::move(status);
        final_outcome = final_status.ok() ? TreeOutcome::Stale : TreeOutcome::Error;
        if (!report.durable_committed && stopped.load(std::memory_order_acquire)) {
            final_outcome = TreeOutcome::Cancelled;
        }
        stage               = Stage::FinishMetrics;
        const auto terminal = final_status.ok() ? TreeOutcome::Success : final_outcome;
        const auto now      = tree_metrics_now_ns();
        if (capture_metrics) {
            (void)capture_metrics->finish(terminal, now);
        }
        if (commit_metrics) {
            (void)commit_metrics->finish(terminal, now);
        }
    }
    if (writeback_metrics && !writeback_metrics->finish_some(final_outcome)) {
        wake();
        return;
    }
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    status = std::move(final_status);
    options.write.frames.writeback_metrics.reset();
    writeback_metrics.reset();
    report.status = std::move(status);
    if (!report.status.ok()) {
        record_work(publisher.work_metrics(), TreeWorkCounter::CheckpointFailures, 1, options.cause);
    }
    if (!report.durable_committed) {
        report.durable_coverage =
            chunk ? chunk->durable_coverage.load(std::memory_order_acquire) : backend->checkpoints->durable_coverage();
        report.snapshot_sequence = chunk ? chunk->snapshot_sequence.load(std::memory_order_acquire)
                                         : backend->checkpoints->snapshot_sequence();
    }
    view.reset();
    capture_request.reset();
    finish_request.reset();
    handoff.reset();
    structure.reset();
    write.reset();
    checkpoint.reset();
    chunk_checkpoint.reset();
    rebase.reset();
    written_pages.reset();
    baseline.reset();
    planning_stream.reset();
    planner.reset();
    loader.reset();
    budget = {};
    work_owner.reset();
    store.reset();
    prepare_handoff    = {};
    flush_result       = {};
    prepare_structure  = {};
    prepare_work       = {};
    prepare_work_async = {};
    prepared_work      = {};
    if (report.durable_committed && prepare_cleanup) {
        try {
            prepare_cleanup();
        }
        catch (...) {
            CRB_LOG_WARN("post-checkpoint cleanup submission failed; retaining extents for a later pass");
        }
    }
    prepare_cleanup = {};
    if (on_terminal) {
        try {
            on_terminal();
        }
        catch (...) {
            CRB_LOG_ERROR("checkpoint terminal observer threw");
        }
    }
    on_terminal = {};
    cancel_work = {};
    owner.reset();
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (!report.durable_committed && stopped.load(std::memory_order_acquire)) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(context, signal, report.status.without_message());
}
} // namespace crowdb::tree
