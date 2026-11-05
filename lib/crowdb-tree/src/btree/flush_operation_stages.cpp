// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_progress.h"
#include "flush_operation_internal.h"

namespace crowdb::tree
{
void FlushOperation::State::publish_successor()
{
    if (!published_sources) {
        auto next    = detail::reserved_shared<PublishedSources>(metadata, metadata);
        next->floor  = capture->floor();
        next->memory = capture->next_catalog()->memory;
        next->tables.assign(capture->next_catalog()->tables.begin(), capture->next_catalog()->tables.end());
        published_sources = std::move(next);
    }
    if (!source_record) {
        source_record = detail::reserved_shared<InstallRecord>(
            metadata, InstallToken{.generation = capture->next_catalog()->generation,
                                   .batch      = capture->batch_id(),
                                   .range      = UINT64_MAX - 1,
                                   .sequence   = 0});
    }
    auto request         = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    request->transaction = {.record                = source_record,
                            .expected_coverage     = report.floor,
                            .expected_sources      = expected_sources,
                            .sources               = published_sources,
                            .metadata              = metadata,
                            .flush_admission_owner = admission_owner};
    auto self            = shared_from_this();
    request->on_done     = [self](PublicationResult result) {
        try {
            if (!result.status.ok()) {
                self->fail(std::move(result.status));
                return;
            }
            auto status = self->sources.activate_successor(self->capture, self->capture->batch_id());
            if (!status.ok()) {
                self->fail(std::move(status));
                return;
            }
            if (self->capture_metrics) {
                (void)self->capture_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
            }
            if (self->progress) {
                self->progress->advance();
            }
            self->dispatch(CpuStage::kDrain);
        }
        catch (const std::bad_alloc &) {
            self->fail(Status::resource_exhausted());
        }
        catch (const std::exception &) {
            self->fail(Status::internal_error());
        }
    };
    if (!publisher.try_submit(request, options.publication)) {
        fail(Status::resource_exhausted("source publication queue full"));
    }
}

void FlushOperation::State::drain_sources()
{
    if (!capture->drained()) {
        drain_metrics.emplace(publisher.work_metrics(), TreeStage::FlushWriterDrain, tree_metrics_now_ns());
    }
    drain_attempt.emplace(capture->drain_sender(), shared_from_this(), Stage::kDrain);
    stdexec::start(drain_attempt->connected);
}

void FlushOperation::State::start_batch()
{
    if (stopped()) {
        fail(Status::unavailable("flush stopped before handoff"));
        return;
    }
    if (!batch) {
        auto inputs   = detail::reserved_shared<PublishedSources>(metadata, metadata);
        inputs->floor = report.floor;
        if (capture) {
            inputs->memory = capture->expected_catalog()->memory;
            inputs->tables.assign(capture->expected_catalog()->tables.begin(),
                                  capture->expected_catalog()->tables.end());
        }
        auto weak             = weak_from_this();
        auto maintenance_step = [weak](auto view, auto done) {
            if (auto self = weak.lock()) {
                self->maintain(std::move(view), std::move(done));
            }
            else {
                done(Status::unavailable(), 0);
            }
        };
        options.measure_batch = false;
        auto status =
            FlushBatch::prepare(publisher, runtime, lane, report.batch_id, report.frontier, options,
                                std::move(maintenance_step), &batch, std::move(inputs), metadata, admission_owner);
        if (!status.ok()) {
            fail(std::move(status));
            return;
        }
        cancel_batch.store(batch.get(), std::memory_order_release);
    }
    batch_attempt.emplace(batch->sender(), shared_from_this(), Stage::kBatch);
    if (stopped()) {
        batch->request_stop();
    }
    stdexec::start(batch_attempt->connected);
}

void FlushOperation::State::Receiver::set_value() noexcept
{
    auto self = std::move(owner);
    self->stage_done(stage, Status::Ok());
}

void FlushOperation::State::Receiver::set_error(Status status) noexcept
{
    auto self = std::move(owner);
    self->stage_done(stage, std::move(status));
}

void FlushOperation::State::Receiver::set_stopped() noexcept
{
    auto self = std::move(owner);
    self->stage_done(stage, Status::unavailable());
}

void FlushOperation::State::stage_done(Stage stage, Status status) noexcept
{
    try {
        if (stage == Stage::kDrain && drain_metrics) {
            auto outcome = status.ok() ? TreeOutcome::Success : TreeOutcome::Error;
            if (stopped()) {
                outcome = TreeOutcome::Cancelled;
            }
            (void)drain_metrics->finish(outcome, tree_metrics_now_ns());
        }
        if (!status.ok()) {
            fail(std::move(status));
            return;
        }
        if (stage == Stage::kDrain) {
            if (capture_only && !frozen.base) {
                frozen = {.catalog  = capture->expected_catalog(),
                          .base     = publisher.capture_complete(),
                          .frontier = capture->frontier(),
                          .batch_id = capture->batch_id()};
            }
            if (capture_only) {
                report.handoff_status = Status::Ok();
                report.phase          = FlushPhase::kCaptured;
                finish();
                return;
            }
            dispatch(CpuStage::kBatch);
            return;
        }
        report = batch->result();
        if (!report.handoff_status.ok() && capture) {
            (void)sources.mark_repair(capture, capture->batch_id(), report.published_coverage);
        }
        if (report.handoff_status.ok() && report.maintenance_status.ok() && source_acknowledged) {
            complete_success.store(true, std::memory_order_release);
        }
        finish();
    }
    catch (const std::bad_alloc &) {
        fail(Status::resource_exhausted());
    }
    catch (const std::exception &) {
        fail(Status::internal_error());
    }
}

Status FlushOperation::State::acknowledge(const PublishedTreeState &view)
{
    if (source_acknowledged) {
        return Status::Ok();
    }
    if (!capture && options.persistent && view.coverage == report.frontier && view.sources->floor == report.frontier) {
        source_acknowledged = true;
        admitted            = false;
        return Status::Ok();
    }
    if (!capture || view.coverage != capture->frontier() || view.sources->floor != capture->frontier()) {
        return Status::unavailable("published coverage does not match source capture");
    }
    size_t index = 0;
    for (const auto &table : capture->next_catalog()->tables) {
        if (table != capture->successor() && table->slot_range().max <= capture->frontier()) {
            continue;
        }
        if (index == view.sources->tables.size() || view.sources->tables[index++] != table) {
            return Status::unavailable("published source membership does not match covered capture");
        }
    }
    if (index != view.sources->tables.size()) {
        return Status::unavailable("unexpected covered source membership");
    }
    auto status = sources.finish_covered(capture, capture->batch_id(), view.coverage);
    if (!status.ok()) {
        return status;
    }
    if (repair_metrics) {
        (void)repair_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    report.published_coverage = view.coverage;
    source_acknowledged       = true;
    admitted                  = false;
    capture.reset();
    source_record.reset();
    expected_sources.reset();
    published_sources.reset();
    options.loader.reset();
    options.consolidation_loader.reset();
    metadata->seal();
    // Automatic maintenance still owns the prepaid cold-page load window.
    // The terminal flush path seals Routing and I/O after its last callback.
    return Status::Ok();
}

void FlushOperation::State::maintain(std::shared_ptr<const PublishedTreeState> view,
                                     FlushBatch::MaintenanceComplete           done)
{
    if (options.persistent && maintenance) {
        auto self       = shared_from_this();
        persistent_done = std::move(done);
        maintenance(std::move(view), [self](Status result, uint64_t deferred) mutable {
            const auto current = self->publisher.capture();
            if (!current->checkpoint_inflight && current->flush_admission_owner == 0) {
                auto acknowledged = self->acknowledge(*current);
                if (acknowledged.ok() && !result.ok()) {
                    self->replaceable.store(true, std::memory_order_release);
                }
                if (result.ok()) {
                    result = std::move(acknowledged);
                }
            }
            auto completed = std::move(self->persistent_done);
            completed(std::move(result), deferred);
        });
        return;
    }
    auto status = acknowledge(*view);
    if (!status.ok()) {
        done(std::move(status), view->stats.dirty_pages);
        return;
    }
    if (maintenance) {
        maintenance(std::move(view), std::move(done));
    }
    else {
        done(Status::Ok(), view->stats.dirty_pages);
    }
}
} // namespace crowdb::tree
