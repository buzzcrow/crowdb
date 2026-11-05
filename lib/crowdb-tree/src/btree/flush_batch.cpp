// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_batch_internal.h"

#include <algorithm>

namespace crowdb::tree
{
Status FlushBatch::prepare(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                           std::shared_ptr<detail::FlushRuntime::Lane> lane, uint64_t batch_id, uint64_t frontier,
                           FlushBatchOptions options, Maintenance maintenance, std::shared_ptr<FlushBatch> *out,
                           std::shared_ptr<const PublishedSources>        inputs,
                           std::shared_ptr<detail::AllocationReservation> metadata, uint64_t flush_admission_owner)
try {
    if (out == nullptr || !lane || options.parallelism == 0 || options.install_run_window == 0 ||
        options.inline_entries == 0 || options.inline_bytes == 0 || options.max_run_bytes == 0 ||
        options.run_limits.max_entries == 0 || options.run_limits.max_key_bytes == 0 ||
        options.run_limits.key_block_bytes == 0 || options.routing_leaves_per_step == 0 ||
        options.max_route_depth == 0 || options.max_route_depth > 64) {
        return Status::invalid_argument("flush needs bounded ranges, runs and a runtime lane");
    }
    auto state             = detail::reserved_shared<State>(metadata, publisher, runtime, metadata);
    state->captured        = publisher.capture();
    state->inputs          = inputs ? std::move(inputs) : state->captured->sources;
    state->metadata        = std::move(metadata);
    state->admission_owner = flush_admission_owner;
    if (state->captured->flush_admission_owner != flush_admission_owner) {
        return Status::unavailable("flush does not own the captured publication admission");
    }
    if (frontier < state->captured->coverage || state->inputs->floor != state->captured->coverage ||
        state->inputs->tables.size() > options.max_sources) {
        return Status::invalid_argument("invalid flush frontier or source count");
    }
    if (state->captured->sources->inheritance) {
        const auto &inherited = *state->captured->sources->inheritance;
        const auto &published = state->captured->sources->tables;
        if (inherited.local_count > published.size() || frontier != inherited.frontier ||
            state->inputs->tables.size() != published.size() - inherited.local_count ||
            !std::equal(state->inputs->tables.begin(), state->inputs->tables.end(),
                        published.begin() + static_cast<std::ptrdiff_t>(inherited.local_count))) {
            return Status::invalid_argument("split handoff must consume exactly its inherited source suffix");
        }
    }
    for (const auto &table : state->inputs->tables) {
        if (!table || !table->closed() || table->writers() != 0) {
            return Status::unavailable("captured writers must drain asynchronously before handoff");
        }
        if (std::find(state->captured->sources->tables.begin(), state->captured->sources->tables.end(), table) ==
            state->captured->sources->tables.end()) {
            return Status::invalid_argument("flush input is absent from the published catalog");
        }
    }
    for (const auto &table : state->captured->sources->tables) {
        if (!table || (table->closed() && std::find(state->inputs->tables.begin(), state->inputs->tables.end(),
                                                    table) == state->inputs->tables.end())) {
            return Status::invalid_argument("every closed source must participate in the captured batch");
        }
    }
    state->lane    = std::move(lane);
    state->options = std::move(options);
    if (state->options.consolidation) {
        state->options.consolidation->pages.gc_floor =
            std::max(state->captured->gc_floor, std::min(publisher.requested_gc_floor(), frontier));
    }
    state->maintenance               = std::move(maintenance);
    state->report.batch_id           = batch_id;
    state->report.floor              = state->captured->coverage;
    state->report.frontier           = frontier;
    state->report.published_coverage = state->captured->coverage;
    state->coverage_record           = detail::reserved_shared<InstallRecord>(
        state->metadata,
        InstallToken{.generation = state->captured->generation, .batch = batch_id, .range = UINT64_MAX, .sequence = 0});
    state->routing_record = detail::reserved_shared<InstallRecord>(
        state->metadata,
        InstallToken{
            .generation = state->captured->generation, .batch = batch_id, .range = UINT64_MAX - 2, .sequence = 0});
    const auto root = state->captured->mapping.get(state->captured->root_page_id);
    if (!root || root->level > 64) {
        return Status::unavailable("flush needs an initialized bounded routing tree");
    }
    *out = detail::reserved_construct<FlushBatch>(
        state->metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) FlushBatch(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("flush preparation allocation failed");
}

FlushBatch::~FlushBatch() = default;

detail::CallbackSender FlushBatch::sender() noexcept
{
    return {this, &FlushBatch::submit};
}

void FlushBatch::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto state = static_cast<FlushBatch *>(source)->state_;
    state->start(complete, context);
}

void FlushBatch::request_stop() noexcept
{
    state_->lifecycle.fetch_or(State::kStopped, std::memory_order_acq_rel);
    // The join owns late completions. Each queued CPU step observes this flag;
    // it never frees another step's receiver or payload on cancellation.
}

const FlushResult &FlushBatch::result() const noexcept
{
    return state_->report;
}

void FlushBatch::State::start(detail::CallbackComplete callback, void *argument)
{
    auto    previous = lifecycle.load(std::memory_order_acquire);
    uint8_t accepted;
    do {
        if ((previous & kRunning) != 0) {
            callback(argument, detail::CallbackSignal::kError, Status::unavailable("flush batch already running"));
            return;
        }
        accepted = kRunning | kStarted | ((previous & kStarted) == 0 ? previous & kStopped : 0);
    } while (!lifecycle.compare_exchange_weak(previous, accepted, std::memory_order_acq_rel));
    complete = callback;
    context  = argument;
    terminal.store(false);
    batch_metrics.reset();
    if (options.measure_batch) {
        batch_metrics.emplace(publisher.work_metrics(), TreeStage::FlushBatch, tree_metrics_now_ns());
    }
    handoff_metrics.reset();
    maintenance_metrics.reset();
    try {
        if (coverage_record->committed.load(std::memory_order_acquire)) {
            maintain();
            return;
        }
        record_work(publisher.work_metrics(), TreeWorkCounter::FlushInputTables, inputs->tables.size(),
                    TreeWorkCause::Flush);
        report.phase = FlushPhase::kHandoff;
        handoff_metrics.emplace(publisher.work_metrics(), TreeStage::FlushHandoff, tree_metrics_now_ns());
        if (!planned) {
            // Planning can fail before any range is installed. Retry starts
            // from the same frozen sources and immutable routing view.
            ranges.clear();
            route.reset();
            plan_key.reset();
            schedule_plan();
        }
        else {
            start_handoff();
        }
    }
    catch (const std::bad_alloc &) {
        joined(Status::resource_exhausted());
    }
    catch (const std::exception &) {
        joined(Status::internal_error());
    }
}

void FlushBatch::State::start_handoff()
{
    auto self = shared_from_this();
    join      = detail::reserved_unique<detail::BoundedRangeJoin>(
        metadata, ranges.size(), options.parallelism, this,
        +[](void *context, size_t index, detail::BoundedRangeJoin::Completion done) {
            static_cast<State *>(context)->start_range(index, std::move(done));
        },
        metadata);
    operation = detail::reserved_unique<Attempt>(metadata, join->sender(), self);
    stdexec::start(operation->connected);
}

void FlushBatch::State::Receiver::set_value() noexcept
{
    auto self = std::move(owner);
    self->joined(Status::Ok());
}

void FlushBatch::State::Receiver::set_error(Status error) noexcept
{
    auto self = std::move(owner);
    self->joined(std::move(error));
}

void FlushBatch::State::Receiver::set_stopped() noexcept
{
    auto self = std::move(owner);
    self->joined(Status::unavailable("flush stopped"));
}

void FlushBatch::State::joined(Status status) noexcept
{
    report.handoff_status = std::move(status);
    if (!report.handoff_status.ok()) {
        report.phase = FlushPhase::kRepairRequired;
        finish();
        return;
    }
    try {
        cover();
    }
    catch (const std::bad_alloc &) {
        report.handoff_status = Status::resource_exhausted("flush root publication allocation failed");
        report.phase          = FlushPhase::kRepairRequired;
        finish();
    }
    catch (const std::exception &) {
        report.handoff_status = Status::internal_error();
        report.phase          = FlushPhase::kRepairRequired;
        finish();
    }
}

void FlushBatch::State::finish() noexcept
{
    if (terminal.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto       self     = weak_from_this().lock();
    auto       callback = complete;
    auto      *argument = context;
    const auto now      = tree_metrics_now_ns();
    auto       outcome  = TreeOutcome::Success;
    if (!report.handoff_status.ok() || !report.maintenance_status.ok()) {
        outcome = (lifecycle.load() & kStopped) != 0 ? TreeOutcome::Cancelled : TreeOutcome::Error;
    }
    if (batch_metrics) {
        (void)batch_metrics->finish(outcome, now);
    }
    if (handoff_metrics) {
        (void)handoff_metrics->finish(report.handoff_status.ok() ? TreeOutcome::Success : outcome, now);
    }
    if (maintenance_metrics) {
        (void)maintenance_metrics->finish(report.maintenance_status.ok() ? TreeOutcome::Success : outcome, now);
    }
    if (report.phase == FlushPhase::kComplete && report.handoff_status.ok() && report.maintenance_status.ok()) {
        options.persistence.reset();
        maintenance = {};
        checkpoint_view.reset();
        // A retained completion report needs the committed identity, but no
        // longer needs the pre-persistence pages owned by its install record.
        coverage_record->checkpoint_view.reset();
    }
    lifecycle.fetch_and(static_cast<uint8_t>(~kRunning), std::memory_order_release);
    callback(argument, detail::CallbackSignal::kValue, Status::Ok());
}
} // namespace crowdb::tree
