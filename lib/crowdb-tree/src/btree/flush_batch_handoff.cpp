// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_batch_internal.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<const PublishedPage> append_runs(const PublishedPage                                 &old,
                                                 std::span<const std::shared_ptr<const OverlayBatch>> runs,
                                                 size_t installed, uint64_t entries, uint64_t bytes,
                                                 const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    auto logical = detail::reserved_shared<PageState>(metadata, *old.leaf, metadata);
    logical->overlays.insert(logical->overlays.begin() + static_cast<std::ptrdiff_t>(installed), runs.begin(),
                             runs.end());
    ++logical->content_id;
    ++logical->representation_id;
    auto page  = detail::reserved_shared<PublishedPage>(metadata, old);
    page->leaf = std::move(logical);
    page->merge_outgoing.reset();
    ++page->revision;
    auto &metrics                 = page->metrics;
    metrics.dirty                 = true;
    metrics.kv_known              = false;
    metrics.page_size_known       = false;
    metrics.consolidate_candidate = true;
    metrics.merge_candidate       = false;
    metrics.writeback_candidate   = true;
    metrics.overlay_runs += runs.size();
    metrics.overlay_entries += entries;
    metrics.page_size_estimate_bytes += bytes;
    return page;
}
} // namespace

void FlushBatch::State::start_range(size_t index, detail::BoundedRangeJoin::Completion done)
{
    auto &range = ranges[index];
    range.index = index;
    range.join_completion.emplace(std::move(done));
    start_range(range, {.context = &range, .complete = +[](void *raw, Status status) noexcept {
                            auto &target = *static_cast<Range *>(raw);
                            if (target.join_completion) {
                                auto completion = std::move(*target.join_completion);
                                target.join_completion.reset();
                                completion(std::move(status));
                            }
                        }});
}

void FlushBatch::State::start_range(Range &range, RangeComplete done)
{
    if (range.record->committed.load(std::memory_order_acquire)) {
        finish_range(range, done);
        return;
    }
    if ((lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
        finish_range(range, done, Status::unavailable("flush stopped before range dispatch"));
        return;
    }
    if (!range.probed && DispatchScope::take(*this)) {
        range.probed = true;
        publisher.work_metrics()->dispatch.inline_probe();
        execute_range(range, done, true);
        return;
    }
    range.probed = true;
    auto work    = detail::reserved_unique<Work>(metadata, shared_from_this(), range, done);
    auto status  = runtime.try_submit(lane, &Work::execute, work.get());
    if (status.ok()) {
        publisher.work_metrics()->dispatch.queued_step();
        [[maybe_unused]] auto *scheduled = work.release();
    }
    else if (options.cpu_progress) {
        publisher.work_metrics()->dispatch.queued_step();
        auto *ready = work.release();
        options.cpu_progress->request(ready->node, ready->owner);
    }
    else {
        finish_range(range, done, std::move(status));
    }
}

void FlushBatch::State::Work::execute(void *context) noexcept
{
    auto                        *raw = static_cast<Work *>(context);
    detail::ReservedUnique<Work> work(raw, detail::ReservationDeleter<Work>{raw->owner->metadata});
    DispatchScope                scope(*work->owner);
    work->owner->execute_range(work->range, work->done, false);
}

void FlushBatch::State::execute_range(Range &range, RangeComplete done, bool inline_probe) noexcept
{
    try {
        auto &metrics = range.metrics;
        if (!metrics) {
            record_work(publisher.work_metrics(), TreeWorkCounter::FlushRangeJobs, 1, TreeWorkCause::Flush);
            metrics = detail::reserved_shared<TreeMetricsOperation>(metadata, publisher.work_metrics(),
                                                                    TreeStage::FlushRange, tree_metrics_now_ns());
        }
        install(range, done, inline_probe);
    }
    catch (const std::bad_alloc &) {
        finish_range(range, done, Status::resource_exhausted());
    }
    catch (const std::exception &) {
        finish_range(range, done, Status::internal_error());
    }
}

void FlushBatch::State::install(Range &range, RangeComplete done, bool inline_probe)
{
    if ((lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
        finish_range(range, done, Status::unavailable("flush stopped before range execution"));
        return;
    }
    if (!range.ready && range.runs.size() < range.next_install_at) {
        auto status = prepare_run(range, inline_probe);
        if (!status.ok()) {
            finish_range(range, done, std::move(status));
            return;
        }
#ifdef CROWDB_TREE_TEST_UTIL
        if (options.after_run_for_tests) {
            options.after_run_for_tests(range.index, range.runs.size());
        }
#endif
        if (inline_probe && range.ready) {
            publisher.work_metrics()->dispatch.inline_range();
        }
        if (!range.ready && range.runs.size() < range.next_install_at) {
            start_range(range, done);
            return;
        }
    }
    if ((lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
        finish_range(range, done, Status::unavailable("flush stopped before range publication"));
        return;
    }
    if (!range.window_prepared) {
        std::shared_ptr<InstallRecord> following;
        if (!range.ready) {
            auto token = range.record->token;
            ++token.sequence;
            following = detail::reserved_shared<InstallRecord>(metadata, token);
        }
        std::shared_ptr<const PublishedPage> replacement;
        if (range.runs.size() != range.installed_runs) {
            const auto runs =
                std::span<const std::shared_ptr<const OverlayBatch>>(range.runs).subspan(range.installed_runs);
            replacement =
                append_runs(*range.expected, runs, range.installed_runs, range.entries - range.installed_entries,
                            range.bytes - range.installed_bytes, metadata);
        }
        // Every post-commit owner is ready before the root exchange. A queue
        // rejection retries this immutable window without preparing it again.
        range.installs.push_back(range.record);
        range.prepared_record = std::move(following);
        range.prepared_page   = std::move(replacement);
        range.window_prepared = true;
    }
    auto request                               = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    request->transaction.record                = range.record;
    request->transaction.metadata              = metadata;
    request->transaction.flush_admission_owner = admission_owner;
    request->transaction.expected_coverage     = report.floor;
    request->transaction.handoff               = {.batch = report.batch_id, .frontier = report.frontier};
    request->transaction.snapshot_effect       = SnapshotChangeEffect::AcceptedPrefix;
    if (range.prepared_page) {
        request->transaction.pages.push_back(
            {.page_id = range.page_id, .expected = range.expected, .replacement = range.prepared_page});
    }
    request->on_done = [self = shared_from_this(), &range, done](PublicationResult result) noexcept {
        if (!result.status.ok()) {
            self->finish_range(range, done, std::move(result.status));
            return;
        }
        self->publisher.work_metrics()->dispatch.installed(result.copied.bytes);

        record_work(self->publisher.work_metrics(), TreeWorkCounter::FlushHandoffBytes,
                    range.bytes - range.installed_bytes, TreeWorkCause::Flush);
        range.installed_runs    = range.runs.size();
        range.installed_entries = range.entries;
        range.installed_bytes   = range.bytes;
        if (range.prepared_page) {
            range.expected = std::move(range.prepared_page);
        }
        range.window_prepared = false;
        if (!range.prepared_record) {
            self->finish_range(range, done);
            return;
        }
        range.record          = std::move(range.prepared_record);
        range.next_install_at = range.next_install_at > SIZE_MAX / 2 ? SIZE_MAX : range.next_install_at * 2;
        try {
            self->start_range(range, done);
        }
        catch (const std::bad_alloc &) {
            self->finish_range(range, done, Status::resource_exhausted());
        }
        catch (const std::exception &) {
            self->finish_range(range, done, Status::internal_error());
        }
    };
    if (!publisher.try_submit(request, options.publication)) {
        finish_range(range, done, Status::resource_exhausted("publication queue is full or closed"));
    }
}

void FlushBatch::State::finish_range(Range &range, RangeComplete done, Status status) noexcept
{
    auto metrics = std::move(range.metrics);
    if (metrics) {
        auto outcome = status.ok() ? TreeOutcome::Success : TreeOutcome::Error;
        if ((lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
            outcome = TreeOutcome::Cancelled;
        }
        (void)metrics->finish(outcome, tree_metrics_now_ns());
    }
    done(std::move(status));
}

void FlushBatch::State::cover()
{
    auto  request                     = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    auto &transaction                 = request->transaction;
    transaction.record                = coverage_record;
    transaction.metadata              = metadata;
    transaction.flush_admission_owner = admission_owner;
    transaction.expected_coverage     = report.floor;
    transaction.coverage              = report.frontier;
    if (report.frontier > report.floor) {
        transaction.handoff = {.batch = report.batch_id, .frontier = report.frontier};
    }
    transaction.expected_sources = captured->sources;
    auto sources                 = detail::reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor               = report.frontier;
    sources->memory              = captured->sources->memory;
    const auto retained_count =
        captured->sources->inheritance ? captured->sources->inheritance->local_count : captured->sources->tables.size();
    uint64_t detached = 0;
    for (size_t index = 0; index < retained_count; ++index) {
        const auto &table = captured->sources->tables[index];
        const bool  captured_input =
            std::find(inputs->tables.begin(), inputs->tables.end(), table) != inputs->tables.end();
        if (!captured_input || table->slot_range().max > report.frontier) {
            sources->tables.push_back(table);
        }
        else {
            ++detached;
        }
    }
    transaction.sources = std::move(sources);
    transaction.required_installs.reserve(ranges.size());
    for (const auto &range : ranges) {
        transaction.required_installs.push_back(range.record);
    }
    if (subtree) {
        auto status = collect_subtree(transaction);
        if (!status.ok()) {
            joined(std::move(status));
            return;
        }
    }
    auto self        = shared_from_this();
    request->on_done = [self, detached](PublicationResult result) {
        self->report.handoff_status = std::move(result.status);
        if (!self->report.handoff_status.ok()) {
            self->report.phase = FlushPhase::kRepairRequired;
            self->finish();
            return;
        }
        record_work(self->publisher.work_metrics(), TreeWorkCounter::SourceDetached, detached, TreeWorkCause::Flush);
        self->report.published_coverage = self->report.frontier;
        self->report.phase              = FlushPhase::kCovered;
        (void)self->handoff_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
        self->captured.reset();
        self->inputs.reset();
        self->options.loader.reset();
        self->options.consolidation_loader.reset();
        self->ranges.clear();
        self->subtree.reset();
        self->maintain();
    };
    if (!publisher.try_submit(request, options.publication)) {
        report.handoff_status = Status::resource_exhausted("coverage publication not accepted");
        report.phase          = FlushPhase::kRepairRequired;
        finish();
    }
}

void FlushBatch::State::maintain() noexcept
{
    try {
        do_maintain();
    }
    catch (const std::bad_alloc &) {
        report.maintenance_status = Status::resource_exhausted();
        report.phase              = FlushPhase::kComplete;
        finish();
    }
    catch (const std::exception &) {
        report.maintenance_status = Status::internal_error();
        report.phase              = FlushPhase::kComplete;
        finish();
    }
}

void FlushBatch::State::do_maintain()
{
    maintenance_metrics.emplace(publisher.work_metrics(), TreeStage::MaintenancePlan, tree_metrics_now_ns());
    report.phase = FlushPhase::kMaintenance;
    if (!maintenance) {
        report.deferred_pages     = publisher.capture()->stats.dirty_pages;
        report.maintenance_status = Status::Ok();
        report.phase              = FlushPhase::kComplete;
        finish();
        return;
    }
    const auto generation = maintenance_generation.fetch_add(2, std::memory_order_acq_rel) + 2;
    auto       self       = shared_from_this();
    auto       done       = [self, generation](Status status, uint64_t deferred) {
        auto expected = generation;
        if (!self->maintenance_generation.compare_exchange_strong(expected, generation | 1,
                                                                  std::memory_order_acq_rel)) {
            return;
        }
        self->report.maintenance_status = std::move(status);
        self->report.deferred_pages     = deferred;
        self->report.phase              = FlushPhase::kComplete;
        self->finish();
    };
    try {
        maintenance(publisher.capture(), done);
    }
    catch (const std::bad_alloc &) {
        done(Status::resource_exhausted(), publisher.capture()->stats.dirty_pages);
    }
    catch (const std::exception &) {
        done(Status::internal_error(), publisher.capture()->stats.dirty_pages);
    }
}
} // namespace crowdb::tree
