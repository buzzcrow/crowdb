// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_batch_internal.h"
#include "page_load.h"

namespace crowdb::tree
{
void FlushBatch::State::acquire_route()
{
    const auto current = publisher.capture();
    if (current->coverage != report.floor || current->sources != captured->sources ||
        current->generation != captured->generation) {
        joined(Status::unavailable("flush sources or coverage changed before routing"));
        return;
    }
    captured             = current;
    auto request         = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    request->transaction = {
        .record                = routing_record,
        .expected_coverage     = report.floor,
        .metadata              = metadata,
        .handoff               = PendingHandoff{.batch = report.batch_id, .frontier = report.frontier},
        .expected_commit_seq   = current->commit_seq,
        .flush_admission_owner = admission_owner
    };
    auto self        = shared_from_this();
    request->on_done = [self](PublicationResult result) noexcept {
        try {
            self->schedule_plan(std::move(result.status));
        }
        catch (const std::bad_alloc &) {
            self->joined(Status::resource_exhausted());
        }
        catch (const std::exception &) {
            self->joined(Status::internal_error());
        }
    };
    if (!publisher.try_submit(request, options.publication)) {
        joined(Status::resource_exhausted("flush routing admission queue full"));
    }
}

std::optional<OwnedKey> FlushBatch::State::first_input(std::optional<Slice> lower)
{
    if (options.selection.lower && (!lower || *lower < Slice(*options.selection.lower))) {
        lower = Slice(*options.selection.lower);
    }
    std::optional<OwnedKey> first;
    for (const auto &table : inputs->tables) {
        auto guard  = table->read_guard(metadata);
        auto cursor = table->range_cursor(lower, options.selection.upper_slice(), report.frontier, report.floor);
        if (cursor.valid() && (!first || cursor.key().compare(Slice(*first)) < 0)) {
            first.emplace(cursor.key(), metadata);
        }
    }
    return first;
}

void FlushBatch::State::schedule_plan(Status status, std::shared_ptr<const InnerPageState> loaded, bool resume)
{
    auto work =
        detail::reserved_unique<PlanWork>(metadata, shared_from_this(), std::move(status), std::move(loaded), resume);
    auto submitted = runtime.try_submit(lane, &PlanWork::execute, work.get());
    if (submitted.ok()) {
        [[maybe_unused]] auto *scheduled = work.release();
    }
    else if (options.cpu_progress) {
        auto *ready = work.release();
        options.cpu_progress->request(ready->node, ready->owner);
    }
    else {
        joined(std::move(submitted));
    }
}

void FlushBatch::State::PlanWork::execute(void *context) noexcept
{
    auto                            *raw = static_cast<PlanWork *>(context);
    detail::ReservedUnique<PlanWork> work(raw, detail::ReservationDeleter<PlanWork>{raw->owner->metadata});
    DispatchScope                    scope(*work->owner);
    try {
        if (!work->status.ok()) {
            work->owner->joined(std::move(work->status));
            return;
        }
        work->owner->plan_step(std::move(work->loaded), work->resume);
    }
    catch (const std::bad_alloc &) {
        work->owner->joined(Status::resource_exhausted("flush routing allocation failed"));
    }
    catch (const std::exception &) {
        work->owner->joined(Status::internal_error("flush routing failed"));
    }
}

void FlushBatch::State::plan_step(std::shared_ptr<const InnerPageState> loaded, bool resume)
{
    if ((lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
        joined(Status::unavailable("flush stopped while routing"));
        return;
    }
    if (report.frontier > report.floor && !routing_record->committed.load(std::memory_order_acquire)) {
        acquire_route();
        return;
    }
    if (options.consolidation) {
        start_subtree();
        return;
    }
    if (!route) {
        route    = detail::reserved_unique<PublishedRouteCursor>(metadata, captured->root_page_id, captured->mapping,
                                                                 options.max_route_depth, false, metadata);
        plan_key = first_input({});
    }
    for (size_t visited = 0; visited < options.routing_leaves_per_step && plan_key; ++visited) {
        auto status = resume ? route->resume_inner(std::move(loaded)) : route->seek(Slice(*plan_key), false);
        resume      = false;
        if (!status.ok()) {
            joined(std::move(status));
            return;
        }
        if (auto backing = route->needs_inner()) {
            if (!options.loader) {
                joined(Status::unavailable("cold flush index requires an asynchronous loader"));
                return;
            }
            auto self = shared_from_this();
            options.loader->load_inner(std::move(backing), [self](Status loaded_status, auto inner) noexcept {
                try {
                    self->schedule_plan(std::move(loaded_status), std::move(inner), true);
                }
                catch (const std::bad_alloc &) {
                    self->joined(Status::resource_exhausted());
                }
                catch (const std::exception &) {
                    self->joined(Status::internal_error());
                }
            });
            return;
        }
        const auto &leaf = route->leaf();
        if (!leaf || !leaf->range.contains(Slice(*plan_key))) {
            joined(Status::corruption("flush input has no matching captured leaf range"));
            return;
        }
        const auto pid      = route->leaf_page_id();
        auto       progress = detail::reserved_shared<InstallRecord>(
            metadata,
            InstallToken{
                .generation = captured->generation, .batch = report.batch_id, .range = ranges.size(), .sequence = 0});
        auto selected = leaf->range;
        if (options.selection.lower && (!selected.lower || *selected.lower < *options.selection.lower)) {
            selected.lower = options.selection.lower;
        }
        if (options.selection.upper && (!selected.upper || *selected.upper > *options.selection.upper)) {
            selected.upper = options.selection.upper;
        }
        ranges.push_back({.page_id  = pid,
                          .bounds   = std::move(selected),
                          .expected = captured->mapping.get(pid),
                          .record   = std::move(progress),
                          .runs     = OverlayStorage{OverlayAllocator(metadata)},
                          .installs =
                              detail::ReservedVector<std::shared_ptr<InstallRecord>>{
                                  detail::ReservationAllocator<std::shared_ptr<InstallRecord>>(metadata)},
                          .next_key        = {},
                          .next_install_at = options.install_run_window});
        // The next source seek jumps past this entire leaf, including keys
        // belonging to other source tables. Untouched subtrees are never read.
        plan_key = leaf->range.upper ? first_input(Slice(*leaf->range.upper)) : std::nullopt;
    }
    if (plan_key) {
        schedule_plan();
        return;
    }
    route.reset();
    planned = true;
    start_handoff();
}
} // namespace crowdb::tree
