// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "crowdb-common/log.h"
#include "maintenance_operation_internal.h"
#include "maintenance_split_layout.h"
#include "page_cache_residency.h"

#include <algorithm>
#include <cassert>
#include <exception>
#include <stdexcept>

namespace crowdb::tree::detail
{
namespace
{
std::atomic<uint64_t> next_maintenance_invocation{1};
}

MaintenanceOperation::MaintenanceOperation(std::shared_ptr<MaintenanceFactory> owner, uint64_t tree_generation,
                                           uint64_t dirty_pages, FlushBatch::MaintenanceComplete completion,
                                           std::shared_ptr<AllocationReservation> reservation)
    : factory(std::move(owner)),
      loader(std::exchange(factory->loader, {})),
      done(std::move(completion)),
      publication(factory->options.publication),
      generation(tree_generation),
      invocation(next_maintenance_invocation.fetch_add(1, std::memory_order_relaxed)),
      dirty(dirty_pages),
      queued_resources(reservation, TreeResourceMetric::MaintenanceQueued),
      active_resources(reservation, TreeResourceMetric::MaintenanceInflight),
      outputs(ReservationAllocator<uint64_t>(reservation))
{
    if (!loader) {
        loader = factory->retry_loader.lock();
    }
    publication.metadata       = std::move(reservation);
    publication.pages.metadata = publication.metadata;
    activity_slot              = factory->publisher.start_maintenance(tree_metrics_now_ns());
}

MaintenanceOperation::~MaintenanceOperation()
{
    if (activity_slot != PublicationCoordinator::kMaintenanceSlots) {
        factory->publisher.finish_maintenance(activity_slot);
    }
}

void MaintenanceOperation::schedule() noexcept
{
    wake_requested.store(true, std::memory_order_release);
    if (finished.load(std::memory_order_acquire) || scheduled.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    queued_resources.begin(1);
    try {
        if (factory->cpu_progress) {
            factory->cpu_progress->request(cpu_node, lifetime);
            return;
        }
        if (factory->runtime.try_submit(factory->lane, &MaintenanceOperation::run, this).ok()) {
            return;
        }
    }
    catch (const std::bad_alloc &) {
        scheduled.exchange(false, std::memory_order_acq_rel);
        finish(target_view ? Status::resource_exhausted() : Status::Ok());
        return;
    }
    scheduled.exchange(false, std::memory_order_acq_rel);
    finish(target_view ? Status::unavailable("split continuation unavailable") : Status::Ok());
}

void MaintenanceOperation::run(void *context) noexcept
{
    auto self = static_cast<MaintenanceOperation *>(context)->weak_from_this().lock();
    if (!self) {
        std::terminate();
    }
    self->wake_requested.exchange(false, std::memory_order_acq_rel);
    self->queued_resources.reset();
    if (!self->metrics_started) {
        self->metrics_started = true;
        self->active_resources.begin(1);
    }
    try {
        self->step();
    }
    catch (const std::bad_alloc &) {
        self->finish(self->target_view ? Status::resource_exhausted() : Status::Ok());
    }
    catch (const std::exception &) {
        self->finish(Status::internal_error());
    }
    self->scheduled.exchange(false, std::memory_order_acq_rel);
    if (self->wake_requested.load(std::memory_order_acquire) && !self->finished.load(std::memory_order_acquire)) {
        self->schedule();
    }
}

void MaintenanceOperation::step()
{
    if (target_stopped.load(std::memory_order_acquire)) {
        if (!cold || !cold->waiting || cold->ready.load(std::memory_order_acquire)) {
            finish(Status::unavailable("checkpoint split cancelled"));
        }
        return;
    }
    if (cold) {
        cold_step();
        return;
    }
    if (!initialized) {
        initialize();
        return;
    }
    if (phase == Phase::Collect) {
        collect();
        return;
    }
    if (steps == factory->options.max_steps || candidates->empty()) {
        finish(Status::Ok());
        return;
    }
    if (index == candidates->size()) {
        if (phase == Phase::Merge) {
            finish(Status::Ok());
            return;
        }
        if (phase == Phase::InnerMerge) {
            phase = Phase::Analyze;
        }
        else if (phase == Phase::Analyze) {
            phase = Phase::Coalesce;
        }
        else if (phase == Phase::Coalesce) {
            phase = Phase::ClassifyMerge;
        }
        else {
            phase = Phase::Merge;
        }
        index = 0;
    }
    process();
}

void MaintenanceOperation::initialize()
{
    const auto &options = factory->options;
    if (!factory->lane || options.visits_per_step == 0 || options.max_visits == 0 || options.max_candidates == 0 ||
        options.max_steps == 0 || options.publication.metadata) {
        finish(Status::invalid_argument("maintenance needs bounded work and fresh reclaim credit"));
        return;
    }
    candidates =
        reserved_shared<Candidates>(publication.metadata, ReservationAllocator<Candidate>(publication.metadata));
    candidates->reserve(options.max_candidates);
    const auto view = capture_l1();
    if (view->generation != generation) {
        finish(Status::unavailable("maintenance generation changed"));
        return;
    }
    if (view->root_page_id == kInvalidPageId || view->mapping.empty()) {
        finish(Status::invalid_argument("maintenance requires an initialized tree"));
        return;
    }
    if (!view->mapping.get(view->root_page_id) || view->next_page_id == 0 ||
        view->next_page_id > MappingView::kPageLimit) {
        finish(Status::corruption("maintenance root is missing"));
        return;
    }
    if (target_view) {
        const auto current = factory->publisher.capture();
        if (current->generation != generation || current->l1_seq != target_view->l1_seq) {
            finish(Status::unavailable("checkpoint split view changed"));
            return;
        }
        candidates->push_back({.pid = target_pid});
        initialized = true;
        phase       = Phase::Analyze;
        schedule();
        return;
    }
    scan_limit = view->next_page_id;
    // Each flush creates a fresh factory. Keep fairness on the Tree owner so
    // bounded collection does not repeatedly start at the leftmost pages.
    scan_pid    = factory->publisher.maintenance_scan_pid() % scan_limit;
    initialized = true;
    schedule();
}

std::shared_ptr<const PublishedTreeState> MaintenanceOperation::capture_l1()
{
    if (target_view) {
        return target_view;
    }
    auto current = factory->publisher.capture();
    dirty        = current->stats.dirty_pages;
    auto view    = reserved_shared<PublishedTreeState>(publication.metadata, *current);
    view->sources.reset();
    return view;
}

void MaintenanceOperation::collect()
{
    const auto view = capture_l1();
    if (view->generation != generation) {
        finish(Status::unavailable("maintenance generation changed"));
        return;
    }
    const auto &options = factory->options;
    const auto  limit   = std::min<uint64_t>(options.max_visits, scan_limit);
    size_t      visited = 0;
    while (visits < limit && visited < options.visits_per_step && candidates->size() < options.max_candidates) {
        const auto pid = scan_pid;
        scan_pid       = (scan_pid + 1) % scan_limit;
        ++visits;
        ++visited;
        const auto page = view->mapping.get(pid);
        if (!page) {
            continue;
        }
        if ((page->level == 0 && (!page->leaf || page->inner)) ||
            (page->level != 0 &&
             (page->leaf || (page->inner ? page->inner->children.empty() ||
                                               page->inner->separators.size() + 1 != page->inner->children.size()
                                         : !page->inner_backing || !page->inner_backing->readable())))) {
            finish(Status::corruption("maintenance found an invalid page shape"));
            return;
        }
        const auto right = page->leaf ? view->mapping.get(page->leaf->right_sibling) : nullptr;
        // The right leaf can become small during this operation's later
        // analyze/coalesce phases. Retain its left pair before that happens.
        const bool shrinking_right =
            right && right->leaf &&
            (!right->metrics.page_size_known || !right->metrics.kv_known || right->metrics.consolidate_candidate ||
             maintenance_needs_coalesce(*right->leaf, publication.pages.max_sources) ||
             right->metrics.page_size_bytes < publication.pages.merge_trigger_bytes);
        if (page->level == 0 &&
            ((publication.inner_merge_keys != 0 && view->mapping.get(view->root_page_id)->level > 1) ||
             shrinking_right || !page->metrics.page_size_known || !page->metrics.kv_known ||
             page->metrics.split_candidate || page->metrics.page_size_bytes > publication.pages.primary_page_bytes ||
             page->metrics.consolidate_candidate || page->metrics.merge_candidate ||
             page->metrics.page_size_bytes < publication.pages.merge_trigger_bytes ||
             maintenance_needs_coalesce(*page->leaf, publication.pages.max_sources))) {
            candidates->push_back({.pid = pid});
        }
    }
    factory->publisher.set_maintenance_scan_pid(scan_pid);
    if (visits == limit || candidates->size() == options.max_candidates) {
        phase = Phase::InnerMerge;
    }
    schedule();
}

void MaintenanceOperation::advance()
{
    if (target_view) {
        finish(Status::resource_exhausted("checkpoint split made no structural progress"));
        return;
    }
    ++index;
    schedule();
}

void MaintenanceOperation::finish(Status status) noexcept
{
    if (finished.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (activity_slot != PublicationCoordinator::kMaintenanceSlots) {
        factory->publisher.finish_maintenance(activity_slot);
        activity_slot = PublicationCoordinator::kMaintenanceSlots;
    }
    cold.reset();
    loader.reset();
    queued_resources.reset();
    active_resources.reset();
    if (action_metrics) {
        (void)action_metrics->finish(status.ok() ? TreeOutcome::Retry : TreeOutcome::Error, tree_metrics_now_ns());
    }
    try {
        dirty = factory->publisher.capture()->stats.dirty_pages;
    }
    catch (const std::bad_alloc &) {
        if (status.ok()) {
            status = Status::resource_exhausted();
        }
    }
    if (publication.metadata) {
        publication.metadata->seal();
    }
    if (publication.snapshot_metadata) {
        publication.snapshot_metadata->seal();
    }
    auto self = lifetime;
    lifetime.reset();
    factory->active.clear(std::memory_order_release);
    try {
        done(std::move(status), dirty);
    }
    catch (...) {
        CRB_LOG_ERROR("maintenance receiver threw after terminal; operation "
                      "ownership was released");
    }
}
} // namespace crowdb::tree::detail

namespace crowdb::tree
{
FlushBatch::Maintenance make_page_maintenance(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                                              std::shared_ptr<detail::FlushRuntime::Lane> lane,
                                              PageMaintenanceOperationOptions             options)
{
    return make_page_maintenance(publisher, runtime, std::move(lane), std::move(options), {}, {});
}

FlushBatch::Maintenance make_page_maintenance(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                                              std::shared_ptr<detail::FlushRuntime::Lane> lane,
                                              PageMaintenanceOperationOptions             options,
                                              std::shared_ptr<PageLoader>                 loader,
                                              std::shared_ptr<ApplyDispatcher>            progress)
{
    auto prepaid = options.prepaid;
    if (prepaid && prepaid->category() != detail::BudgetClass::kReclaim) {
        throw std::invalid_argument("maintenance prepaid credit must use the reclaim partition");
    }
    auto factory = detail::reserved_shared<detail::MaintenanceFactory>(
        prepaid, publisher, runtime, std::move(lane), std::move(options), std::move(loader), std::move(progress));
    return [factory](auto view, auto done) {
        if (!view) {
            done(Status::invalid_argument("maintenance needs a captured view"), 0);
            return;
        }
        if (factory->active.test_and_set(std::memory_order_acquire)) {
            done(Status::unavailable("maintenance already active"), view->stats.dirty_pages);
            return;
        }
        auto                                          credit = std::exchange(factory->options.prepaid, {});
        Status                                        status;
        std::shared_ptr<detail::MaintenanceOperation> operation;
        try {
            if (!credit && factory->options.acquire_window) {
                status = factory->options.acquire_window(view->mapping, &factory->options.publication, &credit);
            }
            else if (!credit) {
                if (factory->options.acquire_snapshot) {
                    status = factory->options.acquire_snapshot(&factory->options.publication.snapshot_metadata);
                }
                if (status.ok()) {
                    status = factory->options.acquire ? factory->options.acquire(&credit)
                                                      : Status::invalid_argument("maintenance needs reclaim credit");
                }
            }
            if (status.ok() && credit && credit->category() != detail::BudgetClass::kReclaim) {
                status = Status::invalid_argument("maintenance credit must use the reclaim partition");
            }
            if (status.ok() && credit && credit->available() != 0) {
                operation = detail::reserved_shared<detail::MaintenanceOperation>(
                    credit, factory, view->generation, view->stats.dirty_pages, done, credit);
                if (operation->activity_slot == PublicationCoordinator::kMaintenanceSlots) {
                    operation.reset();
                }
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (const std::exception &) {
            status = Status::internal_error("maintenance credit acquisition failed");
        }
        if (!operation) {
            factory->loader.reset();
            if (factory->options.publication.snapshot_metadata) {
                factory->options.publication.snapshot_metadata->seal();
            }
            if (credit && credit->category() == detail::BudgetClass::kReclaim) {
                credit->seal();
            }
            factory->active.clear(std::memory_order_release);
            done(status.ok() || status.code() == Code::kResourceExhausted || status.code() == Code::kUnavailable
                     ? Status::Ok()
                     : std::move(status),
                 view->stats.dirty_pages);
            return;
        }
        operation->lifetime = operation;
        view.reset();
        operation->schedule();
    };
}
} // namespace crowdb::tree
