// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_split_operation.h"
#include "btree/tree_core_checkpoint_internal.h"

namespace crowdb::tree::checkpoint_detail
{
Status Preparation::structure(std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                              std::shared_ptr<PageSplitOperation> *out)
try {
    using namespace detail;
    auto limits           = structure_options;
    limits.max_candidates = limits.max_visits = limits.visits_per_step = limits.max_steps = 1;
    limits.publication.inner_merge_keys                                                   = 0;
    limits.publication.pages.gc_floor                                                     = view->gc_floor;
    const auto root = view->mapping.get(view->root_page_id);
    const auto page = view->mapping.get(pid);
    if (!root || !page || !page->leaf) {
        return Status::unavailable("checkpoint split target changed");
    }
    auto loads         = core->load_limits();
    loads.max_inflight = loads.max_overflow_pages = 1;
    const auto workers                            = core->binding()->cpu()->worker_count();
    const auto control_bytes =
        PageSplitOperation::control_allocation_bound() + shared_allocation_bound<AllocationReservation>();
    const auto routing = AllocationBound()
                             .add(AsyncStorePageLoader::read_reserve_bound(loads, root->level + 1ULL, workers,
                                                                           page->leaf->bases.size(), 0, 0))
                             .add(shared_allocation_bound<AllocationReservation>())
                             .value();
    const BackendOperationLimits backend_limits{
        .max_io_bytes = loads.max_blob_bytes, .write_calls = 0, .workers = workers, .barriers = false};
    const auto backend_bytes = backend_operation_bound(core->async_backend(), backend_limits);
    auto       account       = core->binding()->account();
    if (routing && backend_bytes) {
        const auto reclaim_available = ResourceBudget::available(account, BudgetClass::kReclaim);
        const auto routing_available = ResourceBudget::available(account, BudgetClass::kRouting);
        const auto fixed_routing     = AllocationBound(*routing).add(*backend_bytes).value();
        limit_page_maintenance_window(
            view->mapping, 0, reclaim_available > control_bytes ? reclaim_available - control_bytes : 0,
            fixed_routing && *fixed_routing < routing_available ? routing_available - *fixed_routing : 0,
            core->publisher().tracks_cache_residency(), &limits);
    }
    const auto maintenance =
        page_maintenance_reserve_bound(view->mapping, 0, limits, core->publisher().tracks_cache_residency(), true);
    const auto snapshot_bytes = page_maintenance_snapshot_reserve_bound(view->mapping, limits);
    const auto reclaim        = AllocationBound(maintenance.value_or(SIZE_MAX)).add(control_bytes).value();
    if (!reclaim || !routing || !backend_bytes || !snapshot_bytes) {
        return Status::resource_exhausted("checkpoint split window bound overflow");
    }
    auto backend  = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *backend_bytes);
    auto read     = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *routing);
    auto snapshot = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *snapshot_bytes);
    auto bytes    = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, *reclaim);
    auto io       = ResourceBudget::try_reserve(account, BudgetClass::kIo, 1);
    if (!backend || !read || !snapshot || !bytes || !io) {
        return Status::resource_exhausted("checkpoint split window credit unavailable");
    }
    auto phase                           = reserved_shared<Credits>(control);
    phase->snapshot                      = AllocationReservation::create(std::move(*snapshot));
    limits.publication.snapshot_metadata = phase->snapshot;
    phase->budget.bytes                  = AllocationReservation::create(std::move(*read));
    phase->budget.io           = reserved_shared<AllocationReservation>(phase->budget.reservation(), std::move(*io));
    phase->budget.work_metrics = core->publisher().work_metrics();
    phase->budget.cause        = options.cause;
    phase->budget.cpu_progress = cpu_progress;
    auto status =
        bind_backend_operation(core->async_backend(), backend_limits, std::move(*backend), &phase->budget.backend);
    if (!status.ok()) {
        return status;
    }
    auto loader = reserved_shared<AsyncStorePageLoader>(
        phase->budget.reservation(), ticket->backend(core->async_backend()), core->binding()->cpu(),
        core->binding()->lane(), loads, OverflowBackingResolver{}, phase->budget);
    limits.prepaid = AllocationReservation::create(std::move(*bytes));
    return PageSplitOperation::create(core->publisher(), *core->binding()->cpu(), core->binding()->lane(),
                                      std::move(view), pid, std::move(limits), std::move(loader), cpu_progress,
                                      std::move(phase), options.cause, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("checkpoint split allocation failed");
}
} // namespace crowdb::tree::checkpoint_detail
