// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/read_value_bound.h"
#include "btree/tree_core_read_internal.h"

#include <algorithm>

namespace crowdb::tree::core_read_detail
{
Status prepare(TreeCore &core, bool scan, size_t control_bytes, size_t result_bytes, ReadPreparation *out)
{
    return prepare(core, core.capture(), core.read_limits(), scan, control_bytes, result_bytes, out);
}

Status prepare(TreeCore &core, std::shared_ptr<const PublishedTreeState> view, PublishedReadLimits limits, bool scan,
               size_t control_bytes, size_t result_bytes, ReadPreparation *out)
{
    using namespace detail;
    ReadPreparation ready;
    ready.binding   = core.binding();
    ready.view      = std::move(view);
    ready.limits    = std::move(limits);
    const auto root = ready.view->mapping.get(ready.view->root_page_id);
    if (core.closed() || !root || root->level >= ready.limits.max_route_depth) {
        return Status::unavailable("read requires an open bounded published root");
    }
    if (ready.view->sources->tables.size() > ready.limits.max_memtables) {
        return Status::resource_exhausted("captured read exceeds the source count limit");
    }
    ready.limits.max_route_depth     = root->level + 1ULL;
    ready.limits.max_memtables       = std::max<size_t>(1, ready.view->sources->tables.size());
    auto loads                       = core.load_limits();
    loads.max_inflight               = 1;
    const size_t workers             = ready.binding->cpu()->worker_count();
    const auto   shape               = ready.view->mapping.copy_bounds();
    ready.limits.page_metadata_bytes = shape.max_leaf_bytes;
    const auto captured_bases =
        AllocationBound().add_product(shape.leaf_pages, shape.max_leaf_bases).value().value_or(SIZE_MAX);
    const auto leaf_window = AllocationBound(shape.max_leaf_bases).add(scan ? ready.limits.max_entries : 0).value();
    if (!leaf_window) {
        return Status::resource_exhausted("captured read frame window overflow");
    }
    const size_t leaves       = std::min(*leaf_window, scan ? captured_bases : shape.max_leaf_bases);
    const size_t values       = scan ? ready.limits.max_entries : 1;
    const size_t cached_pages = core.cache()->retained_page_limit();
    // One point lookup visits one routing path and one logical leaf. A scan
    // can retain more decoded pages, but cannot visit absent index PIDs.
    const size_t cached_inner = std::min<uint64_t>(cached_pages, scan ? ready.view->stats.inner_pages : root->level);
    const size_t cached_leaf  = std::min(cached_pages, scan ? captured_bases : shape.max_leaf_bases);
    // Only fixed L1 cells can require an overflow allocation. L0 cells lend
    // their independent payload owners, including writes after this capture.
    const auto value_limit = overflow_value_bytes_bound(*ready.view, loads.max_value_bytes);
    const auto value_bytes =
        AllocationBound(value_limit).add(scan && value_limit != 0 ? ready.limits.max_result_bytes : 0).value();
    auto bounded_loads = loads;
    // Every accepted overflow frame contributes at least one value byte. This
    // tightens reservation only; the loader's public validation caps stay fixed.
    bounded_loads.max_overflow_pages = std::min<uint64_t>(loads.max_overflow_pages, value_limit);
    auto loader_bound =
        AsyncStorePageLoader::primary_reserve_bound(loads, root->level, workers, leaves, cached_leaf, cached_inner);
    if (value_limit != 0) {
        loader_bound = value_bytes
                         ? AsyncStorePageLoader::read_reserve_bound(bounded_loads, root->level, workers, leaves, values,
                                                                    *value_bytes, cached_leaf, cached_inner)
                         : std::nullopt;
    }
    const auto bound = AllocationBound(control_bytes)
                           .add_product(2, shared_allocation_bound<AllocationReservation>())
                           .add(TreeOperationLifetime::ticket_allocation_bound())
                           .add(scan ? PublishedScan::allocation_bound(ready.limits, workers)
                                     : PublishedGet::allocation_bound(ready.limits, workers))
                           .add(loader_bound)
                           .add(core.cache()->load_reserve_bound(root->level + leaves, workers))
                           .add(SharedPageCache::binding_allocation_bound())
                           .add(result_bytes)
                           .value();
    if (!bound) {
        return Status::resource_exhausted("read admission bound overflow");
    }
    auto bytes = ResourceBudget::try_reserve(ready.binding->account(), BudgetClass::kRouting, *bound);
    auto io    = ResourceBudget::try_reserve(ready.binding->account(), BudgetClass::kIo, 1);
    if (!bytes) {
        return Status::resource_exhausted("read Routing reserve unavailable: requested=" + std::to_string(*bound));
    }
    if (!io) {
        return Status::resource_exhausted("read I/O reserve unavailable");
    }
    ready.credit          = AllocationReservation::create(std::move(*bytes));
    ready.io              = reserved_shared<AllocationReservation>(ready.credit, std::move(*io));
    ready.limits.metadata = ready.credit;
    auto backend_status   = reserve_backend_operation(
        core.async_backend(), ready.binding->account(),
        {.max_io_bytes = loads.max_blob_bytes, .write_calls = 0, .workers = workers, .barriers = false},
        &ready.backend);
    if (!backend_status.ok()) {
        return backend_status;
    }
    auto status = core.operation_lifetime().acquire(ready.credit, &ready.ticket);
    if (!status.ok()) {
        return status;
    }
    auto loader = reserved_shared<AsyncStorePageLoader>(ready.credit, ready.ticket->backend(core.async_backend()),
                                                        ready.binding->cpu(), ready.binding->lane(), loads,
                                                        OverflowBackingResolver{},
                                                        PageLoadBudget{.bytes        = ready.credit,
                                                                       .io           = ready.io,
                                                                       .work_metrics = core.publisher().work_metrics(),
                                                                       .backend      = ready.backend,
                                                                       .cpu_progress = core.cpu_progress(),
                                                                       .cause        = TreeWorkCause::Read});
    status      = core.cache()->bind_reserved(std::move(loader), ready.credit, &ready.loader);
    if (status.ok()) {
        *out = std::move(ready);
    }
    return status;
}
} // namespace crowdb::tree::core_read_detail
