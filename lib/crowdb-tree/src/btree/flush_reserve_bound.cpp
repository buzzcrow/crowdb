// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_reservation_internal.h"
#include "page_cache.h"

#include <algorithm>

namespace crowdb::tree::detail
{
Status flush_reserve_bounds(PublicationCoordinator &publisher, const PublishedTreeState &view,
                            const SourceCatalog &sources, const FlushPreparationOptions &preparation,
                            size_t control_bytes, const FlushBatchOptions &batch, FlushReserveBounds *out,
                            std::optional<WriteFootprint> stable_active, bool add_successor)
{
    if (!preparation.runtime || !preparation.store || !sources.active_budget || out == nullptr || batch.loader ||
        preparation.maintenance.prepaid || preparation.maintenance.acquire ||
        preparation.maintenance.acquire_snapshot || preparation.maintenance.acquire_window ||
        preparation.maintenance.publication.metadata) {
        return Status::invalid_argument("automatic flush requires owned runtime/store and unbound progress "
                                        "consumers");
    }
    if (sources.floor != view.coverage || !view.sources || view.sources->floor != sources.floor ||
        sources.tables.size() != view.sources->tables.size() ||
        !std::equal(sources.tables.begin(), sources.tables.end(), view.sources->tables.begin())) {
        return Status::unavailable("source catalog differs from the reserved mapping view");
    }
    const auto root = view.mapping.get(view.root_page_id);
    if (!root || root->level > batch.max_route_depth) {
        return Status::invalid_argument("automatic flush needs an initialized bounded routing tree");
    }
    const auto &source_limits = sources.active_budget->options();
    const auto  per_source_entries =
        AllocationBound(source_limits.max_entries).add(source_limits.max_gap_entries).value();
    const auto per_source_keys =
        AllocationBound(source_limits.max_key_bytes).add(source_limits.max_gap_key_bytes).value();
    if (!per_source_entries || !per_source_keys) {
        return Status::resource_exhausted("source admission bound overflow");
    }
    AllocationBound entry_bound;
    AllocationBound key_bound;
    for (const auto &table : sources.tables) {
        if (!table) {
            return Status::invalid_argument("source catalog contains an absent table");
        }
        // Closed, drained sources cannot grow. Each linked key contributes at
        // most one frontier-selected version; node storage includes its key.
        // An accepted writer still requires the full configured allowance.
        const auto summary = table->source_stats();
        const auto sealed  = table->sealed_admission_bound();
        if (stable_active && table == sources.active) {
            entry_bound.add(stable_active->entries);
            key_bound.add(stable_active->key_bytes);
        }
        else if (summary.known && summary.frozen_tables != 0 && summary.writers == 0) {
            entry_bound.add(summary.records);
            key_bound.add(summary.memory.node_bytes);
        }
        else if (sealed) {
            entry_bound.add(sealed->entries);
            key_bound.add(sealed->key_bytes);
        }
        else {
            entry_bound.add(*per_source_entries);
            key_bound.add(*per_source_keys);
        }
    }
    // Rotation seals the current source and installs one still-open successor.
    // The latter keeps its complete future ordinary and gap allowance.
    const auto source_count = sources.tables.size() + (add_successor ? 1 : 0);
    if (add_successor) {
        entry_bound.add(*per_source_entries);
        key_bound.add(*per_source_keys);
    }
    const auto entries = entry_bound.value();
    const auto keys    = key_bound.value();
    if (!entries || !keys) {
        return Status::resource_exhausted("captured source bound overflow");
    }
    const auto workers = preparation.runtime->worker_count();
    auto handoff = AllocationBound(control_bytes)
                       .add(FlushBatch::metadata_reserve_bound(view.mapping, source_count, *entries, *keys, workers,
                                                               batch, publisher.tracks_cache_residency()))
                       .add(SourceAdmission::capture_allocation_bound(source_count))
                       .add_product(2, PublishedSources::allocation_bound())
                       .add_product(source_count, 2 * sizeof(std::shared_ptr<MemTable>))
                       .add(2 * sizeof(std::shared_ptr<MemTable>))
                       .add_product(4, shared_allocation_bound<AllocationReservation>())
                       .add(shared_allocation_bound<ReclaimAdmission>())
                       .add(preparation.cache ? SharedPageCache::binding_allocation_bound() : 0)
                       .add(preparation.cache ? preparation.cache->load_reserve_bound(root->level, workers)
                                              : std::optional<size_t>{0})
                       .value();
    // Handoff adds overlays without growing the captured base arrays. The
    // Analyze may split bases and add resident inners. Inner underflow repair
    // retains one captured route and both adjacent siblings at each level.
    // Maintenance-created inners already own resident routes.
    // Concurrent checkpoint completion can replace a leaf with one
    // cold base, so an initially empty array still needs that single allowance.
    const auto cold_bases = std::min(preparation.maintenance.publication.pages.max_sources,
                                     std::max(size_t{1}, view.mapping.copy_bounds().max_leaf_bases));
    const auto path_depth =
        std::min(static_cast<size_t>(root->level), preparation.maintenance.publication.max_path_pages);
    const auto cold_depth =
        path_depth +
        (root->level < 2 ? 0 : static_cast<size_t>(std::min<uint64_t>(view.stats.inner_pages, 3 * path_depth)));
    // Only captured inner pages can become cold cache entries during this
    // operation. Maintenance-created inner pages already own resident routes.
    const auto routing =
        AllocationBound()
            .add(AsyncStorePageLoader::routing_reserve_bound(
                preparation.loads, root->level, workers,
                preparation.cache ? std::min<uint64_t>(preparation.cache->retained_page_limit(), view.stats.inner_pages)
                                  : 0))
            .add(AsyncStorePageLoader::primary_reserve_bound(preparation.loads, cold_depth, 1, cold_bases))
            .value();
    const auto backend = backend_operation_bound(
        preparation.store.get(),
        {.max_io_bytes = preparation.loads.max_blob_bytes, .write_calls = 0, .workers = workers, .barriers = false});
    auto maintenance = preparation.maintenance;
    if (routing && backend) {
        const auto account       = sources.active_budget->options().account;
        const auto reclaim_limit = add_successor ? ResourceBudget::capacity(account, BudgetClass::kReclaim)
                                                 : ResourceBudget::available(account, BudgetClass::kReclaim);
        const auto routing_limit = add_successor ? ResourceBudget::capacity(account, BudgetClass::kRouting)
                                                 : ResourceBudget::available(account, BudgetClass::kRouting);
        const auto fixed         = AllocationBound(*routing).add(*backend).value();
        limit_page_maintenance_window(view.mapping, *entries, reclaim_limit,
                                      fixed && *fixed < routing_limit ? routing_limit - *fixed : 0,
                                      publisher.tracks_cache_residency(), &maintenance);
    }
    const auto reclaim =
        page_maintenance_reserve_bound(view.mapping, *entries, maintenance, publisher.tracks_cache_residency(), true);
    const auto snapshot = page_maintenance_snapshot_reserve_bound(view.mapping, maintenance);
    const auto total_routing =
        routing && backend && snapshot ? AllocationBound(*routing).add(*backend).add(*snapshot).value() : std::nullopt;
    if (!handoff || !total_routing || !reclaim || !snapshot) {
        return Status::resource_exhausted("flush reserve bounds exceed the configured work limits");
    }
    *out = {
        .amounts                  = {*handoff, *total_routing, *reclaim, size_t{1}},
        .backend_bytes            = *backend,
        .snapshot_bytes           = *snapshot,
        .maintenance_output_pages = maintenance.publication.pages.max_output_pages
    };
    return Status::Ok();
}
} // namespace crowdb::tree::detail
