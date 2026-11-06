// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_reservation_internal.h"
#include "page_cache.h"
#include "publication_lane.h"
#include "snapshot/flush_persistence.h"

namespace crowdb::tree::detail
{
Status ReclaimAdmission::acquire_window(const MappingView &mapping, MaintenancePublicationOptions *publication,
                                        std::shared_ptr<AllocationReservation> *out) const
{
    if (publication == nullptr || out == nullptr || *out) {
        return Status::invalid_argument("maintenance window requires fresh outputs");
    }
    auto selected = options;
    limit_page_maintenance_window(mapping, 0, ResourceBudget::available(account, BudgetClass::kReclaim),
                                  ResourceBudget::available(account, BudgetClass::kRouting),
                                  publisher->tracks_cache_residency(), &selected);
    const auto snapshot = page_maintenance_snapshot_reserve_bound(mapping, selected);
    const auto reclaim =
        page_maintenance_reserve_bound(mapping, 0, selected, publisher->tracks_cache_residency(), true);
    if (!snapshot || !reclaim) {
        return Status::resource_exhausted("maintenance window reserve bound overflow");
    }
    auto snapshot_credit = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *snapshot);
    if (!snapshot_credit) {
        return Status::unavailable("maintenance snapshot reserve unavailable");
    }
    auto reclaim_credit = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, *reclaim);
    if (!reclaim_credit) {
        return Status::unavailable("maintenance reclaim reserve unavailable");
    }
    auto snapshot_owner                 = AllocationReservation::create(std::move(*snapshot_credit));
    auto reclaim_owner                  = AllocationReservation::create(std::move(*reclaim_credit));
    publication->pages.max_output_pages = selected.publication.pages.max_output_pages;
    publication->snapshot_metadata      = std::move(snapshot_owner);
    *out                                = std::move(reclaim_owner);
    return Status::Ok();
}

Status bind_flush_reservations(PublicationCoordinator &publisher, const FlushPreparationOptions &preparation,
                               const std::shared_ptr<ResourceBudget::Account> &account,
                               std::shared_ptr<FlushRuntime::Lane> lane, const FlushReserveBounds &bounds,
                               FlushCredits credits, FlushBatchOptions *batch, FlushBatch::Maintenance *maintenance,
                               FlushReservations *out)
{
    if (!preparation.runtime || !preparation.store || !lane || out == nullptr || !account || batch == nullptr ||
        batch->loader || maintenance == nullptr || *maintenance || bounds.snapshot_bytes == 0 ||
        bounds.maintenance_output_pages < 2 ||
        bounds.maintenance_output_pages > preparation.maintenance.publication.pages.max_output_pages) {
        return Status::invalid_argument("flush binding requires fresh consumers and owned progress grants");
    }
    if (preparation.publication && preparation.publication->capacity() <= batch->parallelism) {
        return Status::invalid_argument("flush publication window is smaller than the parallel handoff");
    }
    for (size_t index = 0; index < credits.size(); ++index) {
        if (!credits[index].belongs_to(account, FlushReserveBounds::categories[index]) || credits[index].allocated() ||
            credits[index].amount() < bounds.amounts[index]) {
            return Status::invalid_argument("flush binding requires each complete unused partition grant");
        }
    }
    auto backend_credit = credits[1].split(bounds.backend_bytes);
    if (!backend_credit) {
        return Status::invalid_argument("flush routing grant lacks backend capacity");
    }
    auto snapshot_credit = credits[1].split(bounds.snapshot_bytes);
    if (!snapshot_credit) {
        return Status::invalid_argument("flush routing grant lacks snapshot selection capacity");
    }
    FlushReservations reserved;
    reserved.cache      = preparation.cache;
    reserved.handoff    = AllocationReservation::create(std::move(credits[0]));
    reserved.routing    = reserved_shared<AllocationReservation>(reserved.handoff, std::move(credits[1]));
    reserved.reclaim    = reserved_shared<AllocationReservation>(reserved.handoff, std::move(credits[2]));
    reserved.snapshot   = AllocationReservation::create(std::move(*snapshot_credit));
    auto backend_status = bind_backend_operation(preparation.store.get(),
                                                 {.max_io_bytes = preparation.loads.max_blob_bytes,
                                                  .write_calls  = 0,
                                                  .workers      = preparation.runtime->worker_count(),
                                                  .barriers     = false},
                                                 std::move(*backend_credit), &reserved.backend);
    if (!backend_status.ok()) {
        return backend_status;
    }
    batch->cpu_progress = preparation.cpu_progress;
    batch->publication  = preparation.publication;
    batch->persistent   = preparation.persistent;
    batch->persistence  = preparation.persistence;
    if (preparation.persistent) {
        auto persistence = preparation.persistence;
        if (!persistence) {
            return Status::invalid_argument("persistent flush needs its durable sink");
        }
        // Binding runs under the existing flush preparation admission. A
        // request that waited or joined an older round must use this round's
        // current checkpoint token and durable baseline.
        persistence->configure(*batch);
        *maintenance = [persistence](auto view, auto done) { persistence->commit(std::move(view), std::move(done)); };
    }
    auto loads         = preparation.loads;
    loads.max_inflight = SIZE_MAX;
    batch->loader      = reserved_shared<AsyncStorePageLoader>(reserved.routing, preparation.store, preparation.runtime,
                                                               lane, loads, OverflowBackingResolver{},
                                                               PageLoadBudget{.bytes        = reserved.routing,
                                                                              .work_metrics = publisher.work_metrics(),
                                                                              .backend      = reserved.backend,
                                                                              .cpu_progress = preparation.cpu_progress,
                                                                              .cause        = TreeWorkCause::Flush});
    batch->consolidation_loader = batch->loader;
    if (preparation.cache) {
        batch->loader = preparation.cache->bind(batch->loader, reserved.handoff, PageLoadPriority::Maintenance);
    }
    batch->consolidation                         = preparation.maintenance.publication;
    batch->consolidation->pages.max_output_pages = bounds.maintenance_output_pages;
    batch->consolidation->metadata               = reserved.reclaim;
    batch->consolidation->snapshot_metadata      = reserved.snapshot;
    *out                                         = std::move(reserved);
    return Status::Ok();
}

Status prepare_flush_reservations(PublicationCoordinator &publisher, const PublishedTreeState &view,
                                  const SourceCatalog &sources, const FlushPreparationOptions &preparation,
                                  size_t control_bytes, std::shared_ptr<FlushRuntime::Lane> lane,
                                  FlushBatchOptions *batch, FlushBatch::Maintenance *maintenance,
                                  FlushReservations *out, std::optional<WriteFootprint> stable_active)
{
    if (!lane || batch == nullptr || maintenance == nullptr || *maintenance || out == nullptr) {
        return Status::invalid_argument("flush preparation requires fresh owned progress consumers");
    }
    FlushReserveBounds bounds;
    auto               status =
        flush_reserve_bounds(publisher, view, sources, preparation, control_bytes, *batch, &bounds, stable_active);
    if (!status.ok()) {
        return status;
    }
    FlushCredits         credits;
    constexpr std::array names{"handoff", "routing", "reclaim"};
    for (size_t index = 0; index < credits.size(); ++index) {
        const auto capacity =
            ResourceBudget::capacity(sources.active_budget->options().account, FlushReserveBounds::categories[index]);
        if (bounds.amounts[index] > capacity) {
            return Status::resource_exhausted(
                "flush " + std::string(names[index]) + " bound exceeds configured capacity: required " +
                std::to_string(bounds.amounts[index]) + " limit " + std::to_string(capacity));
        }
        auto credit = ResourceBudget::try_reserve(sources.active_budget->options().account,
                                                  FlushReserveBounds::categories[index], bounds.amounts[index]);
        if (!credit) {
            return Status::unavailable("flush " + std::string(names[index]) + " reserve unavailable: required " +
                                       std::to_string(bounds.amounts[index]));
        }
        credits[index] = std::move(*credit);
    }
    return bind_flush_reservations(publisher, preparation, sources.active_budget->options().account, std::move(lane),
                                   bounds, std::move(credits), batch, maintenance, out);
}
} // namespace crowdb::tree::detail
