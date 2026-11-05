// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_controller_internal.h"
#include "flush_operation_internal.h"

#include <algorithm>

namespace crowdb::tree
{
Status FlushController::State::prepare_covered(const FlushPreparationOptions                   &preparation,
                                               const std::shared_ptr<const PublishedTreeState> &view,
                                               const std::shared_ptr<const SourceCatalog>      &catalog,
                                               std::shared_ptr<FlushOperation>                 *out)
try {
    if (preparation.persistent || !preparation.store || !catalog->active_budget || options.loader ||
        preparation.maintenance.prepaid || preparation.maintenance.acquire ||
        preparation.maintenance.publication.metadata || !view->sources || view->sources->inheritance ||
        view->pending_handoff || view->flush_admission_owner != 0 || catalog->floor != view->coverage ||
        view->sources->floor != catalog->floor || view->sources->tables.size() != catalog->tables.size() ||
        !std::equal(view->sources->tables.begin(), view->sources->tables.end(), catalog->tables.begin()) ||
        contiguous.load(std::memory_order_acquire) > catalog->floor) {
        return Status::Ok();
    }
    // This load fixes an already covered prefix. Writers completing after it
    // remain in the current source and belong to a subsequent flush.
    constexpr auto bytes = detail::shared_allocation_bound<FlushOperation::State>() +
                           detail::shared_allocation_bound<FlushOperation>() +
                           detail::shared_allocation_bound<detail::AllocationReservation>();
    auto           paid  = detail::ResourceBudget::try_reserve(catalog->active_budget->options().account,
                                                               detail::BudgetClass::kHandoff, bytes);
    if (!paid) {
        return Status::unavailable("covered flush control reserve unavailable");
    }
    auto metadata       = detail::AllocationReservation::create(std::move(*paid));
    auto bounds         = options;
    bounds.cpu_progress = preparation.cpu_progress;
    bounds.publication  = preparation.publication;
    auto operation = detail::reserved_shared<FlushOperation::State>(metadata, publisher, runtime, sources, contiguous,
                                                                    lane, std::move(bounds), FlushBatch::Maintenance{});
    operation->progress                  = progress;
    operation->metadata                  = metadata;
    operation->frozen.base               = view;
    operation->frozen.frontier           = catalog->floor;
    operation->report.floor              = catalog->floor;
    operation->report.frontier           = catalog->floor;
    operation->report.published_coverage = catalog->floor;
    operation->report.handoff_status     = Status::Ok();
    operation->report.maintenance_status = Status::Ok();
    operation->report.deferred_pages     = view->stats.dirty_pages;
    operation->report.phase              = FlushPhase::kComplete;
    operation->complete_success.store(true, std::memory_order_release);
    auto result = detail::reserved_construct<FlushOperation>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) FlushOperation(std::move(operation)); });
    metadata->seal();
    pending = std::move(result);
    *out    = pending;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("covered flush control allocation failed");
}
} // namespace crowdb::tree
