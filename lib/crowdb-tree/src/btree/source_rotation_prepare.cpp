// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "source_rotation.h"

#include <algorithm>

namespace crowdb::tree
{
void SourceRotation::prepare(const std::shared_ptr<const SourceCatalog> &catalog)
{
    const auto view = publisher_.capture();
    if (!view->sources) {
        finish();
        return;
    }
    const auto local_count =
        view->sources->inheritance ? view->sources->inheritance->local_count : view->sources->tables.size();
    if (local_count != catalog->tables.size() || view->sources->floor != catalog->floor ||
        view->flush_admission_owner != 0 || local_count > view->sources->tables.size() ||
        !std::equal(catalog->tables.begin(), catalog->tables.end(), view->sources->tables.begin())) {
        finish();
        return;
    }
    const auto bound =
        detail::AllocationBound(SourceAdmission::rotation_allocation_bound(catalog->tables.size()).value_or(SIZE_MAX))
            .add(PublicationCoordinator::empty_commit_reserve_bytes())
            .add(PublishedSources::allocation_bound())
            .add(detail::shared_allocation_bound<PublicationRequest>())
            .add(detail::shared_allocation_bound<InstallRecord>())
            .add(detail::shared_allocation_bound<detail::AllocationReservation>())
            .add_product(view->sources->tables.size() + 1, sizeof(std::shared_ptr<MemTable>))
            .value();
    auto credit = bound ? detail::ResourceBudget::try_reserve(account_, detail::BudgetClass::kAdmittedWriter, *bound)
                        : std::nullopt;
    if (!credit) {
        finish();
        return;
    }
    metadata_ = detail::AllocationReservation::create(std::move(*credit));
    auto next = detail::reserved_shared<PublishedSources>(metadata_, metadata_);
    next->tables.reserve(view->sources->tables.size() + 1);
    next->floor       = view->sources->floor;
    next->memory      = catalog->memory;
    next->inheritance = view->sources->inheritance;
    if (next->inheritance) {
        ++next->inheritance->local_count;
    }
    auto request                = detail::reserved_shared<PublicationRequest>(metadata_, metadata_);
    request->transaction.record = detail::reserved_shared<InstallRecord>(
        metadata_, InstallToken{.generation = view->generation, .batch = target_, .range = 0, .sequence = 0});
    auto status = sources_->prepare_rotation(metadata_, catalog, &rotation_);
    if (!status.ok()) {
        finish();
        return;
    }
    next->tables.insert(next->tables.end(), rotation_->next()->tables.begin(), rotation_->next()->tables.end());
    next->tables.insert(next->tables.end(), view->sources->tables.begin() + static_cast<ptrdiff_t>(local_count),
                        view->sources->tables.end());
    request->transaction.expected_sources  = view->sources;
    request->transaction.sources           = std::move(next);
    request->transaction.expected_coverage = catalog->floor;
    request->on_done                       = [self = shared_from_this()](PublicationResult result) noexcept {
        self->committed_ = result.status.ok();
        self->phase_.store(Phase::Acknowledge, std::memory_order_release);
        self->schedule();
    };
    phase_.store(Phase::Publishing, std::memory_order_release);
    if (!publisher_.try_submit(request)) {
        committed_ = false;
        phase_.store(Phase::Acknowledge, std::memory_order_release);
        schedule();
    }
}

} // namespace crowdb::tree
