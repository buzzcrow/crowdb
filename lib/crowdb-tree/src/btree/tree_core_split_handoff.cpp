// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_split_wait.h"
#include "snapshot/operation_wait.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
SourceWriteOptions combined_limits(SourceWriteOptions local, const SourceWriteOptions &source)
{
    local.max_entries       = std::max(local.max_entries, source.max_entries);
    local.max_key_bytes     = std::max(local.max_key_bytes, source.max_key_bytes);
    local.max_gap_entries   = std::max(local.max_gap_entries, source.max_gap_entries);
    local.max_gap_key_bytes = std::max(local.max_gap_key_bytes, source.max_gap_key_bytes);
    local.max_key_size      = std::max(local.max_key_size, source.max_key_size);
    return local;
}
} // namespace

Status TreeCore::State::prepare_split_handoff(const SplitSourceSession &session, uint64_t identity, uint64_t end)
{
    if (split_handoff) {
        return split_handoff->source_identity == identity && split_handoff->session == session.session &&
                       split_handoff->frontier == end
                 ? Status::Ok()
                 : Status::invalid_argument("another split handoff requires completion");
    }
    const auto view = publisher->capture();
    if (!view->sources->inheritance || !session.covers(view->coverage, end)) {
        return Status::unavailable("split handoff requires its retained source prefix");
    }
    const auto  locals    = sources->catalog();
    const auto &inherited = *view->sources->inheritance;
    if (inherited.owner_identity != identity || inherited.session != session.session || inherited.frontier != end) {
        return Status::invalid_argument("split handoff does not match installed inheritance");
    }
    const auto scratch = detail::AllocationBound(detail::shared_allocation_bound<detail::AllocationReservation>())
                             .add(detail::shared_allocation_bound<SourceWriteBudget>())
                             .add_product(view->sources->tables.size(), sizeof(std::shared_ptr<MemTable>))
                             .value();
    if (!scratch) {
        return Status::resource_exhausted("split reserve catalog bound overflow");
    }
    auto reserved = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, *scratch);
    if (!reserved) {
        return Status::unavailable("split reserve catalog admission unavailable");
    }
    auto          control = detail::AllocationReservation::create(std::move(*reserved));
    SourceCatalog catalog(control);
    catalog.tables.assign(view->sources->tables.begin(), view->sources->tables.end());
    catalog.floor         = view->coverage;
    const auto limits     = combined_limits(locals->active_budget->options(), session.limits);
    catalog.active_budget = detail::reserved_shared<SourceWriteBudget>(control, limits);
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = lifetime->acquire(binding->lifecycle_budget(), &ticket);
    if (!status.ok()) {
        return status;
    }
    auto prepare        = preparation;
    prepare.store       = ticket->backend(backend.get());
    const auto controls = detail::AllocationBound(detail::shared_allocation_bound<SplitHandoff>())
                              .add(SourceAdmission::coverage_allocation_bound(locals->tables.size()))
                              .add_product(2, OwnedKey::allocation_overhead())
                              .add_product(2, limits.max_key_size + 1)
                              .value();
    if (!controls) {
        return Status::resource_exhausted("split handoff control bound overflow");
    }
    FlushBatchOptions options;
    options.parallelism =
        std::max<size_t>(1, std::min(binding->cpu()->worker_count(), binding->options().limits.io_operations));
    options.run_limits.max_entries     = 128;
    options.run_limits.max_key_bytes   = std::max<size_t>(256 * 1024, limits.max_key_size);
    options.run_limits.key_block_bytes = 1024;
    FlushBatch::Maintenance   maintenance;
    detail::FlushReservations reservations;
    status = detail::prepare_flush_reservations(*publisher, *view, catalog, prepare, *controls, binding->lane(),
                                                &options, &maintenance, &reservations);
    if (!status.ok()) {
        return status;
    }
    auto handoff             = detail::reserved_shared<SplitHandoff>(reservations.handoff);
    handoff->reservations    = std::move(reservations);
    handoff->source_identity = identity;
    handoff->session         = session.session;
    handoff->frontier        = end;
    const auto &metadata     = handoff->reservations.handoff;
    status                   = sources->prepare_coverage(end, metadata, &handoff->coverage);
    if (!status.ok()) {
        return status;
    }
    if (config.key_range.start()) {
        options.selection.lower.emplace(Slice(*config.key_range.start()), metadata);
    }
    if (config.key_range.end()) {
        options.selection.upper.emplace(Slice(*config.key_range.end()), metadata);
    }
    auto inputs   = detail::reserved_shared<PublishedSources>(metadata, metadata);
    inputs->floor = view->coverage;
    inputs->tables.assign(view->sources->tables.begin() + static_cast<std::ptrdiff_t>(inherited.local_count),
                          view->sources->tables.end());
    handoff->maintenance = std::move(maintenance);
    auto acknowledge     = [source = sources.get(), weak = std::weak_ptr<SplitHandoff>(handoff)](auto published,
                                                                                                 auto done) {
        auto current = weak.lock();
        if (!current) {
            done(Status::unavailable("split handoff owner released"), 0);
            return;
        }
        auto result = source->install_coverage(current->coverage, *published);
        if (!result.ok()) {
            done(std::move(result), published->stats.dirty_pages);
            return;
        }
        if (current->maintenance) {
            current->maintenance(std::move(published), std::move(done));
        }
        else {
            done(Status::Ok(), published->stats.dirty_pages);
        }
    };
    status = FlushBatch::prepare(*publisher, *binding->cpu(), binding->lane(), session.session, end, std::move(options),
                                 std::move(acknowledge), &handoff->operation, std::move(inputs), metadata);
    if (!status.ok()) {
        return status;
    }
    split_handoff = std::move(handoff);
    return Status::Ok();
}

Status TreeCore::publish_split_view(uint64_t generation, uint64_t journal_frontier, TreeCore &destination,
                                    const KeyRange &range)
try {
    if (&destination == this || config().runtime != destination.config().runtime || !range.validate().ok() ||
        range.start() != destination.config().key_range.start() ||
        range.end() != destination.config().key_range.end()) {
        return Status::invalid_argument("split publication must cover the destination's complete key range");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("split source ingress closed");
    }
    detail::SplitGate source_gate(state_->split_busy);
    detail::SplitGate destination_gate(destination.state_->split_busy);
    if (!source_gate.held() || !destination_gate.held()) {
        return Status::unavailable("split administration is busy");
    }
    const auto session = state_->split_sources;
    if (!session || !session->active || generation == 0 || generation != session->session ||
        journal_frontier > session->frontier) {
        return Status::invalid_argument("split source generation or frontier is not active");
    }
    // Keep the source session fixed across the destination's short ingress fence.
    auto status = destination.install_split_overlay_guarded(*this, journal_frontier);
    if (!status.ok()) {
        return status;
    }
    State::Ingress destination_ingress(*destination.state_);
    if (destination_ingress.owner == nullptr) {
        return Status::unavailable("split destination ingress closed");
    }
    if (!destination.capture()->sources->inheritance && destination.capture()->coverage == journal_frontier &&
        !destination.state_->split_handoff) {
        return Status::Ok();
    }
    status = destination.state_->prepare_split_handoff(*session, state_->sources->identity(), journal_frontier);
    if (!status.ok()) {
        return status;
    }
    const auto handoff = destination.state_->split_handoff;
    if (!handoff->operation) {
        return Status::Ok();
    }
    status = detail::wait_snapshot_operation(handoff->operation);
    if (!status.ok()) {
        return status;
    }
    const auto &report = handoff->operation->result();
    status             = report.handoff_status.ok() ? report.maintenance_status : report.handoff_status;
    if (status.ok()) {
        handoff->operation.reset();
        handoff->coverage.reset();
        handoff->maintenance = {};
        handoff->reservations.seal();
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("split handoff allocation failed");
}
} // namespace crowdb::tree
