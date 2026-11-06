// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/runtime_range_rebuild.h"
#include "btree/tree_core_internal.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
namespace
{
Status inherit_sources(const SplitSourceSession &session, uint64_t identity, uint64_t end,
                       const std::shared_ptr<TreeRuntimeBinding> &binding, std::shared_ptr<const PublishedSources> *out)
{
    const auto floor = session.baseline->view()->coverage;
    if (floor == end) {
        return Status::Ok();
    }
    const auto bytes = detail::AllocationBound(PublishedSources::allocation_bound())
                           .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add_product(session.table_count, sizeof(std::shared_ptr<MemTable>))
                           .value();
    if (!bytes) {
        return Status::resource_exhausted("split baseline source bound overflow");
    }
    auto paid = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kHandoff, *bytes);
    if (!paid) {
        return Status::resource_exhausted("split baseline source admission unavailable");
    }
    auto metadata  = detail::AllocationReservation::create(std::move(*paid));
    auto sources   = detail::reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor = floor;
    sources->tables.reserve(session.table_count);
    sources->inheritance = PublishedSources::Inheritance{
        .owner_identity = identity, .session = session.session, .frontier = end, .local_count = 0};
    sources->inheritance->memory.known = true;
    for (size_t i = 0; i < session.table_count; ++i) {
        sources->tables.push_back(session.tables[i]);
        sources->inheritance->memory += session.tables[i]->source_stats();
    }
    metadata->seal();
    *out = std::move(sources);
    return Status::Ok();
}
} // namespace

Status TreeCore::install_split_baseline(const SplitSourceSession &session, uint64_t identity, uint64_t end)
{
    SplitGeneration split;
    {
        State::Ingress ingress(*state_);
        if (ingress.owner == nullptr) {
            return Status::unavailable("split destination ingress closed");
        }
        split.expected   = capture();
        const auto &view = *split.expected;
        const auto  root = view.mapping.get(view.root_page_id);
        if (view.coverage != 0 || max_seen_slot() != 0 || view.stats.page_count() != 1 || !root || !root->leaf ||
            !root->leaf->bases.empty() || !root->leaf->overlays.empty() || view.sources->inheritance ||
            view.pending_handoff || view.flush_admission_owner != 0 || view.checkpoint_inflight) {
            return Status::invalid_argument("split L1 baseline requires an empty unwritten destination");
        }
        const auto floor = session.baseline->view()->coverage;
        if (end < floor || !session.covers(floor, end)) {
            return Status::invalid_argument("split sources do not cover the fixed L1 baseline");
        }
        if (end != floor && session.table_count >= read_limits().max_memtables) {
            return Status::resource_exhausted("split source catalog exceeds the destination read window");
        }
    }
    RuntimeRangeImport staging;
    auto               status = copy_snapshot_range(session.baseline, *this, config().key_range, staging);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<const PublishedTreeState> candidate;
    status = prepare_readable(std::move(staging.catalog), staging.credit, &candidate);
    if (!status.ok()) {
        return status;
    }
    status = inherit_sources(session, identity, end, binding(), &split.inherited);
    if (!status.ok()) {
        return status;
    }
    // No destination mutation has occurred. Generation replacement validates
    // the original root again under its ingress fence before one atomic install
    // of the independent L1 and the complete inherited source prefix.
    return install_generation(std::move(candidate), &split);
}
} // namespace crowdb::tree
