// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache.h"
#include "publication_generation_internal.h"
#include "publication_snapshot.h"
#include "source_admission.h"

#include <cassert>

namespace crowdb::tree
{
std::optional<size_t> PublicationCoordinator::replacement_allocation_bound(const MappingView &mapping,
                                                                           size_t             sources) noexcept
{
    using detail::shared_allocation_bound;
    detail::AllocationBound bound(sizeof(Root) + shared_allocation_bound<Replacement>() +
                                  shared_allocation_bound<CheckpointLedger>() +
                                  shared_allocation_bound<PublishedTreeState>() + PublishedSources::allocation_bound());
    bound.add_product(sources, sizeof(std::shared_ptr<MemTable>));
    // Each pending member has a red-black node plus the resource charge header.
    size_t pages = 0;
    mapping.visit(&pages, [](void *raw, uint64_t, const auto &) { ++*static_cast<size_t *>(raw); });
    bound.add_product(pages, 512);
    bound.add(SnapshotChanges::initial_allocation_bound(pages));
    bound.add(PageCacheResidency::initial_allocation_bound(mapping));
    bound.add(merge_membership_reserve_bytes(mapping.copy_bounds().merge_candidate_pages));
    return bound.value();
}

Status PublicationCoordinator::prepare_replacement(PublicationBootstrap bootstrap, std::shared_ptr<Replacement> *out,
                                                   std::shared_ptr<const PublishedSources> inherited) const
try {
    if (out == nullptr || *out || !bootstrap.initial || !bootstrap.source_catalog || !bootstrap.initial->sources ||
        !bootstrap.initial->sources->tables.empty()) {
        return Status::invalid_argument("replacement requires a private mapping and empty source catalog");
    }
    const auto &catalog = *bootstrap.source_catalog;
    if (catalog.floor != bootstrap.initial->coverage || catalog.generation != bootstrap.initial->generation ||
        !catalog.active || catalog.tables.size() != 1 || catalog.tables.front() != catalog.active) {
        return Status::invalid_argument("replacement source catalog differs from the candidate generation");
    }
    if (inherited && (!inherited->inheritance || inherited->floor != catalog.floor || inherited->tables.empty() ||
                      inherited->tables.front() != catalog.active || inherited->inheritance->local_count != 1 ||
                      inherited->inheritance->frontier <= catalog.floor)) {
        return Status::invalid_argument("replacement inheritance differs from its local source catalog");
    }
    auto prepared             = detail::reserved_shared<Replacement>(bootstrap.metadata);
    prepared->state           = detail::reserved_shared<PublishedTreeState>(bootstrap.metadata, *bootstrap.initial);
    auto &state               = *prepared->state;
    state.gc_floor            = 0;
    state.checkpoint_token    = 0;
    state.checkpoint_inflight = false;
    state.clear_merge_candidates(bootstrap.metadata);
    initialize_snapshot_changes(state, bootstrap.metadata);
    state.pending_handoff.reset();
    state.flush_admission_owner = 0;
    if (inherited) {
        state.sources = std::move(inherited);
    }
    else {
        auto sources    = detail::reserved_shared<PublishedSources>(bootstrap.metadata, bootstrap.metadata);
        sources->memory = catalog.memory;
        sources->tables.assign(catalog.tables.begin(), catalog.tables.end());
        sources->floor = catalog.floor;
        state.sources  = std::move(sources);
    }
    prepared->checkpoint = detail::reserved_shared<CheckpointLedger>(bootstrap.metadata, checkpoint_upstream_);
    {
        detail::BudgetMemoryResource::Scope scope(prepared->checkpoint->resource, bootstrap.metadata);
        state.mapping.visit(&prepared->checkpoint->pending, [](void *context, uint64_t pid, const auto &) {
            static_cast<CheckpointPendingMetrics *>(context)->change_page(pid);
        });
        prepared->checkpoint->pending.change_mapping();
        prepared->checkpoint->pending.copy_to(state.stats);
        state.checkpoint_pending_since_ns = prepared->checkpoint->pending.oldest_since_ns();
    }
    if (page_cache_) {
        prepared->residency = page_cache_->residency()->prepare_replacement(state.mapping, bootstrap.metadata);
    }
    prepared->root                       = Root::create(prepared->state, bootstrap.metadata);
    prepared->root->checkpoint_allocator = prepared->checkpoint;
    *out                                 = std::move(prepared);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("generation publication preparation exhausted credit");
}

void PublicationCoordinator::install_replacement(std::shared_ptr<Replacement> replacement) noexcept
{
    assert(replacement && replacement->root != nullptr && idle());
    const auto *current = root_.load(std::memory_order_acquire);
    assert(replacement->state->generation == current->state->generation + 1);
    replacement->state->commit_seq = current->state->commit_seq + 1;
    requested_gc_floor_.store(0, std::memory_order_release);
    checkpoint_ = std::move(replacement->checkpoint);
    auto *next  = std::exchange(replacement->root, nullptr);
    auto *old   = root_.exchange(next, std::memory_order_acq_rel);
    PageCacheResidency::publish(std::move(replacement->residency));

    old->state->retirement.mark();
    epoch_.defer(old);
}
} // namespace crowdb::tree
