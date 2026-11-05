// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache.h"
#include "publication_generation_internal.h"
#include "publication_snapshot.h"
#include "source_admission.h"

#include <algorithm>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
std::atomic<uint64_t> next_publisher_identity{1};

std::shared_ptr<const PublishedSources> initial_sources(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                                        uint64_t                                              floor)
{
    auto sources   = detail::reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor = floor;
    return sources;
}

std::shared_ptr<const PublishedTreeState> initial_state(const PublicationBootstrap &bootstrap, uint64_t generation)
{
    auto state = bootstrap.initial
                   ? detail::reserved_shared<PublishedTreeState>(bootstrap.metadata, *bootstrap.initial)
                   : detail::reserved_shared<PublishedTreeState>(bootstrap.metadata, bootstrap.metadata);
    if (bootstrap.initial && (state->generation != generation || state->pending_handoff ||
                              state->flush_admission_owner != 0 || state->checkpoint_inflight)) {
        throw std::invalid_argument("initial publication must be a completed state in this generation");
    }
    state->generation = generation;
    state->clear_merge_candidates(bootstrap.metadata);
    if (bootstrap.source_catalog) {
        const auto &catalog = *bootstrap.source_catalog;
        if (catalog.floor != state->coverage || catalog.generation != generation || !catalog.active ||
            catalog.tables.empty() ||
            std::find(catalog.tables.begin(), catalog.tables.end(), catalog.active) == catalog.tables.end()) {
            throw std::invalid_argument("initial source catalog differs from publication floor or generation");
        }
        if (state->sources && !state->sources->tables.empty() &&
            (state->sources->floor != catalog.floor || state->sources->tables.size() != catalog.tables.size() ||
             !std::equal(state->sources->tables.begin(), state->sources->tables.end(), catalog.tables.begin()))) {
            throw std::invalid_argument("initial publication contains a different source catalog");
        }
        auto sources    = detail::reserved_shared<PublishedSources>(bootstrap.metadata, bootstrap.metadata);
        sources->memory = catalog.memory;
        sources->tables.assign(catalog.tables.begin(), catalog.tables.end());
        sources->floor = catalog.floor;
        state->sources = std::move(sources);
    }
    if (!state->sources || state->sources->floor != state->coverage) {
        throw std::invalid_argument("initial publication source floor differs from coverage");
    }
    initialize_snapshot_changes(*state, bootstrap.metadata);
    return state;
}
} // namespace

PublishedTreeState::PublishedTreeState(std::shared_ptr<detail::AllocationReservation> metadata, uint64_t source_floor)
    : sources(initial_sources(metadata, source_floor))
{
}

std::optional<size_t> PublicationCoordinator::allocation_bound(size_t source_count, size_t max_threads,
                                                               const MappingView *mapping,
                                                               bool               track_residency) noexcept
{
    using detail::shared_allocation_bound;
    auto bound =
        detail::AllocationBound(
            sizeof(PublicationCoordinator) + sizeof(Root) + shared_allocation_bound<CheckpointLedger>() +
            shared_allocation_bound<PublishedTreeState>() + (2 * PublishedSources::allocation_bound()) +
            shared_allocation_bound<TreeWorkMetrics>() + EpochManager::infrastructure_allocation_bound())
            .add(mapping != nullptr ? merge_membership_reserve_bytes(mapping->copy_bounds().merge_candidate_pages) : 0)
            .add_product(source_count, sizeof(std::shared_ptr<MemTable>))
            .add_product(max_threads, EpochManager::participant_allocation_bytes())
            .add(track_residency && mapping != nullptr ? PageCacheResidency::initial_allocation_bound(*mapping) : 0);
    if (mapping != nullptr) {
        size_t count = 0;
        mapping->visit(&count, [](void *raw, uint64_t, const auto &) { ++*static_cast<size_t *>(raw); });
        bound.add(SnapshotChanges::initial_allocation_bound(count));
    }
    return bound.value();
}

PublicationCoordinator::PublicationCoordinator(size_t queue_capacity, uint64_t generation,
                                               std::shared_ptr<TreeWorkMetrics> metrics,
                                               std::pmr::memory_resource       *checkpoint_resource,
                                               std::shared_ptr<SharedPageCache> page_cache,
                                               PublicationBootstrap             bootstrap)
    : capacity_(queue_capacity),
      identity_(next_publisher_identity.fetch_add(1, std::memory_order_relaxed)),
      checkpoint_upstream_(checkpoint_resource),
      checkpoint_(detail::reserved_shared<CheckpointLedger>(bootstrap.metadata, checkpoint_resource)),
      epoch_(bootstrap.metadata),
      work_metrics_(metrics ? std::move(metrics) : detail::reserved_shared<TreeWorkMetrics>(bootstrap.metadata)),
      page_cache_(std::move(page_cache))
{
    if (queue_capacity == 0 || (page_cache_ && !page_cache_->residency())) {
        throw std::invalid_argument("publication needs queue capacity and a cache with residency metrics");
    }
    auto  initial   = initial_state(bootstrap, generation);
    auto  residency = page_cache_ && !initial->mapping.empty()
                        ? page_cache_->residency()->prepare_initial(initial->mapping, bootstrap.metadata)
                        : nullptr;
    auto *root      = Root::create(std::move(initial), bootstrap.metadata);
    if (page_cache_ && !page_cache_->claim_publication()) {
        Root::dispose(root);
        throw std::invalid_argument("cache residency already belongs to a publication lifetime");
    }
    root->checkpoint_allocator = checkpoint_;
    root_.store(root, std::memory_order_release);
    if (residency) {
        PageCacheResidency::publish(std::move(residency));
    }
}
} // namespace crowdb::tree
