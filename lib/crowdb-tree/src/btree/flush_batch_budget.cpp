// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_subtree.h"
#include "page_cache_residency.h"

#include <algorithm>

namespace crowdb::tree
{
std::optional<size_t> FlushBatch::metadata_reserve_bound(const MappingView &mapping, size_t sources, size_t entries,
                                                         size_t key_bytes, size_t workers,
                                                         const FlushBatchOptions &options, bool track_residency,
                                                         size_t max_inner_children)
{
    if (sources == 0 || sources > options.max_sources || workers == 0 || options.parallelism == 0 ||
        options.run_limits.max_entries == 0 || options.run_limits.max_key_bytes == 0 ||
        options.run_limits.key_block_bytes == 0 || options.routing_leaves_per_step == 0 ||
        options.install_run_window == 0 || options.inline_entries == 0 || options.inline_bytes == 0 ||
        options.max_run_bytes == 0) {
        return {};
    }
    const auto mapping_bounds = mapping.copy_bounds();
    // A finite source prefix can span any number of existing leaves. Its
    // entry allowance bounds the complete range metadata; parallelism and
    // routing steps bound active work independently of the Tree's size.
    const auto ranges = std::min(options.persistent ? std::max(size_t{1}, entries + options.persistent_pages) : entries,
                                 mapping_bounds.leaf_pages);
    if (mapping_bounds.leaf_pages == 0) {
        return {};
    }
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    const auto installs = AllocationBound(ranges).add(entries / options.install_run_window).value();
    if (!installs) {
        return {};
    }
    const auto      parallelism = std::min(ranges, options.parallelism);
    AllocationBound bytes(shared_allocation_bound<State>() + shared_allocation_bound<FlushBatch>() +
                          sizeof(State::Attempt));
    // Standalone batches can publish a first partial handoff without a
    // controller admission. Retain its complete source-free base once.
    bytes.add(PublicationCoordinator::complete_view_reserve_bytes());
    bytes.add(PublishedRouteCursor::allocation_bound(options.max_route_depth));
    const auto paths = AllocationBound(ranges).add_product(ranges, mapping_bounds.max_level).add(1).value();
    // A cold index has no resident child arrays in the captured mapping.
    // Loading it still creates the same fork and ordered-join storage.
    const auto decoded_inner =
        AllocationBound(shared_allocation_bound<PublishedPage>() + InnerPageState::allocation_bound())
            .add_product(max_inner_children, 3 * (sizeof(uint64_t) + sizeof(OwnedKey)))
            .value();
    if (!decoded_inner) {
        return {};
    }
    const auto inner_bytes = std::max(mapping_bounds.max_inner_bytes, *decoded_inner);
    const auto slots       = AllocationBound().add_product(ranges, (inner_bytes / sizeof(uint64_t)) + 1).value();
    if (!paths || !slots) {
        return {};
    }
    bytes.add_product(*paths, shared_allocation_bound<State::Subtree>() + sizeof(detail::MaintenanceBuilder) +
                                  State::Subtree::fork_bytes());
    const auto child_slots =
        AllocationBound()
            .add_product(std::min(mapping_bounds.leaf_pages, *slots),
                         sizeof(std::shared_ptr<State::Subtree>) + (3 * sizeof(State::Subtree::Child)) +
                             (3 * (sizeof(uint64_t) + sizeof(OwnedKey))) + (3 * sizeof(PageReplacement)) +
                             (options.persistent ? 3 * sizeof(State::Subtree::FinalPage) : 0))
            .value();
    if (!child_slots) {
        return {};
    }
    bytes.add_product(mapping_bounds.max_level, *child_slots);
    bytes.add(detail::BoundedRangeJoin::allocation_bound(options.parallelism));

    // Every accepted range can leave a retired COW root alive. Count all paths,
    // request/record storage, checkpoint membership nodes and the final join
    // proof even when no explicit reclamation runs during this batch.
    bytes.add_product(*installs, MappingView::path_copy_bound());
    bytes.add_product(*installs, PublicationCoordinator::snapshot_change_reserve_bytes());
    bytes.add_product(*installs, PublicationCoordinator::empty_commit_reserve_bytes());
    bytes.add_product(*installs, MergePairMetrics::invalidation_bytes(2));
    // The admitted handoff only removes existing sibling pairs. Each endpoint
    // belongs to at most two pairs, so its membership/proof can change at most
    // twice across the whole batch, regardless of the number of install windows.
    const auto memberships = AllocationBound().add_product(2, mapping_bounds.merge_candidate_pages).value();
    const auto per_install = AllocationBound().add_product(4, *installs).value();
    if (!memberships || !per_install) {
        return {};
    }
    bytes.add(PublicationCoordinator::merge_membership_reserve_bytes(std::min(*memberships, *per_install)));
    bytes.add_product(ranges, (3 * sizeof(State::Range)) + sizeof(std::shared_ptr<InstallRecord>) +
                                  sizeof(PageReplacement) + shared_allocation_bound<TreeMetricsOperation>() + 1024);
    bytes.add_product(*installs, detail::shared_allocation_bound<InstallRecord>() +
                                     (3 * sizeof(std::shared_ptr<InstallRecord>)) + sizeof(PageReplacement));
    bytes.add_product(2, PublicationCoordinator::empty_commit_reserve_bytes());
    bytes.add_product(*installs, mapping_bounds.max_leaf_append_bytes);
    if (track_residency) {
        // Handoff adds overlays but preserves each leaf's base array. Overlay
        // descriptor bytes cannot be treated as additional cache base slots.
        const auto touched_bases = AllocationBound().add_product(*installs, mapping_bounds.max_leaf_bases).value();
        if (!touched_bases) {
            return {};
        }
        bytes.add_product(*installs, PageCacheResidency::page_allocation_bound(0));
        bytes.add_product(*touched_bases,
                          PageCacheResidency::page_allocation_bound(1) - PageCacheResidency::page_allocation_bound(0));
    }

    // At worst every input entry owns one arena block and one run. Vector
    // growth includes old/new arrays; run references also coexist in the
    // prepared range and the new PageState. Each append can allocate at most
    // one entry array, including all retired growth arrays, so the total number
    // of arrays is bounded by entries. Values retain existing owners.
    const auto block = std::min(options.run_limits.key_block_bytes, options.run_limits.max_key_bytes);
    bytes.add_product(entries, block);

    bytes.add_product(entries, shared_allocation_bound<EntryKeyArena>() + shared_allocation_bound<OverlayBatch>() +
                                   (3 * sizeof(EntryRef)) + (9 * sizeof(std::shared_ptr<const OverlayBatch>)));
    bytes.add_product(4, key_bytes);
    bytes.add_product(3, std::min(key_bytes, options.run_limits.max_key_bytes));
    bytes.add_product(ranges, 2 * OwnedKey::allocation_overhead());
    bytes.add_product(3, OwnedKey::allocation_overhead());

    const auto merge_source = sizeof(std::shared_ptr<MemTable>) + sizeof(EpochManager::Guard) +
                              sizeof(ConcurrentSkipList::Cursor) + sizeof(MergeSource) + sizeof(int);
    const auto source_work  = AllocationBound()
                                  .add_product(parallelism, merge_source)
                                  .add_product(workers, EpochManager::participant_allocation_bytes())
                                  .value();
    if (!source_work) {
        return {};
    }
    bytes.add_product(sources, *source_work);
    bytes.add_product(workers, 2 * (sizeof(State::Work) + sizeof(State::PlanWork)));
    bytes.add_product(parallelism, 2 * sizeof(State::Work));
    bytes.add(2 * sizeof(State::PlanWork));
    bytes.add(PublishedSources::allocation_bound() + (3 * sizeof(std::shared_ptr<MemTable>)));
    bytes.add_product(sources, 3 * sizeof(std::shared_ptr<MemTable>));
    return bytes.value();
}
} // namespace crowdb::tree
