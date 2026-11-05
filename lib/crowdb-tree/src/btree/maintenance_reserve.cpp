// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "maintenance_operation_internal.h"
#include "maintenance_split_layout.h"
#include "page_cache_residency.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
struct MaintenanceWindow
{
    size_t changed;
    size_t leaves;
};

std::optional<MaintenanceWindow> maintenance_window(const MappingView::CopyBounds       &shape,
                                                    const MaintenancePublicationOptions &options) noexcept
{
    if (options.pages.max_output_pages < 2 || options.max_changed_pages == 0 || options.pages.max_work_entries == 0) {
        return {};
    }
    const auto outputs    = std::min(options.pages.max_output_pages - 1, options.pages.max_work_entries) + 1;
    const auto root_pages = detail::AllocationBound().add_product(2, outputs).value();
    const auto leaf_pages = detail::AllocationBound(outputs).add(2).value();
    if (!root_pages || !leaf_pages) {
        return {};
    }
    const auto changed =
        shape.max_level == 0 ? std::min(options.max_changed_pages, *root_pages) : options.max_changed_pages;
    return MaintenanceWindow{.changed = changed, .leaves = std::min(changed, *leaf_pages)};
}
} // namespace

void limit_page_maintenance_window(const MappingView &mapping, size_t new_runs, size_t reclaim_bytes,
                                   size_t snapshot_bytes, bool track_residency,
                                   PageMaintenanceOperationOptions *options) noexcept
{
    auto &pages = options->publication.pages;
    if (pages.max_output_pages < 2 || pages.max_work_entries == 0) {
        return;
    }
    size_t     low  = 2;
    size_t     high = std::min(pages.max_output_pages - 1, pages.max_work_entries) + 1;
    const auto fits = [&](size_t count) {
        pages.max_output_pages = count;
        const auto reclaim     = page_maintenance_reserve_bound(mapping, new_runs, *options, track_residency, true);
        const auto snapshot    = page_maintenance_snapshot_reserve_bound(mapping, *options);
        return reclaim && snapshot && *reclaim <= reclaim_bytes && *snapshot <= snapshot_bytes;
    };
    if (fits(high)) {
        return;
    }
    while (low < high) {
        const auto middle = low + ((high - low + 1) / 2);
        if (fits(middle)) {
            low = middle;
        }
        else {
            high = middle - 1;
        }
    }
    pages.max_output_pages = low;
}

std::optional<size_t> page_maintenance_snapshot_reserve_bound(const MappingView                     &mapping,
                                                              const PageMaintenanceOperationOptions &options) noexcept
{
    const auto window = maintenance_window(mapping.copy_bounds(), options.publication);
    if (!window) {
        return {};
    }
    const auto              changed = window->changed;
    detail::AllocationBound bytes(detail::shared_allocation_bound<detail::AllocationReservation>());
    bytes.add_product(changed, PublicationCoordinator::snapshot_change_reserve_bytes());
    return bytes.value();
}

std::optional<size_t> page_maintenance_reserve_bound(const MappingView &mapping, size_t new_runs,
                                                     const PageMaintenanceOperationOptions &options,
                                                     bool track_residency, bool separate_snapshot_credit) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    const auto &publication = options.publication;
    const auto &pages       = publication.pages;
    if (options.max_candidates == 0 || options.max_steps == 0 || options.max_visits == 0 ||
        options.visits_per_step == 0 || publication.max_changed_pages == 0 || publication.max_path_pages == 0 ||
        publication.max_path_pages > 64 || pages.max_work_entries == 0) {
        return {};
    }
    const auto mapping_bounds = mapping.copy_bounds();
    // A leaf root has no existing routing path. Its first bounded split can
    // create at most N leaves and N - 1 inner pages, even at minimum fanout.
    // Later steps may defer when this invocation's prepaid credit is spent.
    const auto window = maintenance_window(mapping_bounds, publication);
    if (!window) {
        return {};
    }
    const auto changed = window->changed;
    const auto leaf    = AllocationBound(mapping_bounds.max_leaf_bytes)
                             .add_product(new_runs, 3 * sizeof(std::shared_ptr<const OverlayBatch>))
                             .value();
    const auto inner = AllocationBound(mapping_bounds.max_inner_bytes).add(InnerPageState::allocation_bound()).value();
    if (!leaf || !inner) {
        return {};
    }
    AllocationBound bytes(shared_allocation_bound<detail::MaintenanceFactory>() +
                          shared_allocation_bound<detail::MaintenanceOperation>() +
                          shared_allocation_bound<detail::MaintenanceOperation::Candidates>());
    bytes.add_product(options.max_candidates, sizeof(detail::MaintenanceOperation::Candidate));
    bytes.add(shared_allocation_bound<detail::MaintenanceOperation::Cold>());
    bytes.add(shared_allocation_bound<PublishedTreeState>());
    bytes.add(shared_allocation_bound<PublishedPage>());
    // Only captured routing pages require cold scratch. Inner pages created
    // by this maintenance window already own resident routes. Retain the full
    // bounded replacement arrays, but charge copied radix paths by that depth.
    const auto cold_depth = std::min<size_t>(publication.max_path_pages, mapping_bounds.max_level);
    bytes.add_product((3 * cold_depth) + 1, MappingView::path_copy_bound());
    bytes.add_product((3 * publication.max_path_pages) + 1, sizeof(PageReplacement));
    bytes.add_product(2 * publication.max_path_pages, sizeof(uint64_t));
    bytes.add(*leaf);
    bytes.add_product(window->leaves, *leaf);
    bytes.add_product(changed, *inner);
    // Each new child belongs to one replacement parent. Existing child arrays
    // are covered above, including both inputs of an inner merge. Charge new
    // route entries once across the transaction, not once per changed page.
    bytes.add_product(changed, 3 * (sizeof(uint64_t) + sizeof(OwnedKey)));
    bytes.add_product(changed, MappingView::path_copy_bound());
    if (!separate_snapshot_credit) {
        bytes.add_product(changed, PublicationCoordinator::snapshot_change_reserve_bytes());
    }
    bytes.add_product(changed, sizeof(PageReplacement) + 1024);
    bytes.add_product(changed, OwnedKey::allocation_overhead());
    // Inner sibling repair changes two leaf-parent routes at the bottom
    // level. Their direct pairs and incident changed-leaf pairs retire together.
    const auto invalidated_pairs =
        AllocationBound().add_product(2, publication.max_inner_children).add_product(2, changed).value();
    if (!invalidated_pairs) {
        return {};
    }
    bytes.add(MergePairMetrics::invalidation_bytes(*invalidated_pairs));
    bytes.add_product(options.max_candidates, MergePairMetrics::insertion_bytes());
    const auto changed_memberships =
        detail::AllocationBound(mapping_bounds.merge_candidate_pages).add_product(options.max_candidates, 2).value();
    if (!changed_memberships) {
        return {};
    }
    bytes.add(PublicationCoordinator::merge_membership_reserve_bytes(*changed_memberships));
    if (track_residency) {
        // A split step replaces one leaf with a bounded prefix and suffix;
        // coalesce/merge touch at most two leaves. The remaining replacements
        // are routing pages and cannot acquire a leaf-sized base array.
        const auto leaves = window->leaves;
        bytes.add_product(changed, PageCacheResidency::page_allocation_bound(1));
        bytes.add_product(leaves, PageCacheResidency::page_allocation_bound(std::max(size_t{1}, pages.max_base_views)) -
                                      PageCacheResidency::page_allocation_bound(1));
    }
    bytes.add_product(4, PublicationCoordinator::empty_commit_reserve_bytes());
    // Bounded merge arrays, coalesced entries/runs and separator/layout scratch.
    bytes.add_product(8, pages.max_metadata_bytes);
    bytes.add_product(pages.max_work_entries, (4 * sizeof(EntryRef)) + shared_allocation_bound<OverlayBatch>());
    bytes.add_product(pages.max_sources, 1024);
    bytes.add_product(pages.max_base_views, 1024);
    bytes.add_product(pages.max_output_pages, pages.primary_page_bytes);
    return bytes.value();
}

} // namespace crowdb::tree
