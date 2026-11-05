// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_maintenance.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
PageKeyRange intersection(PageKeyRange left, const PageKeyRange &right)
{
    if (right.lower.has_value() && (!left.lower.has_value() || Slice(*left.lower) < Slice(*right.lower))) {
        left.lower = right.lower;
    }
    if (right.upper.has_value() && (!left.upper.has_value() || Slice(*left.upper) > Slice(*right.upper))) {
        left.upper = right.upper;
    }
    return left;
}

void set_version(PageState *page, PageVersion version)
{
    page->route_generation  = version.route_generation;
    page->content_id        = version.content_id;
    page->representation_id = version.representation_id;
}

Status append_range(const PageState &source, const PageKeyRange &range, const PageMaintenanceLimits &limits,
                    PageState *output)
{
    for (const auto &base : source.bases) {
        if (!base.range.overlaps(range)) {
            continue;
        }
        if (output->bases.size() >= limits.max_base_views) {
            return Status::resource_exhausted("logical base view budget exhausted");
        }
        auto view  = base;
        view.range = intersection(base.range, range);
        output->bases.push_back(std::move(view));
    }
    for (const auto &run : source.overlays) {
        if (run == nullptr) {
            return Status::invalid_argument("logical range has an unowned overlay");
        }
        const auto   entries = run->entries();
        const size_t first   = range.lower.has_value() ? run->lower_bound(Slice(*range.lower)) : 0;
        const size_t end     = range.upper.has_value() ? run->lower_bound(Slice(*range.upper)) : entries.size();
        if (first == end) {
            continue;
        }
        if (end - first > limits.run.max_entries || end - first > limits.max_metadata_bytes / sizeof(EntryRef)) {
            return Status::resource_exhausted("logical range overlay budget exhausted");
        }
        if (first == 0 && end == entries.size()) {
            output->overlays.push_back(run);
            continue;
        }
        EntryStorage selected(entries.begin() + static_cast<ptrdiff_t>(first),
                              entries.begin() + static_cast<ptrdiff_t>(end), EntryAllocator(limits.metadata));
        std::shared_ptr<const OverlayBatch> view;
        auto status = OverlayBatch::build_owned(run->id(), std::move(selected), limits.run, &view);
        if (!status.ok()) {
            return status;
        }
        output->overlays.push_back(std::move(view));
    }
    return Status::Ok();
}
} // namespace

LeafSplitResult split_leaf_ranges(const LeafLayoutPlan &layout, std::span<const SplitTarget> targets,
                                  const PageMaintenanceLimits &limits)
{
    LeafSplitResult result(limits.metadata);
    if (!layout.status.ok() || layout.readiness != MaintenanceReadiness::kReady) {
        result.status    = layout.status;
        result.readiness = layout.readiness;
        return result;
    }
    if (layout.source == nullptr || targets.empty() || targets.size() != layout.separators.size() + 1 ||
        targets.size() != layout.primary_bytes.size() || targets.size() > limits.max_output_pages) {
        result.status = Status::invalid_argument("split targets must match a complete captured layout");
        return result;
    }
    for (size_t i = 0; i < targets.size(); ++i) {
        if (targets[i].page_id == kInvalidPageId || layout.primary_bytes[i] > limits.primary_page_bytes ||
            targets[i].version.route_generation <= layout.source->route_generation) {
            result.status = Status::invalid_argument("split target needs a new route generation and valid layout");
            return result;
        }
        for (size_t j = 0; j < i; ++j) {
            if (targets[i].page_id == targets[j].page_id) {
                result.status = Status::invalid_argument("split target page IDs must be unique");
                return result;
            }
        }
        if (i != 0 && (!layout.source->range.contains(Slice(layout.separators[i - 1])) ||
                       (i > 1 && layout.separators[i - 2] >= layout.separators[i - 1]))) {
            result.status = Status::invalid_argument("split separators must be ordered within the source range");
            return result;
        }
    }
    result.pages.reserve(targets.size());
    for (size_t i = 0; i < targets.size(); ++i) {
        auto page         = detail::reserved_shared<PageState>(limits.metadata, limits.metadata);
        page->range.lower = i == 0 ? layout.source->range.lower : std::optional(layout.separators[i - 1]);
        page->range.upper = i + 1 == targets.size() ? layout.source->range.upper : std::optional(layout.separators[i]);
        page->right_sibling = i + 1 == targets.size() ? layout.source->right_sibling : targets[i + 1].page_id;
        set_version(page.get(), targets[i].version);
        result.status = append_range(*layout.source, page->range, limits, page.get());
        if (!result.status.ok()) {
            result.readiness = MaintenanceReadiness::kDeferred;
            result.pages.clear();
            return result;
        }
        result.pages.push_back(std::move(page));
    }
    return result;
}

PageMaintenanceResult plan_leaf_merge(const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                      PageVersion version, uint64_t first_run_id)
{
    PageMaintenanceResult result(limits.metadata);
    if (input.left == nullptr || input.right == nullptr || input.left_page_id == input.right_page_id ||
        input.left_page_id == kInvalidPageId || input.right_page_id == kInvalidPageId ||
        input.left_parent == kInvalidPageId || input.left_parent != input.right_parent ||
        input.left->right_sibling != input.right_page_id || !input.left->range.upper.has_value() ||
        input.left->range.upper != input.right->range.lower ||
        version.route_generation <= std::max(input.left->route_generation, input.right->route_generation) ||
        limits.merge_target_bytes >= limits.target_page_bytes) {
        result.status = Status::invalid_argument("merge requires adjacent leaves of one parent and hysteresis");
        return result;
    }
    const auto left  = plan_leaf_layout(input.left, limits);
    const auto right = plan_leaf_layout(input.right, limits);
    for (const auto *layout : {&left, &right}) {
        if (!layout->status.ok()) {
            result.status    = layout->status;
            result.readiness = layout->readiness;
            return result;
        }
        if (layout->readiness == MaintenanceReadiness::kNeedsBase) {
            result.readiness = MaintenanceReadiness::kNeedsBase;
            result.loads.insert(result.loads.end(), layout->loads.begin(), layout->loads.end());
        }
    }
    if (result.readiness == MaintenanceReadiness::kNeedsBase) {
        return result;
    }
    constexpr size_t overhead = kFrameHeaderSize + kFrameTrailerSize;
    if (std::min(left.total_primary_bytes, right.total_primary_bytes) >= limits.merge_trigger_bytes ||
        left.total_primary_bytes > limits.merge_target_bytes ||
        right.total_primary_bytes - overhead > limits.merge_target_bytes - left.total_primary_bytes) {
        return result;
    }
    auto joined = join_leaf_ranges(input, limits, version);
    if (!joined.status.ok()) {
        return joined;
    }
    return coalesce_overlays(std::move(joined.page), limits, version.representation_id, first_run_id);
}

PageMaintenanceResult join_leaf_ranges(const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                       PageVersion version)
{
    PageMaintenanceResult result(limits.metadata);
    if (!input.left || !input.right || input.left_page_id == input.right_page_id ||
        input.left_page_id == kInvalidPageId || input.right_page_id == kInvalidPageId ||
        input.left_parent == kInvalidPageId || input.left_parent != input.right_parent ||
        input.left->right_sibling != input.right_page_id || !input.left->range.upper ||
        input.left->range.upper != input.right->range.lower ||
        version.route_generation <= std::max(input.left->route_generation, input.right->route_generation)) {
        result.status = Status::invalid_argument("range join requires adjacent leaves of one parent and a new version");
        return result;
    }
    auto joined           = detail::reserved_shared<PageState>(limits.metadata, limits.metadata);
    joined->range         = {.lower = input.left->range.lower, .upper = input.right->range.upper};
    joined->right_sibling = input.right->right_sibling;
    set_version(joined.get(), version);
    for (const auto &source : {input.left, input.right}) {
        result.status = append_range(*source, source->range, limits, joined.get());
        if (!result.status.ok()) {
            result.readiness = MaintenanceReadiness::kDeferred;
            return result;
        }
    }
    result.page = std::move(joined);
    return result;
}
} // namespace crowdb::tree
