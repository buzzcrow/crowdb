// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "writeback_rebase.h"

#include "snapshot/primary_backing.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
bool same_base(const BaseRef &captured, const BaseRef &current)
{
    return captured.identity == current.identity && captured.generation == current.generation &&
           captured.range.lower == current.range.lower && captured.range.upper == current.range.upper &&
           captured.max_slot == current.max_slot && captured.backing == current.backing &&
           (captured.backing || captured.resident == current.resident);
}

Status leaf_rebase(const PageState &captured, const PageState &current, BaseRef base, const FinalPageSummary *summary,
                   uint64_t coverage, const std::shared_ptr<detail::AllocationReservation> &metadata,
                   PublishedPage &replacement, size_t max_sources)
{
    if (captured.route_generation != current.route_generation || captured.range.lower != current.range.lower ||
        captured.range.upper != current.range.upper || captured.right_sibling != current.right_sibling ||
        current.overlays.size() < captured.overlays.size() || current.bases.size() != captured.bases.size()) {
        return Status::unavailable("writeback route or captured prefix changed");
    }
    const auto retained = current.overlays.size() - captured.overlays.size();
    if (retained > max_sources) {
        return Status::resource_exhausted("writeback descriptor work bound exceeded");
    }
    if (!std::equal(captured.bases.begin(), captured.bases.end(), current.bases.begin(), same_base) ||
        !std::equal(captured.overlays.begin(), captured.overlays.end(),
                    current.overlays.begin() + static_cast<std::ptrdiff_t>(retained))) {
        return Status::unavailable("writeback base or captured run identity changed");
    }
    for (const auto &run : captured.overlays) {
        if (!run || run->max_slot() > coverage) {
            return Status::invalid_argument("writeback capture does not cover its runs");
        }
    }
    if (current.representation_id == UINT64_MAX) {
        return Status::resource_exhausted("writeback representation identity exhausted");
    }
    auto leaf               = detail::reserved_shared<PageState>(metadata, metadata);
    leaf->range             = current.range;
    leaf->route_generation  = current.route_generation;
    leaf->content_id        = current.content_id;
    leaf->representation_id = current.representation_id + 1;
    leaf->right_sibling     = current.right_sibling;
    leaf->bases.push_back(std::move(base));
    leaf->overlays.assign(current.overlays.begin(), current.overlays.begin() + static_cast<std::ptrdiff_t>(retained));
    auto &metrics                 = replacement.metrics;
    metrics.dirty                 = retained != 0;
    metrics.writeback_candidate   = metrics.dirty;
    metrics.evict_candidate       = false;
    metrics.consolidate_candidate = retained != 0 && metrics.consolidate_candidate;
    metrics.overlay_runs          = retained;
    metrics.overlay_entries       = 0;
    for (const auto &run : leaf->overlays) {
        if (!run) {
            return Status::invalid_argument("writeback current run is absent");
        }
        const auto first = leaf->range.lower ? run->lower_bound(*leaf->range.lower) : 0;
        const auto last  = leaf->range.upper ? run->lower_bound(*leaf->range.upper) : run->entries().size();
        if (last < first || last - first > UINT64_MAX - metrics.overlay_entries) {
            return Status::invalid_argument("writeback run range is invalid");
        }
        metrics.overlay_entries += last - first;
    }
    if (retained == 0 && summary != nullptr) {
        if (summary->live_value_bytes > UINT64_MAX - summary->live_key_bytes) {
            return Status::resource_exhausted("writeback logical byte total overflow");
        }
        metrics.page_size_known          = true;
        metrics.page_size_bytes          = summary->primary_used_bytes;
        metrics.page_size_estimate_bytes = summary->primary_used_bytes;
        metrics.kv_known                 = summary->live_counts_known;
        metrics.kv_count                 = summary->live_keys;
        metrics.kv_bytes                 = summary->live_key_bytes + summary->live_value_bytes;
        metrics.split_candidate          = false;
        metrics.split_ready              = false;
    }
    replacement.leaf = std::move(leaf);
    return Status::Ok();
}
} // namespace

Status build_writeback_rebase(const std::shared_ptr<const PublishedPage> &captured,
                              const std::shared_ptr<const PublishedPage> &current, const DurableCapturedFrame &written,
                              uint64_t coverage, const std::shared_ptr<detail::AllocationReservation> &metadata,
                              std::shared_ptr<const PublishedPage> *out, size_t max_sources, uint64_t gc_floor)
try {
    if (!captured || !current || out == nullptr || *out || !written.backing || !written.backing->reloadable() ||
        written.page_id >= MappingView::kPageLimit || max_sources == 0 || captured->level != current->level ||
        (current->level == 0 ? written.type != page_type::kLeafBase : written.type != page_type::kInnerBase)) {
        return Status::invalid_argument("invalid writeback receipt or page kind");
    }
    if (current->revision == UINT64_MAX) {
        return Status::resource_exhausted("writeback page revision exhausted");
    }
    auto replacement = detail::reserved_shared<PublishedPage>(metadata, *current);
    replacement->revision++;
    replacement->allocation_owner.reset();
    if (current->level == 0) {
        if (!captured->leaf || !current->leaf) {
            return Status::invalid_argument("writeback leaf descriptor is absent");
        }
        BaseRef base{.identity   = written.backing->content_id,
                     .generation = written.backing->generation,
                     .range      = current->leaf->range,
                     .max_slot   = coverage,
                     .backing    = written.backing};
        auto    status =
            leaf_rebase(*captured->leaf, *current->leaf, std::move(base), written.summary ? &*written.summary : nullptr,
                        coverage, metadata, *replacement, max_sources);
        if (!status.ok()) {
            return status;
        }
        replacement->primary = make_primary_backing(written, *captured->leaf, coverage, gc_floor, metadata);
    }
    else {
        if (captured->inner != current->inner || captured->inner_backing != current->inner_backing) {
            return Status::unavailable("writeback inner routing changed");
        }
        replacement->primary       = make_inner_primary_backing(written, *current, coverage, gc_floor, metadata);
        replacement->inner_backing = written.backing;
        replacement->metrics.dirty = false;
        replacement->metrics.writeback_candidate = false;
        replacement->metrics.evict_candidate     = replacement->inner != nullptr;
    }
    *out = std::move(replacement);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("writeback rebase metadata exhausted");
}

} // namespace crowdb::tree
