// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "primary_backing.h"

namespace crowdb::tree
{
std::shared_ptr<const PrimaryBackingMetadata>
make_primary_backing(const DurableCapturedFrame &frame, const PageState &page, uint64_t coverage, uint64_t gc_floor,
                     const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (frame.type != page_type::kLeafBase || !frame.backing || !frame.backing->reloadable() || !frame.summary ||
        !frame.stored_overflow || frame.frame_bytes == 0 || frame.logical_bytes == 0) {
        return {};
    }
    auto proof                  = detail::reserved_shared<PrimaryBackingMetadata>(metadata);
    proof->frame                = frame;
    auto summary                = *frame.summary;
    summary.new_overflow_frames = 0;
    summary.dropped_tombstones  = 0;
    proof->frame.summary        = summary;
    proof->range                = page.range;
    proof->right_sibling        = page.right_sibling;
    proof->coverage             = coverage;
    proof->gc_floor             = gc_floor;
    return proof;
}

std::shared_ptr<const PrimaryBackingMetadata>
make_inner_primary_backing(const DurableCapturedFrame &frame, const PublishedPage &page, uint64_t coverage,
                           uint64_t gc_floor, const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (frame.type != page_type::kInnerBase || page.level == 0 || !frame.backing || !frame.backing->reloadable() ||
        !frame.summary || !frame.summary->inner_fences || frame.frame_bytes == 0 || frame.logical_bytes == 0) {
        return {};
    }
    auto proof         = detail::reserved_shared<PrimaryBackingMetadata>(metadata);
    proof->frame       = frame;
    proof->coverage    = coverage;
    proof->gc_floor    = gc_floor;
    proof->inner_route = page.inner;
    return proof;
}

bool can_reuse_inner_primary(const PublishedPage &page, uint64_t pid, const InnerEncodeFences &fences, uint64_t backend,
                             uint32_t frame_bytes) noexcept
{
    if (backend == 0 || page.level == 0 || page.leaf || !page.inner || !page.primary) {
        return false;
    }
    const auto &proof = *page.primary;
    const auto &frame = proof.frame;
    if (frame.page_id != pid || frame.type != page_type::kInnerBase || frame.frame_bytes != frame_bytes ||
        !frame.backing || !frame.backing->reloadable() || frame.backing->backend_id != backend ||
        frame.backing != page.inner_backing || !frame.summary || !frame.summary->inner_fences) {
        return false;
    }
    const auto &stored = *frame.summary->inner_fences;
    if (stored.lower_leaf != fences.lower_leaf || stored.upper_leaf != fences.upper_leaf) {
        return false;
    }
    if (proof.inner_route.lock() == page.inner) {
        return true;
    }
    const auto &decoded = page.inner->frame_proof;
    return decoded && decoded->self_page_id == pid && decoded->frame_bytes == frame_bytes && decoded->page_id_fences &&
           decoded->has_fences == (fences.lower_leaf != kInvalidPageId) &&
           (!decoded->has_fences ||
            (decoded->lower_leaf == fences.lower_leaf && decoded->upper_leaf == fences.upper_leaf));
}

bool can_reuse_primary(const PublishedPage &page, uint64_t pid, const PageKeyRange &range, uint64_t gc_floor,
                       uint64_t backend, uint32_t frame_bytes) noexcept
{
    if (backend == 0 || page.level != 0 || !page.leaf || !page.primary || !page.leaf->overlays.empty() ||
        page.leaf->bases.size() != 1) {
        return false;
    }
    const auto &proof = *page.primary;
    const auto &frame = proof.frame;
    const auto &base  = page.leaf->bases.front();
    // Encoding keeps one winner per key. An exact live-only summary proves
    // that advancing retention cannot remove a cell from this stored frame.
    const bool live_only =
        frame.summary && frame.summary->live_counts_known && frame.summary->encoded_entries == frame.summary->live_keys;
    const bool compatible_floor = proof.gc_floor == gc_floor || (proof.gc_floor < gc_floor && live_only);
    return frame.page_id == pid && frame.type == page_type::kLeafBase && frame.frame_bytes == frame_bytes &&
           frame.backing && frame.backing->reloadable() && frame.backing->backend_id == backend &&
           frame.backing == base.backing && frame.summary && frame.stored_overflow && compatible_floor &&
           proof.right_sibling == page.leaf->right_sibling && proof.range.lower == range.lower &&
           proof.range.upper == range.upper && base.range.lower == range.lower && base.range.upper == range.upper &&
           (!base.max_slot || *base.max_slot >= proof.coverage);
}
} // namespace crowdb::tree
