// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "writeback_selection.h"

#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
namespace
{
bool resident_child_fences(const PublishedPage &page, uint64_t pid, InnerEncodeFences *out)
{
    auto backing = page.inner_backing;
    if (page.level == 0) {
        if (!page.leaf || !page.leaf->overlays.empty() || page.leaf->bases.size() != 1) {
            return false;
        }
        const auto &base = page.leaf->bases.front();
        if (base.range.lower != page.leaf->range.lower || base.range.upper != page.leaf->range.upper) {
            return false;
        }
        backing = base.backing;
    }
    if (!backing || backing->storage != BackingStorage::kResidentFrame || !backing->readable() ||
        backing->resident_frame.size() < kFrameHeaderSize + kFrameTrailerSize) {
        return false;
    }
    // Import verified the entire graph before publishing this immutable raw
    // frame. Header access borrows its existing owner and performs no I/O.
    const auto *frame = backing->resident_frame.data();
    if (frame_u64(frame, fh::kSelfpage_id) != pid) {
        return false;
    }
    if (page.level == 0 && frame_page_type(frame) == page_type::kLeafBase) {
        const bool nonempty = frame_u32(frame, fh::kSlotCount) != 0 || frame_u32(frame, fh::kDeltaCount) != 0;
        *out                = nonempty ? InnerEncodeFences{.lower_leaf = pid, .upper_leaf = pid} : InnerEncodeFences{};
        return true;
    }
    if (page.level == 0 || frame_page_type(frame) != page_type::kInnerBase || !frame_fences_are_page_ids(frame)) {
        return false;
    }
    *out = {.lower_leaf = frame_lower_fence_page_id(frame), .upper_leaf = frame_upper_fence_page_id(frame)};
    return true;
}

bool child_fences(const PublishedPage &page, uint64_t pid, InnerEncodeFences *out)
{
    if (page.metrics.dirty) {
        return false;
    }
    if (!page.primary || !page.primary->frame.backing) {
        return resident_child_fences(page, pid, out);
    }
    const auto &proof = *page.primary;
    const auto &frame = proof.frame;
    if (!frame.summary) {
        return resident_child_fences(page, pid, out);
    }
    const auto &summary = *frame.summary;
    if (page.level == 0) {
        if (!page.leaf || !can_reuse_primary(page, pid, page.leaf->range, proof.gc_floor, frame.backing->backend_id,
                                             frame.frame_bytes)) {
            return false;
        }
        *out = summary.encoded_entries != 0 ? InnerEncodeFences{.lower_leaf = pid, .upper_leaf = pid}
                                            : InnerEncodeFences{};
        return true;
    }
    if (!summary.inner_fences || frame.page_id != pid || frame.backing != page.inner_backing ||
        !frame.backing->reloadable()) {
        return false;
    }
    const auto fences = *summary.inner_fences;
    if (page.inner && !can_reuse_inner_primary(page, pid, fences, frame.backing->backend_id, frame.frame_bytes)) {
        return false;
    }
    *out = fences;
    return true;
}

Status check_inner(const PublishedPage &page, size_t max_children)
{
    if (page.level == 0 || page.leaf || !page.inner || page.inner->children.empty() ||
        page.inner->children.size() != page.inner->separators.size() + 1) {
        return Status::corruption("writeback candidate has no resident routing descriptor");
    }
    if (page.inner->children.size() > max_children) {
        return Status::resource_exhausted("writeback child window exceeds its page bound");
    }
    return Status::Ok();
}

Status missing_child(const PublishedTreeState &view, const PublishedPage &page, size_t max_children,
                     std::optional<uint64_t> *out)
{
    auto status = check_inner(page, max_children);
    if (!status.ok()) {
        return status;
    }
    for (const auto pid : page.inner->children) {
        const auto child = view.mapping.get(pid);
        if (!child || child->level + uint64_t{1} != page.level) {
            return Status::corruption("writeback child is absent or has the wrong captured level");
        }
        InnerEncodeFences fences;
        if (!child_fences(*child, pid, &fences)) {
            *out = pid;
            return Status::Ok();
        }
    }
    out->reset();
    return Status::Ok();
}
} // namespace

Status select_writeback_page(const PublishedTreeState &view, const CapturedFrameLimits &limits,
                             std::optional<uint64_t> *out)
{
    *out = view.mapping.oldest_writeback_page();
    for (size_t depth = 0; out->has_value() && depth < limits.max_depth; ++depth) {
        const auto page = view.mapping.get(out->value());
        if (!page) {
            return Status::corruption("writeback candidate is absent from captured mapping");
        }
        if (page->level == 0) {
            return Status::Ok();
        }
        std::optional<uint64_t> child;
        auto                    status = missing_child(view, *page, limits.encode.max_entries, &child);
        if (!status.ok() || !child) {
            return status;
        }
        *out = child;
    }
    return out->has_value() ? Status::resource_exhausted("writeback dependency route exceeds its depth bound")
                            : Status::Ok();
}

Status writeback_inner_fences(const PublishedTreeState &view, const PublishedPage &page, size_t max_children,
                              InnerEncodeFences *out)
{
    auto status = check_inner(page, max_children);
    if (!status.ok()) {
        return status;
    }
    *out = {};
    for (const auto pid : page.inner->children) {
        const auto child = view.mapping.get(pid);
        if (!child || child->level + uint64_t{1} != page.level) {
            return Status::corruption("writeback child is absent or has the wrong captured level");
        }
        InnerEncodeFences fences;
        if (!child_fences(*child, pid, &fences)) {
            return Status::unavailable("writeback inner needs its captured child receipts");
        }
        if (fences.lower_leaf != kInvalidPageId) {
            if (out->lower_leaf == kInvalidPageId) {
                out->lower_leaf = fences.lower_leaf;
            }
            out->upper_leaf = fences.upper_leaf;
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree
