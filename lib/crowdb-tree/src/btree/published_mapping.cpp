// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "logical_page.h"
#include "overflow_backing.h"
#include "published_state.h"
#include "snapshot/primary_backing.h"

#include <algorithm>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
size_t add_bound(size_t left, size_t right) noexcept
{
    return left > SIZE_MAX - right ? SIZE_MAX : left + right;
}

size_t multiply_bound(size_t count, size_t bytes) noexcept
{
    return bytes != 0 && count > SIZE_MAX / bytes ? SIZE_MAX : count * bytes;
}

uint64_t oldest_time(uint64_t left, uint64_t right) noexcept
{
    if (left == 0) {
        return right;
    }
    return right == 0 ? left : std::min(left, right);
}

bool has_candidate(const std::shared_ptr<const PublishedPage> &page) noexcept
{
    if (!page) {
        return false;
    }
    const auto &metrics = page->metrics;
    return metrics.consolidate_candidate || metrics.split_candidate || metrics.merge_candidate ||
           metrics.writeback_candidate || metrics.evict_candidate;
}

void include_bounds(MappingView::CopyBounds &total, const MappingView::CopyBounds &part) noexcept
{
    total.merge_candidate_pages    = add_bound(total.merge_candidate_pages, part.merge_candidate_pages);
    total.leaf_pages               = add_bound(total.leaf_pages, part.leaf_pages);
    total.leaf_bytes               = add_bound(total.leaf_bytes, part.leaf_bytes);
    total.max_leaf_bytes           = std::max(total.max_leaf_bytes, part.max_leaf_bytes);
    total.max_leaf_append_bytes    = std::max(total.max_leaf_append_bytes, part.max_leaf_append_bytes);
    total.max_leaf_bases           = std::max(total.max_leaf_bases, part.max_leaf_bases);
    total.may_have_overflow_values = total.may_have_overflow_values || part.may_have_overflow_values;
    total.max_inner_bytes          = std::max(total.max_inner_bytes, part.max_inner_bytes);
    total.max_level                = std::max(total.max_level, part.max_level);
    total.candidate_since_ns       = oldest_time(total.candidate_since_ns, part.candidate_since_ns);
    if (part.writeback_page_since_ns != 0 &&
        (total.writeback_page_since_ns == 0 || part.writeback_page_since_ns < total.writeback_page_since_ns ||
         (part.writeback_page_since_ns == total.writeback_page_since_ns &&
          part.oldest_writeback_page_pid < total.oldest_writeback_page_pid))) {
        total.writeback_page_since_ns   = part.writeback_page_since_ns;
        total.oldest_writeback_page_pid = part.oldest_writeback_page_pid;
    }
    if (part.writeback_since_ns != 0 &&
        (total.writeback_since_ns == 0 || part.writeback_since_ns < total.writeback_since_ns ||
         (part.writeback_since_ns == total.writeback_since_ns &&
          part.oldest_writeback_pid < total.oldest_writeback_pid))) {
        total.writeback_since_ns   = part.writeback_since_ns;
        total.oldest_writeback_pid = part.oldest_writeback_pid;
    }
}

MappingView::CopyBounds page_bounds(const PublishedPage &page) noexcept
{
    if (page.level != 0 && page.inner) {
        auto bytes = detail::shared_allocation_bound<PublishedPage>() + InnerPageState::allocation_bound();
        bytes      = add_bound(bytes, multiply_bound(page.inner->children.size(), 3 * sizeof(uint64_t)));
        bytes      = add_bound(bytes, multiply_bound(page.inner->separators.size(), 3 * sizeof(OwnedKey)));
        return {.max_inner_bytes = bytes, .max_level = page.level};
    }
    if (page.level != 0 || !page.leaf) {
        return {.max_level = page.level};
    }
    // The old overlay array and its replacement can coexist during insert.
    // New run references are counted separately by the incoming batch bound.
    auto bytes = detail::shared_allocation_bound<PublishedPage>() + PageState::allocation_bound();
    bytes      = add_bound(bytes, multiply_bound(page.leaf->bases.size(), sizeof(BaseRef)));
    bytes =
        add_bound(bytes, multiply_bound(page.leaf->overlays.size(), 3 * sizeof(std::shared_ptr<const OverlayBatch>)));
    const bool overflow =
        std::any_of(page.leaf->bases.begin(), page.leaf->bases.end(),
                    [&](const auto &base) {
                        // Completed writers attach the exact immutable dependency set.
                        // Missing metadata remains unknown; a null set does not prove an
                        // inline-only frame. Recovery can supply the same proof through
                        // its validated primary receipt without a resident decoded page.
                        if (base.backing && base.backing->overflow && base.backing->overflow->pages.empty()) {
                            return false;
                        }
                        const auto *frame = page.primary ? &page.primary->frame : nullptr;
                        return !frame || !base.backing || frame->backing != base.backing || !frame->stored_overflow ||
                               !frame->stored_overflow->empty();
                    }) ||
        std::any_of(page.leaf->overlays.begin(), page.leaf->overlays.end(),
                    [](const auto &run) { return !run || run->has_overflow_values(); });
    return {.leaf_pages               = 1,
            .merge_candidate_pages    = static_cast<size_t>(page.metrics.merge_candidate),
            .leaf_bytes               = bytes,
            .max_leaf_bytes           = bytes,
            .max_leaf_append_bytes    = bytes,
            .max_leaf_bases           = page.leaf->bases.size(),
            .may_have_overflow_values = overflow};
}
} // namespace

MappingView::CopyBounds MappingView::copy_bounds() const noexcept
{
    return root_ ? root_->bounds : CopyBounds{};
}

std::optional<uint64_t> MappingView::oldest_writeback_leaf() const noexcept
{
    const auto bounds = copy_bounds();
    return bounds.writeback_since_ns != 0 ? std::optional<uint64_t>(bounds.oldest_writeback_pid) : std::nullopt;
}

std::optional<uint64_t> MappingView::oldest_writeback_page() const noexcept
{
    const auto bounds = copy_bounds();
    return bounds.writeback_page_since_ns != 0 ? std::optional<uint64_t>(bounds.oldest_writeback_page_pid)
                                               : std::nullopt;
}

size_t MappingView::path_copy_bound() noexcept
{
    return (4 * detail::shared_allocation_bound<Directory>()) + detail::shared_allocation_bound<Segment>();
}

std::shared_ptr<const PublishedPage> MappingView::get(uint64_t page_id) const
{
    if (page_id >= kPageLimit) {
        return {};
    }
    auto directory = root_;
    for (unsigned shift : {24U, 18U, 12U}) {
        if (!directory) {
            return {};
        }
        directory = directory->children[(page_id >> shift) & 63];
    }
    if (!directory) {
        return {};
    }
    const auto &segment = directory->segments[(page_id >> 6) & 63];
    return segment ? segment->pages[page_id & 63] : nullptr;
}

std::shared_ptr<const MappingView::Directory>
MappingView::replace_directory(const std::shared_ptr<const Directory> &old, unsigned shift,
                               std::span<const PageReplacement> replacements, CopyStats &copied,
                               const std::shared_ptr<AllocationBudget> &metadata, uint64_t now_ns)
{
    auto next = old ? detail::reserved_shared<Directory>(metadata, *old) : detail::reserved_shared<Directory>(metadata);
    ++copied.directories;
    copied.bytes += sizeof(Directory);
    while (!replacements.empty()) {
        const auto index = (replacements.front().page_id >> shift) & 63;
        size_t     count = 1;
        while (count < replacements.size() && ((replacements[count].page_id >> shift) & 63) == index) {
            ++count;
        }
        auto group = replacements.first(count);
        if (shift > 6) {
            next->children[index] =
                replace_directory(next->children[index], shift - 6, group, copied, metadata, now_ns);
        }
        else {
            auto segment = next->segments[index] ? detail::reserved_shared<Segment>(metadata, *next->segments[index])
                                                 : detail::reserved_shared<Segment>(metadata);
            ++copied.segments;
            copied.bytes += sizeof(Segment);
            for (const auto &replacement : group) {
                const auto slot  = replacement.page_id & 63;
                auto      &since = segment->candidate_since_ns[slot];
                if (!has_candidate(replacement.replacement)) {
                    since = 0;
                }
                else if (since == 0) {
                    since = now_ns;
                }
                auto       &writeback_since = segment->writeback_since_ns[slot];
                const auto &page            = replacement.replacement;
                if (!page || !page->metrics.dirty || !page->metrics.writeback_candidate) {
                    writeback_since = 0;
                }
                else if (writeback_since == 0) {
                    writeback_since = now_ns;
                }
                segment->pages[slot] = replacement.replacement;
            }
            segment->bounds = {};
            for (const auto &page : segment->pages) {
                if (page) {
                    include_bounds(segment->bounds, page_bounds(*page));
                }
            }
            const auto prefix = group.front().page_id & ~(kFanout - 1);
            for (size_t slot = 0; slot < kFanout; ++slot) {
                if (segment->writeback_since_ns[slot] != 0) {
                    CopyBounds candidate{.writeback_page_since_ns   = segment->writeback_since_ns[slot],
                                         .oldest_writeback_page_pid = prefix + slot};
                    if (segment->pages[slot]->level == 0) {
                        candidate.writeback_since_ns   = segment->writeback_since_ns[slot];
                        candidate.oldest_writeback_pid = prefix + slot;
                    }
                    include_bounds(segment->bounds, candidate);
                }
            }
            for (const auto since : segment->candidate_since_ns) {
                segment->bounds.candidate_since_ns = oldest_time(segment->bounds.candidate_since_ns, since);
            }
            const bool empty      = std::none_of(segment->pages.begin(), segment->pages.end(),
                                                 [](const auto &page) { return static_cast<bool>(page); });
            next->segments[index] = empty ? nullptr : std::move(segment);
        }
        replacements = replacements.subspan(count);
    }
    next->bounds = {};
    for (const auto &child : next->children) {
        if (child) {
            include_bounds(next->bounds, child->bounds);
        }
    }
    for (const auto &segment : next->segments) {
        if (segment) {
            include_bounds(next->bounds, segment->bounds);
        }
    }
    const bool has_children = std::any_of(next->children.begin(), next->children.end(),
                                          [](const auto &child) { return static_cast<bool>(child); });
    const bool has_segments = std::any_of(next->segments.begin(), next->segments.end(),
                                          [](const auto &segment) { return static_cast<bool>(segment); });
    return has_children || has_segments ? std::move(next) : nullptr;
}

MappingView MappingView::replace(std::span<const PageReplacement> replacements, CopyStats &copied,
                                 const std::shared_ptr<AllocationBudget> &metadata) const
{
    uint64_t previous = 0;
    bool     first    = true;
    for (const auto &replacement : replacements) {
        if (replacement.page_id >= kPageLimit || (!first && replacement.page_id <= previous)) {
            throw std::invalid_argument("mapping replacements must have unique ordered PIDs");
        }
        first    = false;
        previous = replacement.page_id;
    }
    auto result = *this;
    if (!replacements.empty()) {
        result.root_ = replace_directory(root_, 24, replacements, copied, metadata, tree_metrics_now_ns());
    }
    return result;
}
} // namespace crowdb::tree
