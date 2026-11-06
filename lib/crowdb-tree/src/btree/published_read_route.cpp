// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_route.h"

#include <algorithm>
#include <utility>

namespace crowdb::tree
{
PageKeyRange PublishedRouteCursor::child_range(const Frame &frame) const
{
    const auto &separators = frame.inner->separators;
    return {.lower = frame.child == 0 ? frame.bounds.lower : std::optional(separators[frame.child - 1]),
            .upper = frame.child == separators.size() ? frame.bounds.upper : std::optional(separators[frame.child])};
}

Status PublishedRouteCursor::seek(std::optional<Slice> key, bool exclusive)
{
    skipped_ = 0;
    stack_.clear();
    leaf_.reset();
    leaf_id_ = kInvalidPageId;
    pending_.reset();
    if (root_ == kInvalidPageId && mapping_.empty()) {
        return Status::Ok();
    }
    if (selection_.empty()) {
        skipped_ = 1;
        return Status::Ok();
    }
    return descend(root_, {}, key, exclusive, std::nullopt);
}

Status PublishedRouteCursor::validate_index(const InnerPageState &index, const PageKeyRange &bounds) const
{
    if (index.children.empty() || index.children.size() != index.separators.size() + 1 ||
        index.children.size() > 4096 || stack_.size() >= max_depth_) {
        return Status::corruption("captured index exceeds bounded routing layout");
    }
    for (const auto id : index.children) {
        if (id >= MappingView::kPageLimit) {
            return Status::corruption("captured index has an invalid child PID");
        }
    }
    for (size_t i = 0; i < index.separators.size(); ++i) {
        const Slice separator(index.separators[i]);
        if ((i != 0 && index.separators[i - 1] >= index.separators[i]) ||
            (bounds.lower && separator < Slice(*bounds.lower)) || (bounds.upper && separator > Slice(*bounds.upper))) {
            return Status::corruption("captured routing separators are not strictly ordered");
        }
    }
    return Status::Ok();
}

Status PublishedRouteCursor::descend(uint64_t id, PageKeyRange bounds, std::optional<Slice> key, bool exclusive,
                                     std::optional<uint32_t>               expected_level,
                                     std::shared_ptr<const InnerPageState> loaded)
{
    for (;;) {
        const auto page = mapping_.get(id);
        if (!page || (expected_level && page->level != *expected_level) || page->level > max_depth_) {
            return Status::corruption("captured routing page is absent or has an invalid level");
        }
        if (page->level == 0) {
            if (!page->leaf || (!stack_.empty() &&
                                (page->leaf->range.lower != bounds.lower || page->leaf->range.upper != bounds.upper))) {
                return Status::corruption("captured leaf bounds disagree with routing");
            }
            leaf_    = page->leaf;
            leaf_id_ = id;
            return Status::Ok();
        }
        // A resumed index belongs only to this level. A conditional with the
        // const resident shared_ptr can copy loaded instead of consuming it.
        auto inner = std::exchange(loaded, {});
        if (!inner) {
            inner = page->inner;
        }
        if (!inner) {
            if (!page->inner_backing || !page->inner_backing->readable()) {
                return Status::corruption("captured index has neither resident state nor owned backing");
            }
            pending_ =
                Pending{.id     = id,
                        .page   = page,
                        .bounds = std::move(bounds),
                        .key = key ? std::make_optional<OwnedKey>(*key, stack_.get_allocator().owner()) : std::nullopt,
                        .exclusive = exclusive};
            return Status::Ok();
        }
        auto status = validate_index(*inner, bounds);
        if (!status.ok()) {
            return status;
        }
        size_t child = reverse_ ? inner->children.size() - 1 : 0;
        if (key) {
            auto at =
                reverse_ && exclusive
                    ? std::lower_bound(inner->separators.begin(), inner->separators.end(), *key,
                                       [](const OwnedKey &separator, Slice value) { return Slice(separator) < value; })
                    : std::upper_bound(inner->separators.begin(), inner->separators.end(), *key,
                                       [](Slice value, const OwnedKey &separator) { return value < Slice(separator); });
            child = static_cast<size_t>(at - inner->separators.begin());
        }
        skipped_ += reverse_ ? inner->children.size() - child - 1 : child;
        stack_.push_back({.page = page, .inner = std::move(inner), .bounds = std::move(bounds), .child = child});
        bounds         = child_range(stack_.back());
        id             = stack_.back().inner->children[child];
        expected_level = page->level - 1;
    }
}

Status PublishedRouteCursor::resume_inner(std::shared_ptr<const InnerPageState> inner)
{
    if (!pending_) {
        return Status::invalid_argument("routing cursor has no pending index load");
    }
    if (!inner) {
        return Status::corruption("index loader returned no immutable state");
    }
    auto pending = std::move(*pending_);
    pending_.reset();
    const auto key = pending.key ? std::optional<Slice>(*pending.key) : std::nullopt;
    return descend(pending.id, std::move(pending.bounds), key, pending.exclusive, pending.page->level,
                   std::move(inner));
}

Status PublishedRouteCursor::next()
{
    if (pending_) {
        return Status::unavailable("routing cursor is waiting for an index load");
    }
    leaf_.reset();
    leaf_id_ = kInvalidPageId;
    while (!stack_.empty()) {
        auto      &frame = stack_.back();
        const bool end   = reverse_ ? frame.child == 0 : frame.child + 1 == frame.inner->children.size();
        if (end) {
            stack_.pop_back();
            continue;
        }
        frame.child          = reverse_ ? frame.child - 1 : frame.child + 1;
        const uint64_t id    = frame.inner->children[frame.child];
        const uint32_t level = frame.page->level - 1;
        auto           range = child_range(frame);
        if (!selection_.overlaps(range)) {
            ++skipped_;
            continue;
        }
        return descend(id, std::move(range), std::nullopt, false, level);
    }
    return Status::Ok();
}
} // namespace crowdb::tree
