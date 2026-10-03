// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/btree/descent.h"
#include "crowdb-tree/crowdb-tree.h"

#include <unordered_map>
#include <unordered_set>

namespace crowdb::tree::detail
{
struct NativeBounds
{
    std::optional<std::string> lower;
    std::optional<std::string> upper;
    uint64_t                   lower_leaf_page_id = kInvalidPageId;
    uint64_t                   upper_leaf_page_id = kInvalidPageId;
};

inline bool set_native_frame_fences(NativeFrame *frame, const NativeBounds &bounds)
{
    uint8_t   *bytes      = frame->frame.data();
    const auto page_bytes = static_cast<uint32_t>(frame->frame.size());
    if (!bounds.lower.has_value()) {
        return frame_set_fences(bytes, page_bytes, nullptr, nullptr);
    }
    if (!bounds.upper.has_value()) {
        return false;
    }
    Slice lower(*bounds.lower);
    Slice upper(*bounds.upper);
    if (frame_page_type(bytes) == page_type::kLeafBase) {
        LeafFrameView leaf(bytes, page_bytes);
        for (uint32_t index = 0; index < leaf.count(); ++index) {
            if (leaf.key(index).compare(lower) == 0) {
                lower = leaf.key(index);
            }
            if (leaf.key(index).compare(upper) == 0) {
                upper = leaf.key(index);
            }
        }
        for (uint32_t index = 0; index < leaf.delta_count(); ++index) {
            if (leaf.delta_key(index).compare(lower) == 0) {
                lower = leaf.delta_key(index);
            }
            if (leaf.delta_key(index).compare(upper) == 0) {
                upper = leaf.delta_key(index);
            }
        }
        return frame_set_fences(bytes, page_bytes, &lower, &upper);
    }
    frame_set_inner_fence_pages(bytes, page_bytes, bounds.lower_leaf_page_id, bounds.upper_leaf_page_id);
    return true;
}

inline Status validate_native_snapshot_graph(std::vector<NativeFrame> *frames, uint64_t root_page_id)
{
    std::unordered_map<uint64_t, NativeFrame *> by_id;
    by_id.reserve(frames->size());
    for (NativeFrame &frame : *frames) {
        const uint64_t stored_page_id =
            frame.frame.empty() ? kInvalidPageId : frame_u64(frame.frame.data(), fh::kSelfpage_id);
        if (frame.page_id == kInvalidPageId || frame.frame.empty() ||
            (stored_page_id != kInvalidPageId && stored_page_id != frame.page_id) ||
            !by_id.emplace(frame.page_id, &frame).second) {
            return Status::corruption("native snapshot: duplicate, invalid, or mismatched page ID");
        }
    }
    const auto root = by_id.find(root_page_id);
    if (root == by_id.end() || frame_page_type(root->second->frame.data()) == page_type::kOverflowFrame) {
        return Status::corruption("native snapshot: root page is missing or has an invalid type");
    }

    auto validate_fences = [](NativeFrame *frame, const NativeBounds &bounds) {
        const uint8_t *bytes = frame->frame.data();
        if (!frame_has_lower_fence(bytes) && bounds.lower.has_value() && !set_native_frame_fences(frame, bounds)) {
            return false;
        }
        bytes = frame->frame.data();
        if (frame_has_lower_fence(bytes) != bounds.lower.has_value() ||
            frame_has_upper_fence(bytes) != bounds.upper.has_value()) {
            return false;
        }
        if (!bounds.lower.has_value()) {
            return true;
        }
        if (frame_fences_are_page_ids(bytes)) {
            return frame_lower_fence_page_id(bytes) == bounds.lower_leaf_page_id &&
                   frame_upper_fence_page_id(bytes) == bounds.upper_leaf_page_id;
        }
        return frame_lower_fence(bytes).compare(Slice(*bounds.lower)) == 0 &&
               frame_upper_fence(bytes).compare(Slice(*bounds.upper)) == 0;
    };

    std::unordered_set<uint64_t>     reached;
    std::unordered_set<uint64_t>     active;
    std::unordered_set<uint64_t>     overflow_reached;
    std::vector<const NativeFrame *> leaves;
    std::function<Status(uint64_t, const std::optional<std::string> &, const std::optional<std::string> &,
                         NativeBounds *)>
        walk = [&](uint64_t page_id, const std::optional<std::string> &expected_lower,
                   const std::optional<std::string> &expected_upper, NativeBounds *bounds) -> Status {
        if (active.contains(page_id) || reached.contains(page_id)) {
            return Status::corruption("native snapshot: cyclic or multiply referenced tree page");
        }
        const auto found = by_id.find(page_id);
        if (found == by_id.end()) {
            return Status::corruption("native snapshot: missing child page");
        }
        NativeFrame    &frame = *found->second;
        const page_type type  = frame_page_type(frame.frame.data());
        if (type == page_type::kOverflowFrame) {
            return Status::corruption("native snapshot: overflow page used as a tree child");
        }
        active.insert(page_id);
        reached.insert(page_id);

        if (type == page_type::kLeafBase) {
            LeafFrameView leaf(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()));
            leaves.push_back(&frame);
            bounds->lower_leaf_page_id = page_id;
            bounds->upper_leaf_page_id = page_id;
            auto remember_key          = [bounds](Slice key) {
                if (!bounds->lower.has_value() || key.compare(Slice(*bounds->lower)) < 0) {
                    bounds->lower = key.to_string();
                }
                if (!bounds->upper.has_value() || key.compare(Slice(*bounds->upper)) > 0) {
                    bounds->upper = key.to_string();
                }
            };
            for (uint32_t index = 0; index < leaf.count(); ++index) {
                remember_key(leaf.key(index));
            }
            for (uint32_t index = 0; index < leaf.delta_count(); ++index) {
                remember_key(leaf.delta_key(index));
            }
            if ((expected_lower.has_value() && bounds->lower.has_value() &&
                 Slice(*bounds->lower).compare(Slice(*expected_lower)) < 0) ||
                (expected_upper.has_value() && bounds->upper.has_value() &&
                 Slice(*bounds->upper).compare(Slice(*expected_upper)) >= 0)) {
                return Status::corruption("native snapshot: leaf escapes its routed bounds");
            }
            auto validate_cell = [&](Slice raw_cell) -> Status {
                CellView cell{raw_cell};
                if (!cell.valid() || (cell.is_overflow() && raw_cell.size() != kOverflowCellSize)) {
                    return Status::corruption("native snapshot: invalid leaf cell");
                }
                if (!cell.is_overflow()) {
                    return Status::Ok();
                }
                std::unordered_set<uint64_t> chain;
                uint64_t                     overflow_id = cell.overflow_head();
                while (overflow_id != kInvalidPageId) {
                    if (!chain.insert(overflow_id).second) {
                        return Status::corruption("native snapshot: cyclic overflow chain");
                    }
                    const auto overflow = by_id.find(overflow_id);
                    if (overflow == by_id.end() ||
                        frame_page_type(overflow->second->frame.data()) != page_type::kOverflowFrame) {
                        return Status::corruption("native snapshot: missing overflow page");
                    }
                    if (!overflow_reached.insert(overflow_id).second) {
                        break;
                    }
                    OverflowFrameView view(overflow->second->frame.data(),
                                           static_cast<uint32_t>(overflow->second->frame.size()));
                    overflow_id = view.next_page_id();
                }
                return Status::Ok();
            };
            for (uint32_t index = 0; index < leaf.count(); ++index) {
                Status status = validate_cell(leaf.cell(index));
                if (!status.ok()) {
                    return status;
                }
            }
            for (uint32_t index = 0; index < leaf.delta_count(); ++index) {
                Status status = validate_cell(leaf.delta_cell(index));
                if (!status.ok()) {
                    return status;
                }
            }
            if (!validate_fences(&frame, *bounds)) {
                return Status::corruption("native snapshot: leaf fences are missing or inconsistent");
            }
            active.erase(page_id);
            return Status::Ok();
        }

        InnerFrameView inner(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()));
        for (uint32_t index = 0; index < inner.num_children(); ++index) {
            std::optional<std::string> child_lower =
                index == 0 ? expected_lower : std::optional<std::string>(inner.separator_at(index - 1).to_string());
            std::optional<std::string> child_upper =
                index == inner.num_separators() ? expected_upper
                                                : std::optional<std::string>(inner.separator_at(index).to_string());
            if ((expected_lower.has_value() && child_lower.has_value() &&
                 Slice(*child_lower).compare(Slice(*expected_lower)) < 0) ||
                (expected_upper.has_value() && child_upper.has_value() &&
                 Slice(*child_upper).compare(Slice(*expected_upper)) > 0)) {
                return Status::corruption("native snapshot: inner separator escapes its routed bounds");
            }
            NativeBounds child;
            Status       child_status = walk(inner.child_at(index), child_lower, child_upper, &child);
            if (!child_status.ok()) {
                return child_status;
            }
            if (!bounds->lower.has_value() && child.lower.has_value()) {
                bounds->lower              = child.lower;
                bounds->lower_leaf_page_id = child.lower_leaf_page_id;
            }
            if (child.upper.has_value()) {
                bounds->upper              = child.upper;
                bounds->upper_leaf_page_id = child.upper_leaf_page_id;
            }
        }
        if (!validate_fences(&frame, *bounds)) {
            return Status::corruption("native snapshot: inner fences are missing or inconsistent");
        }
        active.erase(page_id);
        return Status::Ok();
    };

    NativeBounds root_bounds;
    Status       graph_status = walk(root_page_id, std::nullopt, std::nullopt, &root_bounds);
    if (!graph_status.ok()) {
        return graph_status;
    }
    for (size_t index = 0; index < leaves.size(); ++index) {
        LeafFrameView  leaf(leaves[index]->frame.data(), static_cast<uint32_t>(leaves[index]->frame.size()));
        const uint64_t expected = index + 1 < leaves.size() ? leaves[index + 1]->page_id : kInvalidPageId;
        if (leaf.right_sibling() != expected) {
            return Status::corruption("native snapshot: leaf sibling escapes tree order");
        }
    }
    if (reached.size() + overflow_reached.size() != frames->size()) {
        return Status::corruption("native snapshot: unreachable page frame");
    }
    return Status::Ok();
}

// Resolve a leaf chain (head -> ... -> LeafBase) to key-sorted entries by
// highest-slot-wins. Tombstones whose slot <= gc_floor are dropped (logical
// retention GC); all other tombstones are kept.
//
// This is the whole-page form, for the callers that genuinely need every live
// entry (collect_in_order for iter_all/compare, PinnedSnapshot::materialize,
// GC's live walk) -- O(N) is the right cost there. It is a thin loop over
// LeafChainCursor, which merges the chain's already-sorted streams lazily; the
// scan paths drive that cursor directly instead, so a limit-bounded scan pays
// O(limit) rather than O(entries-per-leaf). Each key/cell the cursor
// yields is borrowed from the chain's own resident storage and is copied into
// an owned leaf_entry exactly once, here.
} // namespace crowdb::tree::detail
