// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/range_rebuild.h"

#include "crowdb-tree/cell.h"
#include "crowdb-tree/frame_page.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace crowdb::tree
{
namespace
{

struct RebuildEntry
{
    std::string key;
    std::string cell;
};

struct RebuildLeaf
{
    uint64_t                  source_page_id = kInvalidPageId;
    uint64_t                  page_id        = kInvalidPageId;
    uint64_t                  source_right   = kInvalidPageId;
    bool                      wholly_contained{false};
    std::vector<RebuildEntry> entries;
};

struct RebuildNode
{
    uint64_t    page_id = kInvalidPageId;
    std::string first_key;
};

using FrameMap = std::unordered_map<uint64_t, const NativeFrame *>;

Status copy_overflow_chain(uint64_t head, const FrameMap &source_by_id, std::unordered_set<uint64_t> *copied,
                           std::vector<NativeFrame> *output, RangeRebuildStats *stats)
{
    std::unordered_set<uint64_t> chain;
    uint64_t                     page_id = head;
    while (page_id != kInvalidPageId) {
        if (!chain.insert(page_id).second) {
            return Status::corruption("range rebuild: cyclic overflow chain");
        }
        if (copied->contains(page_id)) {
            return Status::Ok();
        }
        const auto found = source_by_id.find(page_id);
        if (found == source_by_id.end() || frame_page_type(found->second->frame.data()) != page_type::kOverflowFrame) {
            return Status::corruption("range rebuild: missing overflow frame");
        }
        const NativeFrame &frame = *found->second;
        output->push_back(frame);
        copied->insert(page_id);
        ++stats->pages_reused;
        OverflowFrameView overflow(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()));
        page_id = overflow.next_page_id();
    }
    return Status::Ok();
}

void collect_rebuild_leaves(const std::vector<NativeFrame> &source_frames, const KeyRange &range,
                            uint64_t *next_page_id, std::vector<RebuildLeaf> *leaves, RangeRebuildStats *stats)
{
    for (const NativeFrame &frame : source_frames) {
        *next_page_id = std::max(*next_page_id, frame.page_id + 1);
        if (frame_page_type(frame.frame.data()) != page_type::kLeafBase) {
            continue;
        }
        LeafFrameView leaf(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()));
        RebuildLeaf   rebuild_leaf;
        rebuild_leaf.source_page_id   = frame.page_id;
        rebuild_leaf.source_right     = leaf.right_sibling();
        rebuild_leaf.wholly_contained = !leaf.empty();
        for (uint32_t index = 0; index < leaf.count(); ++index) {
            Slice key = leaf.key(index);
            ++stats->entries_examined;
            if (!range.contains(key)) {
                ++stats->entries_filtered;
                rebuild_leaf.wholly_contained = false;
                continue;
            }
            Slice cell = leaf.cell(index);
            rebuild_leaf.entries.push_back({.key = key.to_string(), .cell = std::string(cell.data(), cell.size())});
            ++stats->entries_emitted;
        }
        if (!rebuild_leaf.entries.empty()) {
            leaves->push_back(std::move(rebuild_leaf));
        }
    }
    std::sort(leaves->begin(), leaves->end(), [](const RebuildLeaf &left, const RebuildLeaf &right) {
        return left.entries.front().key < right.entries.front().key;
    });
    for (RebuildLeaf &leaf : *leaves) {
        leaf.page_id = leaf.wholly_contained ? leaf.source_page_id : (*next_page_id)++;
    }
}

Status build_leaf_frames(const std::vector<NativeFrame> &source_frames, uint32_t frame_bytes,
                         std::vector<RebuildLeaf> *leaves, uint64_t *next_page_id, std::vector<NativeFrame> *output,
                         std::vector<RebuildNode> *level, RangeRebuildStats *stats)
{
    FrameMap source_by_id;
    for (const NativeFrame &frame : source_frames) {
        source_by_id.emplace(frame.page_id, &frame);
    }
    std::unordered_set<uint64_t> copied_overflow;

    if (leaves->empty()) {
        const uint64_t   page_id = (*next_page_id)++;
        NativeFrame      frame{.page_id = page_id, .frame = std::vector<uint8_t>(frame_bytes)};
        LeafFrameBuilder builder(frame.frame.data(), frame_bytes);
        builder.finish(page_id, kInvalidPageId);
        output->push_back(std::move(frame));
        level->push_back({.page_id = page_id, .first_key = {}});
        ++stats->pages_rebuilt;
        return Status::Ok();
    }

    for (size_t leaf_index = 0; leaf_index < leaves->size(); ++leaf_index) {
        RebuildLeaf &leaf        = (*leaves)[leaf_index];
        uint64_t   right_sibling = leaf_index + 1 < leaves->size() ? (*leaves)[leaf_index + 1].page_id : kInvalidPageId;
        const auto source        = source_by_id.find(leaf.source_page_id);
        bool       can_reuse     = leaf.wholly_contained && source != source_by_id.end() &&
                                   source->second->frame.size() == frame_bytes && leaf.source_right == right_sibling;
        if (can_reuse) {
            output->push_back(*source->second);
            ++stats->pages_reused;
        }
        else {
            NativeFrame      frame{.page_id = leaf.page_id, .frame = std::vector<uint8_t>(frame_bytes)};
            LeafFrameBuilder builder(frame.frame.data(), frame_bytes);
            for (const RebuildEntry &entry : leaf.entries) {
                if (!builder.try_append_sorted(Slice(entry.key), Slice(entry.cell))) {
                    return Status::resource_exhausted("range rebuild: filtered leaf does not fit destination frame");
                }
            }
            builder.finish(leaf.page_id, right_sibling);
            output->push_back(std::move(frame));
            ++stats->pages_rebuilt;
        }

        for (const RebuildEntry &entry : leaf.entries) {
            CellView cell{Slice(entry.cell)};
            if (!cell.valid()) {
                return Status::corruption("range rebuild: invalid leaf cell");
            }
            if (!cell.is_overflow()) {
                continue;
            }
            if (entry.cell.size() != kOverflowCellSize) {
                return Status::corruption("range rebuild: invalid overflow cell");
            }
            Status copy_status =
                copy_overflow_chain(cell.overflow_head(), source_by_id, &copied_overflow, output, stats);
            if (!copy_status.ok()) {
                return copy_status;
            }
        }
        level->push_back({.page_id = leaf.page_id, .first_key = leaf.entries.front().key});
    }
    return Status::Ok();
}

Status build_inner_frames(uint32_t frame_bytes, uint32_t inner_max_keys, uint64_t *next_page_id,
                          std::vector<RebuildNode> *level, std::vector<NativeFrame> *output, RangeRebuildStats *stats)
{
    const size_t max_children = std::max<size_t>(2, static_cast<size_t>(inner_max_keys) + 1);
    while (level->size() > 1) {
        std::vector<RebuildNode> parents;
        size_t                   begin = 0;
        while (begin < level->size()) {
            size_t count = std::min(max_children, level->size() - begin);
            if (level->size() - (begin + count) == 1 && count > 2) {
                --count;
            }

            bool        built = false;
            NativeFrame frame;
            while (count != 0) {
                frame.page_id = (*next_page_id)++;
                frame.frame.assign(frame_bytes, 0);
                std::vector<uint64_t> children;
                std::vector<Slice>    separators;
                children.reserve(count);
                separators.reserve(count - 1);
                for (size_t index = 0; index < count; ++index) {
                    const RebuildNode &child = (*level)[begin + index];
                    children.push_back(child.page_id);
                    if (index != 0) {
                        separators.emplace_back(child.first_key);
                    }
                }
                if (inner_frame_build(frame.frame.data(), frame_bytes, frame.page_id, children, separators)) {
                    built = true;
                    break;
                }
                --count;
            }
            if (!built) {
                return Status::resource_exhausted("range rebuild: inner separator does not fit destination frame");
            }
            parents.push_back({.page_id = frame.page_id, .first_key = (*level)[begin].first_key});
            output->push_back(std::move(frame));
            ++stats->pages_rebuilt;
            begin += count;
        }
        *level = std::move(parents);
    }
    return Status::Ok();
}

Status build_filtered_frames(const std::vector<NativeFrame> &source_frames, const KeyRange &range,
                             const Options &destination_options, std::vector<NativeFrame> *output,
                             uint64_t *root_page_id, RangeRebuildStats *stats)
{
    uint64_t                 next_page_id = 0;
    std::vector<RebuildLeaf> leaves;
    collect_rebuild_leaves(source_frames, range, &next_page_id, &leaves, stats);

    std::vector<RebuildNode> level;
    Status leaf_status = build_leaf_frames(source_frames, destination_options.frame_bytes, &leaves, &next_page_id,
                                           output, &level, stats);
    if (!leaf_status.ok()) {
        return leaf_status;
    }
    Status inner_status = build_inner_frames(destination_options.frame_bytes, destination_options.inner_max_keys,
                                             &next_page_id, &level, output, stats);
    if (!inner_status.ok()) {
        return inner_status;
    }
    *root_page_id = level.front().page_id;
    return Status::Ok();
}

} // namespace

Status rebuild_range(Crowdbtree &source, const KeyRange &range, Options destination_options,
                     std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats)
{
    if (out == nullptr || destination_options.page_store == nullptr) {
        return Status::invalid_argument("range rebuild requires an output and durable destination store");
    }
    Status range_status = range.validate();
    if (!range_status.ok()) {
        return range_status;
    }

    destination_options.key_range = range;
    std::unique_ptr<Crowdbtree> destination;
    Status                      open_status = Crowdbtree::open(destination_options, &destination);
    if (!open_status.ok()) {
        return open_status;
    }

    RangeRebuildStats        local;
    std::vector<NativeFrame> source_frames;
    uint64_t                 source_root   = kInvalidPageId;
    uint64_t                 at_slot       = 0;
    Status                   native_status = source.collect_native_frames(&source_frames, &source_root, &at_slot);
    if (!native_status.ok()) {
        return native_status;
    }
    if (source_frames.empty()) {
        return Status::corruption("range rebuild: source snapshot has no root");
    }
    const uint32_t source_frame_bytes = static_cast<uint32_t>(source_frames.front().frame.size());
    if (source_frame_bytes != destination_options.frame_bytes ||
        std::any_of(source_frames.begin(), source_frames.end(), [source_frame_bytes](const NativeFrame &frame) {
            return frame.frame.size() != source_frame_bytes;
        })) {
        return Status::invalid_argument("range rebuild requires matching fixed frame sizes");
    }

    bool all_contained = true;
    for (const NativeFrame &frame : source_frames) {
        if (frame_page_type(frame.frame.data()) != page_type::kLeafBase) {
            continue;
        }
        LeafFrameView leaf(frame.frame.data(), source_frame_bytes);
        for (uint32_t index = 0; index < leaf.count(); ++index) {
            ++local.entries_examined;
            if (!range.contains(leaf.key(index))) {
                ++local.entries_filtered;
                all_contained = false;
            }
        }
    }

    std::vector<NativeFrame> output_frames;
    uint64_t                 output_root = source_root;
    if (all_contained) {
        local.entries_emitted = local.entries_examined;
        local.pages_reused    = source_frames.size();
        output_frames         = std::move(source_frames);
    }
    else {
        local = {};
        native_status =
            build_filtered_frames(source_frames, range, destination_options, &output_frames, &output_root, &local);
        if (!native_status.ok()) {
            return native_status;
        }
    }

    native_status = destination->install_snapshot_native(std::move(output_frames), output_root, at_slot);
    if (!native_status.ok()) {
        return native_status;
    }
    Status snapshot_status = destination->snapshot();
    if (!snapshot_status.ok()) {
        return snapshot_status;
    }
    if (stats != nullptr) {
        *stats = local;
    }
    *out = std::move(destination);
    return Status::Ok();
}

} // namespace crowdb::tree
