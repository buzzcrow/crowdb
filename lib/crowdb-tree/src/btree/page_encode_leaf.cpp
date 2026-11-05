// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_encode_internal.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
constexpr uint64_t kPageOverhead = kFrameHeaderSize + kFrameTrailerSize;

Status append_leaf_entry(FinalPageEncodeState &state, Slice key, LogicalCell cell, const CapturedEncodeContext &context,
                         PageEncodePreparation *result)
{
    if (!cell.owner || (cell.flags & ~(kFlagTombstone | kFlagOverflow)) != 0 ||
        (cell.tombstone() && (cell.overflow() || !cell.value.empty()))) {
        return Status::corruption("final leaf has an invalid or unowned logical cell");
    }
    const bool spill = !cell.tombstone() && (cell.overflow() || cell.value.size() > state.limits.inline_value_bytes);
    if (!spill && cell.value.size() > state.limits.frame_bytes) {
        return Status::resource_exhausted("inline value cannot fit the final primary frame");
    }
    const uint64_t cell_bytes = spill ? kOverflowCellSize : kCellHeaderSize + cell.value.size();
    const uint64_t available  = state.limits.frame_bytes - state.summary.primary_used_bytes;
    if (key.size() > available || cell_bytes > available - key.size() ||
        kLeafSlotSize > available - key.size() - cell_bytes) {
        result->needs_split = true;
        return Status::resource_exhausted("captured primary page needs structural split before encoding");
    }
    if (state.entries.size() == state.entries.capacity()) {
        return Status::resource_exhausted("final leaf descriptor budget exhausted");
    }
    FinalLeafEntry entry{.key                = key,
                         .cell               = std::move(cell),
                         .overflow_offset    = state.summary.new_overflow_frames,
                         .encoded_cell_bytes = static_cast<uint32_t>(cell_bytes)};
    uint64_t       value_bytes = entry.cell.value.size();
    if (entry.cell.overflow()) {
        const auto captured_backing = entry.cell.backing ? entry.cell.backing->overflow : nullptr;
        if (entry.cell.encoded.size() != kOverflowCellSize || (!captured_backing && !context.overflow_dependencies)) {
            return Status::invalid_argument("reused overflow needs its captured backend dependency owner");
        }
        const CellView overflow(entry.cell.encoded);
        value_bytes = overflow.overflow_len();
        if (overflow.overflow_head() >= MappingView::kPageLimit || value_bytes == 0) {
            return Status::corruption("invalid captured overflow pointer");
        }
        state.reused.push_back({.head    = overflow.overflow_head(),
                                .bytes   = value_bytes,
                                .owner   = captured_backing ? nullptr : context.overflow_dependencies,
                                .backing = captured_backing});
    }
    else if (spill) {
        const uint64_t cap    = overflow_chunk_cap(state.limits.frame_bytes);
        entry.overflow_frames = (value_bytes / cap) + static_cast<uint64_t>(value_bytes % cap != 0);
        if (entry.overflow_frames > MappingView::kPageLimit - state.summary.new_overflow_frames) {
            return Status::resource_exhausted("encoded overflow identity count exceeds the namespace");
        }
        state.summary.new_overflow_frames += entry.overflow_frames;
    }
    if (!entry.cell.tombstone()) {
        if (value_bytes > UINT64_MAX - state.summary.live_value_bytes ||
            key.size() > UINT64_MAX - state.summary.live_key_bytes) {
            return Status::resource_exhausted("final page logical summary overflow");
        }
        ++state.summary.live_keys;
        state.summary.live_key_bytes += key.size();
        state.summary.live_value_bytes += value_bytes;
    }
    state.summary.primary_used_bytes += key.size() + cell_bytes + kLeafSlotSize;
    ++state.summary.encoded_entries;
    state.entries.push_back(std::move(entry));
    return Status::Ok();
}
} // namespace

Status prepare_leaf(FinalPageEncodeState &state, uint64_t coverage, uint64_t gc_floor,
                    const CapturedEncodeContext &context, PageEncodePreparation *result)
{
    const auto &page = state.page->leaf;
    if (!page || state.page->inner || page->range.lower != context.range.lower ||
        page->range.upper != context.range.upper) {
        return Status::invalid_argument("final leaf must match its captured routing range");
    }
    state.context  = context;
    state.gc_floor = gc_floor;
    state.cursor.emplace(page, context.range, coverage, state.limits.max_sources, false, state.limits.metadata);
    const size_t capacity =
        std::min({state.limits.max_entries,
                  static_cast<size_t>((state.limits.frame_bytes - kPageOverhead) / (kLeafSlotSize + kCellHeaderSize)),
                  state.limits.max_metadata_bytes / (sizeof(FinalLeafEntry) + sizeof(ReusedOverflowDependency))});
    state.entries.reserve(capacity);
    state.reused.reserve(capacity);
    state.summary.primary_used_bytes = kPageOverhead;
    return advance_leaf(state, result);
}

Status advance_leaf(FinalPageEncodeState &state, PageEncodePreparation *result)
{
    if (!state.cursor) {
        return Status::invalid_argument("leaf encoder has no preparation cursor");
    }
    auto &cursor = *state.cursor;
    if (!cursor.status().ok()) {
        return cursor.status();
    }
    while (cursor.valid()) {
        if (++state.visited > state.limits.max_work_entries) {
            return Status::resource_exhausted("final leaf winner work budget exhausted");
        }
        auto cell = cursor.cell();
        if (cell.tombstone() && cell.slot <= state.gc_floor) {
            ++state.summary.dropped_tombstones;
        }
        else {
            auto status = append_leaf_entry(state, cursor.key(), std::move(cell), state.context, result);
            if (!status.ok()) {
                return status;
            }
        }
        cursor.next();
    }
    if (!cursor.status().ok()) {
        return cursor.status();
    }
    if (const auto index = cursor.needs_base()) {
        result->needs_base = {.page = state.page->leaf, .base_index = *index};
        return Status::Ok();
    }
    state.prepared = true;
    state.cursor.reset();
    state.context                   = {};
    state.summary.live_counts_known = true;
    return Status::Ok();
}

} // namespace crowdb::tree::detail
