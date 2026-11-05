// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_encode_internal.h"

#include <algorithm>
#include <cstring>

namespace crowdb::tree::detail
{
namespace
{
void encode_leaf(FinalPageEncodeState &state, uint8_t *output)
{
    const auto bytes = state.limits.frame_bytes;
    frame_put_u32(output, fh::kMagic, kFrameMagicLeaf);
    output[fh::kType] = static_cast<uint8_t>(page_type::kLeafBase);
    frame_put_u64(output, fh::kRightSibling, state.page->leaf->right_sibling);
    uint32_t hi = bytes - static_cast<uint32_t>(kFrameTrailerSize);
    for (size_t index = 0; index < state.entries.size(); ++index) {
        const auto &entry     = state.entries[index];
        const auto  key_bytes = static_cast<uint32_t>(entry.key.size());
        hi -= key_bytes + entry.encoded_cell_bytes;
        if (key_bytes != 0) {
            std::memcpy(output + hi, entry.key.data(), key_bytes);
        }
        state.counters.key_copy_bytes += key_bytes;
        auto *cell = output + hi + key_bytes;
        frame_put_u64(cell, 0, entry.cell.slot);
        cell[sizeof(uint64_t)] = entry.cell.flags;
        if (entry.overflow_frames != 0) {
            cell[sizeof(uint64_t)] = kFlagOverflow;
            frame_put_u64(cell, kCellHeaderSize, state.ids.first + entry.overflow_offset);
            frame_put_u64(cell, kCellHeaderSize + sizeof(uint64_t), entry.cell.value.size());
        }
        else if (entry.cell.overflow()) {
            std::memcpy(cell + kCellHeaderSize, entry.cell.encoded.data() + kCellHeaderSize,
                        kOverflowCellSize - kCellHeaderSize);
        }
        else if (!entry.cell.value.empty()) {
            std::memcpy(cell + kCellHeaderSize, entry.cell.value.data(), entry.cell.value.size());
            state.counters.inline_value_copy_bytes += entry.cell.value.size();
        }
        auto *slot = output + kFrameHeaderSize + (index * kLeafSlotSize);
        frame_put_u32(slot, 0, hi);
        frame_put_u32(slot, sizeof(uint32_t), key_bytes);
        frame_put_u32(slot, 2 * sizeof(uint32_t), entry.encoded_cell_bytes);
    }
    frame_put_u32(output, fh::kSlotCount, static_cast<uint32_t>(state.entries.size()));
    frame_put_u32(output, fh::kFreeLo,
                  static_cast<uint32_t>(kFrameHeaderSize + (state.entries.size() * kLeafSlotSize)));
    frame_put_u32(output, fh::kFreeHi, hi);
    if (!state.entries.empty()) {
        LeafFrameView view(output, bytes);
        const auto    lower = view.key(0);
        const auto    upper = view.key(view.count() - 1);
        (void)frame_set_fences(output, bytes, &lower, &upper);
    }
    else {
        frame_restamp_crc(output, bytes);
    }
}

void encode_inner(FinalPageEncodeState &state, uint8_t *output)
{
    const auto &inner = *state.page->inner;
    frame_put_u32(output, fh::kMagic, kFrameMagicInner);
    output[fh::kType] = static_cast<uint8_t>(page_type::kInnerBase);
    auto lo           = static_cast<uint32_t>(kFrameHeaderSize);
    for (const auto child : inner.children) {
        frame_put_u64(output, lo, child);
        lo += sizeof(uint64_t);
    }
    uint32_t hi = state.limits.frame_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    for (const auto &separator : inner.separators) {
        hi -= static_cast<uint32_t>(separator.size());
        if (separator.size() != 0) {
            std::copy_n(separator.data(), separator.size(), output + hi);
        }
        state.counters.key_copy_bytes += separator.size();
        frame_put_u32(output, lo, hi);
        frame_put_u32(output, lo + sizeof(uint32_t), static_cast<uint32_t>(separator.size()));
        lo += kInnerSlotSize;
    }
    frame_put_u32(output, fh::kSlotCount, static_cast<uint32_t>(inner.separators.size()));
    frame_put_u32(output, fh::kFreeLo, lo);
    frame_put_u32(output, fh::kFreeHi, hi);
    frame_set_inner_fence_pages(output, state.limits.frame_bytes, state.fences.lower_leaf, state.fences.upper_leaf);
}
} // namespace

void encode_primary(FinalPageEncodeState &state, uint8_t *output)
{
    std::memset(output, 0, state.limits.frame_bytes);
    output[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
    frame_put_u64(output, fh::kSelfpage_id, state.pid);
    if (state.page->level == 0) {
        encode_leaf(state, output);
    }
    else {
        encode_inner(state, output);
    }
}
} // namespace crowdb::tree::detail
