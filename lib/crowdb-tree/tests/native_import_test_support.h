// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/native_import.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

namespace crowdb::tree::test_import
{
inline NativeFrame leaf(uint64_t pid, Slice key, Slice cell, uint64_t sibling = kInvalidPageId)
{
    NativeFrame      result{.page_id = pid, .frame = std::vector<uint8_t>(4096)};
    LeafFrameBuilder builder(result.frame.data(), result.frame.size());
    EXPECT_TRUE(builder.try_append_sorted(key, cell));
    builder.finish(pid, sibling);
    return result;
}

inline NativeFrame inline_leaf(uint64_t pid = 0, Slice key = "key", Slice value = "imported", uint64_t slot = 7)
{
    const auto cell = encode_cell_buf(slot, OpKind::kPut, value);
    return leaf(pid, key, Slice(cell));
}

inline std::vector<NativeFrame> overflow_tree(Slice value, uint64_t slot = 7)
{
    const auto               cell = encode_overflow_cell_buf(slot, 1, value.size());
    std::vector<NativeFrame> result;
    result.push_back(leaf(0, "key", Slice(cell)));
    size_t written = 0;
    while (written != value.size()) {
        const auto  count = std::min<size_t>(value.size() - written, overflow_chunk_cap(4096));
        const auto  pid   = result.size();
        const auto  next  = written + count == value.size() ? kInvalidPageId : pid + 1;
        NativeFrame frame{.page_id = pid, .frame = std::vector<uint8_t>(4096)};
        overflow_frame_build(frame.frame.data(), frame.frame.size(), pid, next,
                             reinterpret_cast<const uint8_t *>(value.data() + written), count);
        result.push_back(std::move(frame));
        written += count;
    }
    return result;
}

inline NativeStreamInfo info(const std::vector<NativeFrame> &frames, uint64_t coverage = 7)
{
    return {.root_page_id      = frames.empty() ? kInvalidPageId : frames.front().page_id,
            .at_slot           = coverage,
            .next_page_id      = frames.size(),
            .frame_count       = frames.size(),
            .frame_count_known = true};
}
} // namespace crowdb::tree::test_import
