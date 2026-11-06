// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/graph_summary.h"

namespace crowdb::tree
{
namespace
{
bool add(uint64_t &value, uint64_t amount) noexcept
{
    if (amount > UINT64_MAX - value) {
        return false;
    }
    value += amount;
    return true;
}
} // namespace

bool add_graph_summary(TreeSummary &summary, const ReadablePageFrame &frame,
                       const FinalPageSummary *verified_leaf) noexcept
{
    auto next = summary;
    if (frame.frame_bytes == 0 || !add(next.reachable_page_capacity_bytes, frame.frame_bytes)) {
        return false;
    }
    switch (frame.type) {
    case page_type::kLeafBase: {
        const auto *leaf = verified_leaf;
        if (leaf == nullptr && frame.summary) {
            leaf = &*frame.summary;
        }
        if (leaf == nullptr || !leaf->live_counts_known || !add(next.reachable_leaf_pages, 1) ||
            !add(next.live_kv, leaf->live_keys) || !add(next.live_key_bytes, leaf->live_key_bytes) ||
            !add(next.live_value_bytes, leaf->live_value_bytes) ||
            !add(next.live_logical_bytes, leaf->live_key_bytes) ||
            !add(next.live_logical_bytes, leaf->live_value_bytes)) {
            return false;
        }
        break;
    }
    case page_type::kInnerBase:
        if (!add(next.reachable_inner_pages, 1)) {
            return false;
        }
        break;
    case page_type::kOverflowFrame:
        if (!add(next.reachable_overflow_pages, 1)) {
            return false;
        }
        break;
    default:
        return false;
    }
    summary = next;
    return true;
}
} // namespace crowdb::tree
