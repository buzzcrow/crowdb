// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/graph_summary.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(GraphSummary, CountsEachValidatedGraphFrameAndItsFullCapacity)
{
    TreeSummary       summary;
    ReadablePageFrame leaf{.type = page_type::kLeafBase, .frame_bytes = 4096, .backing = {}};
    leaf.summary.emplace();
    leaf.summary->live_counts_known = true;
    leaf.summary->live_keys         = 3;
    leaf.summary->live_key_bytes    = 9;
    leaf.summary->live_value_bytes  = 10000;
    ASSERT_TRUE(add_graph_summary(summary, leaf));
    ASSERT_TRUE(add_graph_summary(summary, {.type = page_type::kInnerBase, .frame_bytes = 2048, .backing = {}}));
    ASSERT_TRUE(add_graph_summary(summary, {.type = page_type::kOverflowFrame, .frame_bytes = 4096, .backing = {}}));
    ASSERT_TRUE(add_graph_summary(summary, {.type = page_type::kOverflowFrame, .frame_bytes = 4096, .backing = {}}));
    EXPECT_EQ(summary.live_kv, 3U);
    EXPECT_EQ(summary.live_key_bytes, 9U);
    EXPECT_EQ(summary.live_value_bytes, 10000U);
    EXPECT_EQ(summary.live_logical_bytes, 10009U);
    EXPECT_EQ(summary.reachable_leaf_pages, 1U);
    EXPECT_EQ(summary.reachable_inner_pages, 1U);
    EXPECT_EQ(summary.reachable_overflow_pages, 2U);
    EXPECT_EQ(summary.reachable_page_capacity_bytes, 14336U);
}

TEST(GraphSummary, UnknownOrOverflowingReceiptLeavesEveryCounterUnchanged)
{
    TreeSummary summary;
    summary.live_value_bytes = UINT64_MAX;
    ReadablePageFrame leaf{.type = page_type::kLeafBase, .frame_bytes = 4096, .backing = {}};
    EXPECT_FALSE(add_graph_summary(summary, leaf));
    leaf.summary.emplace();
    leaf.summary->live_counts_known = true;
    leaf.summary->live_keys         = 1;
    leaf.summary->live_key_bytes    = 4;
    leaf.summary->live_value_bytes  = 1;
    EXPECT_FALSE(add_graph_summary(summary, leaf));
    EXPECT_EQ(summary.live_value_bytes, UINT64_MAX);
    EXPECT_EQ(summary.live_kv, 0U);
    EXPECT_EQ(summary.live_key_bytes, 0U);
    EXPECT_EQ(summary.reachable_page_capacity_bytes, 0U);
    EXPECT_EQ(summary.reachable_leaf_pages, 0U);
}
} // namespace crowdb::tree
