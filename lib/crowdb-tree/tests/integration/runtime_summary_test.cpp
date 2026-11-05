// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"

namespace crowdb::tree
{
namespace
{
Batch summary_data()
{
    Batch batch;
    for (size_t i = 0; i < 80; ++i) {
        batch.ops.push_back(
            {.key = std::to_string(1000 + i), .kind = OpKind::kPut, .value = std::string(i == 0 ? 20000 : 60, 'x')});
    }
    return batch;
}

void check_summary(const Crowdbtree &tree, uint64_t keys, uint64_t key_bytes, uint64_t values)
{
    const auto summary = tree.tree_summary();
    ASSERT_TRUE(summary.exact);
    EXPECT_EQ(summary.live_kv, keys);
    EXPECT_EQ(summary.live_key_bytes, key_bytes);
    EXPECT_EQ(summary.live_value_bytes, values);
    EXPECT_EQ(summary.live_logical_bytes, key_bytes + values);
    EXPECT_GT(summary.reachable_leaf_pages, 1U);
    EXPECT_GT(summary.reachable_inner_pages, 0U);
    EXPECT_GT(summary.reachable_overflow_pages, 0U);
    EXPECT_EQ(summary.reachable_page_capacity_bytes,
              4096 * (summary.reachable_leaf_pages + summary.reachable_inner_pages + summary.reachable_overflow_pages));
}
} // namespace

TEST(RuntimeSummary, CheckpointAndRecoveryExposeCompleteCountsWithoutLoadingPages)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->snapshot().ok());
    check_summary(*test.tree, 80, 320, 20000 + (79 * 60));
    const auto saved = test.tree->tree_summary();
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    const auto metrics = TreeCoreAccess::get(*test.tree)->publisher().work_metrics();
    const auto loads   = metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count;
    const auto encodes = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    check_summary(*test.tree, 80, 320, 20000 + (79 * 60));
    EXPECT_EQ(test.tree->tree_summary().reachable_page_capacity_bytes, saved.reachable_page_capacity_bytes);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, loads);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encodes);
}

TEST(RuntimeSummary, UnflushedAndPublishedWritesInvalidateOnlyTheCurrentSummary)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto old = TreeCoreAccess::get(*test.tree)->capture();
    if (!old->summary) {
        FAIL() << "fixed summary missing after checkpoint";
    }
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "1001", .kind = OpKind::kDelete, .value = {}}}}).ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->snapshot().ok());
    check_summary(*test.tree, 79, 316, 20000 + (78 * 60));
    EXPECT_EQ(old->summary->live_kv, 80U);
    EXPECT_EQ(old->summary->live_value_bytes, 20000 + (79 * 60));
}
} // namespace crowdb::tree
