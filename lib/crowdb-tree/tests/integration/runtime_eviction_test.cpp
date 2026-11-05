// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"

namespace crowdb::tree
{
TEST(RuntimeEviction, CleanRoutingAndLeafCacheEvictIndependentlyWithoutLosingOldReaders)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    Batch batch;
    for (size_t i = 0; i < 80; ++i) {
        batch.ops.push_back({.key = std::to_string(1000 + i), .kind = OpKind::kPut, .value = std::string(60, 'x')});
    }
    ASSERT_TRUE(test.tree->apply(1, batch).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto *const core = TreeCoreAccess::get(*test.tree);
    const auto  old  = core->capture();
    ASSERT_GT(old->stats.inner_pages, 0U);
    ASSERT_TRUE(old->mapping.get(old->root_page_id)->inner);
    EXPECT_EQ(test.tree->evict_clean_inner(old->stats.inner_pages), 0U);
    EXPECT_GT(test.tree->evict_clean_inner(0), 0U);
    EXPECT_FALSE(core->capture()->mapping.get(old->root_page_id)->inner);
    EXPECT_TRUE(old->mapping.get(old->root_page_id)->inner);
    uint64_t    slot = 0;
    std::string value;
    ASSERT_TRUE(test.tree->get("1000", &slot, &value));
    EXPECT_EQ(value, std::string(60, 'x'));
    const auto metrics = core->publisher().work_metrics();
    const auto loads   = metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count;
    const auto encodes = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    EXPECT_GT(test.tree->evict_clean_leaves(0), 0U);
    ASSERT_TRUE(test.tree->get("1000", &slot, &value));
    EXPECT_EQ(value, std::string(60, 'x'));
    EXPECT_GT(metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, loads);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encodes);
    EXPECT_EQ(test.tree->last_applied_slot(), 1U);
}

TEST(RuntimeEviction, UnbackedDirtyDataSurvivesBothZeroBudgets)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.evict_clean_leaves(0), 0U);
    EXPECT_EQ(tree.evict_clean_inner(0), 0U);
    uint64_t    slot = 0;
    std::string value;
    ASSERT_TRUE(tree.get("key", &slot, &value));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(value, "value");
}
} // namespace crowdb::tree
