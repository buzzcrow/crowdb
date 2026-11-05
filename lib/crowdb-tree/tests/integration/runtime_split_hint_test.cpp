// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(RuntimeSplitHint, OverlayHintDoesNotLoadEncodeOrAdvanceCoverage)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    Crowdbtree tree(config);
    EXPECT_FALSE(tree.approximate_split_key());
    ASSERT_TRUE(tree.apply(1,
                           Batch{
                               .ops = {{.key = "a", .kind = OpKind::kPut, .value = "a"},
                                       {.key = "b", .kind = OpKind::kPut, .value = "b"},
                                       {.key = "c", .kind = OpKind::kPut, .value = "c"}}
    })
                    .ok());
    EXPECT_FALSE(tree.approximate_split_key());
    ASSERT_TRUE(tree.flush().ok());
    const auto before  = tree.version();
    const auto metrics = TreeCoreAccess::get(tree)->publisher().work_metrics();
    const auto loads   = metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count;
    const auto encodes = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    EXPECT_EQ(tree.approximate_split_key(), std::optional<std::string>("b"));
    EXPECT_EQ(tree.version(), before);
    EXPECT_EQ(tree.last_applied_slot(), 1U);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, loads);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encodes);
}

TEST(RuntimeSplitHint, EmptyAndBinaryRangeEndpointsAreExcluded)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.key_range = KeyRange::bounded(std::string{}, std::string("\0c", 2));
    Crowdbtree        tree(config);
    const std::string middle("\0b", 2);
    ASSERT_TRUE(tree.apply(1,
                           Batch{
                               .ops = {{.key = "", .kind = OpKind::kPut, .value = "a"},
                                       {.key = middle, .kind = OpKind::kPut, .value = "b"}}
    })
                    .ok());
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.approximate_split_key(), std::optional(middle));
}
} // namespace crowdb::tree
