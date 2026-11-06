// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void check_live_only_reuse(size_t value_bytes)
{
    MemPageStore store;
    Config       config;
    config.page_store  = &store;
    config.frame_bytes = 65536;
    const std::string value(value_bytes, 'v');
    {
        Crowdbtree tree(config);
        for (uint64_t slot = 1; slot <= 3; ++slot) {
            ASSERT_TRUE(
                tree.apply(slot,
                           Batch{.ops = {{.key = "key" + std::to_string(slot), .kind = OpKind::kPut, .value = value}}})
                    .ok());
        }
        uint64_t durable = 0;
        ASSERT_TRUE(tree.snapshot(&durable).ok());
        ASSERT_EQ(durable, 3U);
        tree.set_gc_watermark(3, 3);
        ASSERT_TRUE(tree.snapshot(&durable).ok());
        EXPECT_EQ(durable, 3U);
        EXPECT_EQ(tree.last_snapshot_pages_written(), 0U);
    }
    Crowdbtree recovered(config);
    for (uint64_t slot = 1; slot <= 3; ++slot) {
        auto result = recovered.get_view("key" + std::to_string(slot));
        ASSERT_TRUE(result.found());
        EXPECT_EQ(result.slot(), slot);
        EXPECT_EQ(result.value().to_string(), value);
    }
}

} // namespace

TEST(CheckpointRetentionReuse, AdvancingFloorWithoutDeletesDoesNotRewriteStoredValues)
{
    check_live_only_reuse(16384);
    check_live_only_reuse(98304);
}

TEST(CheckpointRetentionReuse, AdvancingFloorStillRewritesAStoredTombstoneBeforeRecovery)
{
    MemPageStore store;
    Config       config;
    config.page_store  = &store;
    config.frame_bytes = 65536;
    {
        Crowdbtree tree(config);
        ASSERT_TRUE(tree.apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "old"}}}).ok());
        ASSERT_TRUE(tree.apply(2, Batch{.ops = {{.key = "b", .kind = OpKind::kPut, .value = "live"}}}).ok());
        ASSERT_TRUE(tree.apply(3, Batch{.ops = {{.key = "a", .kind = OpKind::kDelete, .value = ""}}}).ok());
        uint64_t durable = 0;
        ASSERT_TRUE(tree.snapshot(&durable).ok());
        ASSERT_EQ(durable, 3U);
        tree.set_gc_watermark(3, 3);
        ASSERT_TRUE(tree.snapshot(&durable).ok());
        EXPECT_GT(tree.last_snapshot_pages_written(), 0U);
        EXPECT_EQ(durable, 3U);
    }
    Crowdbtree recovered(config);
    EXPECT_FALSE(recovered.get_view("a").found());
    auto result = recovered.get_view("b");
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.slot(), 2U);
    EXPECT_EQ(result.value().to_string(), "live");
}
} // namespace crowdb::tree
