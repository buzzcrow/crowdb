// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_physical_registry.h"
#include "btree/backing_stats_sampler.h"
#include "common/test_tree_core_chunk.h"

namespace crowdb::tree
{
TEST(ChunkBackingVisibility, PublicNextOwnerKeepsTheLedgerAfterThePreviousTreeCollectorIsDestroyed)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, std::string(9000, 'a')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto *core = TreeCoreAccess::get(*test.tree);
    core->binding()->cpu()->drain();
    const auto sampler = core->backing_sampler();
    const auto before  = sampler.snapshot();
    ASSERT_NE(before.scope_id, 0U);
    ASSERT_GT(before.allocated_bytes, 0U);
    auto old = test.tree->snapshot_view();
    ASSERT_NE(old, nullptr);
    test.tree.reset();
    EXPECT_EQ(sampler.snapshot().scope_id, before.scope_id);
    EXPECT_EQ(sampler.snapshot().allocated_bytes, before.allocated_bytes);
    ASSERT_TRUE(test.open().ok());
    core = TreeCoreAccess::get(*test.tree);
    core->binding()->cpu()->drain();
    const auto reopened = test.tree->backing_stats();
    EXPECT_EQ(reopened.scope_id, before.scope_id);
    EXPECT_EQ(reopened.allocated_bytes, before.allocated_bytes);
    EXPECT_EQ(test.tree->tree_state_stats().resources.allocated_backing_bytes, before.allocated_bytes);
    EXPECT_GT(test.tree->tree_state_stats().resources.allocated_bytes, 0U);
    ASSERT_TRUE(test.put(2, std::string(12000, 'b')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    core->binding()->cpu()->drain();
    const auto newer = test.tree->backing_stats();
    EXPECT_EQ(newer.scope_id, before.scope_id);
    EXPECT_GT(newer.allocated_bytes, before.allocated_bytes);
    uint64_t actual = 0;
    for (const auto &[identity, receipt] : test.backend.transport->receipts) {
        (void)identity;
        actual += receipt.physical_bytes;
    }
    EXPECT_EQ(newer.allocated_bytes, actual);
    EXPECT_EQ(sampler.snapshot().allocated_bytes, actual);
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(old->get("key", &slot, &value));
    EXPECT_EQ(value, std::string(9000, 'a'));
    EXPECT_EQ(slot, 1U);
    old.reset();
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    TreeCoreAccess::get(*test.tree)->binding()->cpu()->drain();
    EXPECT_EQ(test.tree->backing_stats().scope_id, before.scope_id);
    EXPECT_EQ(test.tree->backing_stats().allocated_bytes, actual);
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), std::string(12000, 'b'));
}
} // namespace crowdb::tree
