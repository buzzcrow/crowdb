// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
namespace
{
struct TestFacadeCheckpoint
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;
    Config                          config;
    std::unique_ptr<Crowdbtree>     tree;

    TestFacadeCheckpoint()
    {
        store.device            = device;
        config.page_store       = device.get();
        config.async_page_store = &store;
        config.frame_bytes      = 4096;
        config.leaf_split_bytes = 3000;
        config.max_inline_value = 64;
    }

    void open()
    {
        const auto status = Crowdbtree::open(config, &tree);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
};

TEST(FacadeCheckpointDiagnostics, PhysicalWritesExcludeCleanReuseAndIncludeOverflow)
{
    TestFacadeCheckpoint test;
    test.open();
    ASSERT_TRUE(test.tree);
    EXPECT_EQ(test.tree->last_snapshot_pages_written(), 0U);
    ASSERT_TRUE(test.tree->put("large", std::string(12000, 'v')).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_GT(test.tree->last_snapshot_pages_written(), 1U);
    EXPECT_EQ(test.tree->stats().snapshot_pages_written, test.tree->last_snapshot_pages_written());
    const auto written = test.tree->last_snapshot_pages_written();
    EXPECT_EQ(test.tree->stats().snapshot_pages_total, written);
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.tree->last_snapshot_pages_written(), 0U);
    EXPECT_EQ(test.tree->stats().snapshot_pages_total, written);
    EXPECT_EQ(test.tree->last_snapshot_segments_written(), 0U);
    EXPECT_EQ(test.tree->scan_profile().count, 0U);
    test.tree.reset();
    test.open();
    ASSERT_TRUE(test.tree);
    EXPECT_EQ(test.tree->get_view("large").value().to_string(), std::string(12000, 'v'));
}

TEST(FacadeCheckpointDiagnostics, FailedAndStagedWritesPreserveLastDurableObservation)
{
    TestFacadeCheckpoint test;
    test.open();
    ASSERT_TRUE(test.tree);
    ASSERT_TRUE(test.tree->put("a", "one").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    const auto first = test.tree->last_snapshot_pages_written();
    ASSERT_GT(first, 0U);
    ASSERT_TRUE(test.tree->put("a", "two").ok());
    test.store.fail_at = test.store.events.size() + 1;
    EXPECT_FALSE(test.tree->snapshot().ok());
    EXPECT_EQ(test.tree->last_snapshot_pages_written(), first);
    test.store.fail_at = 0;
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 2U);
    const auto repaired = test.tree->last_snapshot_pages_written();
    ASSERT_TRUE(test.tree->put("a", "three").ok());
    auto                                *core = TreeCoreAccess::get(*test.tree);
    std::shared_ptr<CheckpointOperation> writeback;
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    const auto status = detail::wait_snapshot_operation(writeback);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(writeback->result().writeback_completed);
    EXPECT_EQ(test.tree->last_snapshot_pages_written(), repaired);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 2U);
    writeback.reset();
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 3U);
    EXPECT_EQ(test.tree->last_snapshot_pages_written(), 0U);
}

TEST(FacadeCheckpointDiagnostics, SmallerDefaultFrameDerivesSplitButRejectsExplicitInvalidGeometry)
{
    TestFacadeCheckpoint test;
    test.config.leaf_split_bytes = Config{}.leaf_split_bytes;
    test.open();
    ASSERT_TRUE(test.tree);
    EXPECT_LT(TreeCoreAccess::get(*test.tree)->config().leaf_split_bytes, test.config.frame_bytes);
    ASSERT_TRUE(test.tree->put("key", "value").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    test.config.frame_bytes = 256;
    auto status             = Crowdbtree::open(test.config, &test.tree);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(test.tree);
}
} // namespace
} // namespace crowdb::tree
