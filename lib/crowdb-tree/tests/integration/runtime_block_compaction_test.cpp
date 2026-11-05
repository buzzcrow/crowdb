// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_checkpoint_internal.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(RuntimeBlockCompaction, DisabledPublicFacadeDoesNotWriteACheckpoint)
{
    tree_test::TempDir              directory("runtime_gc_disabled_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store = store.get();
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    MergeGcStats stats{.blocks_selected = 99};
    ASSERT_TRUE(tree->compact_sparse_blocks(&stats).ok());
    EXPECT_EQ(stats.blocks_selected, 0U);
    EXPECT_EQ(tree->durable_snapshot_seq(), 0U);
}

TEST(RuntimeBlockCompaction, PublicPassRelocatesSparseStorageAndReopens)
{
    tree_test::TempDir              directory("runtime_gc_public_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store                           = store.get();
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.compression                          = compress_algo::kNone;
    config.merge_gc_max_relocation_bytes        = 1; // The first block is always allowed.
    config.runtime_binding.limits.io_operations = 1;
    config.runtime_binding.cpu_queue_limit      = 3;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    core->local_backend()->gc_active.store(true);
    ASSERT_TRUE(
        core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = std::string(100000, 'a')}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core->binding()->drain();
    ASSERT_TRUE(core->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "kept"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core->binding()->drain();
    ASSERT_TRUE(core->checkpoint().ok());
    core->binding()->drain();
    core->local_backend()->gc_active.store(false);
    MergeGcStats stats;
    auto         status = core->compact_sparse_blocks(&stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    core->binding()->drain();
    EXPECT_GT(stats.blocks_deleted, 0U);
    EXPECT_EQ(stats.blocks_selected, 1U);
    EXPECT_GT(stats.pages_relocated, 0U);
    EXPECT_EQ(core->local_backend()->checkpoints->durable_coverage(), 2U);
    GetView value;
    ASSERT_TRUE(core->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), "kept");
    value = {};
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), "kept");
}

TEST(RuntimeBlockCompaction, ConcurrentCleanupAndInvalidThresholdLeaveCoverageUnchanged)
{
    tree_test::TempDir              directory("runtime_gc_busy_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store                    = store.get();
    config.merge_gc_max_relocation_bytes = 1;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    core->local_backend()->gc_active.store(true);
    MergeGcStats stats;
    EXPECT_EQ(core->compact_sparse_blocks(&stats).code(), Code::kUnavailable);
    EXPECT_EQ(stats.blocks_selected, 0U);
    core->local_backend()->gc_active.store(false);
    EXPECT_EQ(core->local_backend()->checkpoints->durable_coverage(), 0U);
    core.reset();
    config.merge_gc_block_free_threshold = 1.0;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    EXPECT_EQ(core->compact_sparse_blocks(&stats).code(), Code::kInvalidArgument);
}
} // namespace
} // namespace crowdb::tree
