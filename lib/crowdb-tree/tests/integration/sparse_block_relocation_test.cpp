// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_sparse_gc.h"
#include "btree/overflow_backing.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "extent_ledger_test_support.h"
#include "snapshot/operation_wait.h"
#include "snapshot/primary_backing.h"
#include "test_tmp.h"

namespace crowdb::tree
{
namespace
{
TEST(SparseBlockRelocation, SelectionSkipsDenseEmptyAndMissingBlocksAndKeepsItsFence)
{
    tree_test::TempDir              directory("sparse_candidate_");
    std::unique_ptr<BlockPageStore> device;
    constexpr uint64_t              block = 16384;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, block, 4096, &device).ok());
    ASSERT_TRUE(device->ensure_extents(0, 6 * block).ok());
    ASSERT_TRUE(device->delete_block(3).ok());
    test_read::LedgerFixture test({.backend_id = 1, .file_size = 6 * block, .address_limit = 6 * block});
    auto                     dense  = test.recover(block, block);
    auto                     sparse = test.recover(4 * block, 4096);
    ASSERT_TRUE(dense.status.ok() && sparse.status.ok());
    ASSERT_TRUE(test.activate().status.ok());
    auto selected = select_sparse_local_block(*device, *test.ledger, 0, test.credits.credits.reservation());
    ASSERT_TRUE(selected.status.ok()) << selected.status.to_string();
    EXPECT_EQ(selected.exclusion.address, 4 * block);
    EXPECT_EQ(selected.exclusion.length, block);
    EXPECT_EQ(selected.next_block, 5U);
    auto first = test.reserve(3 * block);
    EXPECT_EQ(first.status.code(), Code::kResourceExhausted);
    selected   = {};
    auto retry = test.reserve(3 * block);
    EXPECT_EQ(retry.status.code(), Code::kResourceExhausted);
    sparse         = {};
    auto available = test.reserve(3 * block);
    ASSERT_TRUE(available.status.ok());
    EXPECT_EQ(available.extent.address, 2 * block);
}

TEST(SparseBlockRelocation, CopiesOnlyTheSelectedOverflowFramesAndRetainsOldReadersThroughTwoAnchors)
{
    tree_test::TempDir              directory("sparse_relocation_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &device).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store                           = device.get();
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.compression                          = compress_algo::kNone;
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    auto local = core->local_backend();
    local->gc_active.store(true); // This test drives the two rotations explicitly.
    const std::string payload(100000, 'v');
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = payload}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    auto old  = core->capture();
    auto leaf = old->mapping.get(old->root_page_id);
    ASSERT_TRUE(leaf->primary && leaf->primary->frame.backing->overflow);
    const BackingExclusion interval{.address = 64 * 1024, .length = 64 * 1024};
    size_t                 old_inside  = 0;
    size_t                 old_outside = 0;
    for (const auto &[pid, page] : leaf->primary->frame.backing->overflow->pages) {
        (void)pid;
        interval.overlaps(*page.backing) ? ++old_inside : ++old_outside;
    }
    ASSERT_GT(old_inside, 0U);
    ASSERT_GT(old_outside, 0U);
    auto fence = test_read::await_ledger([&](auto done) {
        return local->ledger->exclude_allocations(interval.address, interval.length, {}, std::move(done));
    });
    ASSERT_TRUE(fence.status.ok()) << fence.status.to_string();
    auto options                   = checkpoint_detail::defaults(*core);
    options.write.frames.exclusion = interval;
    std::shared_ptr<CheckpointOperation> moved;
    ASSERT_TRUE(core->prepare_checkpoint(options, &moved).ok());
    auto status = detail::wait_snapshot_operation(moved);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_GE(moved->result().written_frames, old_inside);
    EXPECT_GT(moved->result().reused_frames, 0U);
    auto current = core->capture()->mapping.get(old->root_page_id);
    ASSERT_TRUE(current->primary && current->primary->frame.backing->overflow);
    EXPECT_FALSE(interval.overlaps(*current->primary->frame.backing));
    for (const auto &[pid, page] : current->primary->frame.backing->overflow->pages) {
        (void)pid;
        EXPECT_FALSE(interval.overlaps(*page.backing));
    }
    moved.reset();
    ASSERT_TRUE(core->prepare_checkpoint(options, &moved).ok());
    ASSERT_TRUE(detail::wait_snapshot_operation(moved).ok());
    EXPECT_EQ(moved->result().written_frames, 0U);
    moved.reset();
    core->cache()->evict();
    core->binding()->cpu()->drain();
    auto claim = [&] {
        return test_read::await_ledger([&](auto done) {
            return local->ledger->claim_free(interval.address, interval.length, {}, std::move(done));
        });
    };
    EXPECT_EQ(claim().status.code(), Code::kNotFound);
    old.reset();
    leaf.reset();
    core->publisher().reclaim();
    core->binding()->account()->drain_retirements();
    auto deletion = claim();
    ASSERT_TRUE(deletion.status.ok()) << deletion.status.to_string();
    ASSERT_TRUE(device->delete_block(1).ok());
    deletion = {};
    fence    = {};
    current.reset();
    GetView actual;
    ASSERT_TRUE(core->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), payload);
    actual = {};
    local.reset();
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), payload);
}
} // namespace
} // namespace crowdb::tree
