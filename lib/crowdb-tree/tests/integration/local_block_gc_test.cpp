// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_block_gc.h"
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"
#include "extent_ledger_test_support.h"
#include "local_checkpoint_test_support.h"
#include "test_tmp.h"

#include <future>

namespace crowdb::tree
{
using namespace test_read;

namespace
{
struct TestBlockGc
{
    tree_test::TempDir              directory{"block_gc_"};
    std::unique_ptr<BlockPageStore> store;
    LedgerFixture                   ledger;

    explicit TestBlockGc(uint64_t size = 6 * 4096, uint64_t block_size = 4096)
        : ledger({.backend_id = 1, .file_size = size, .address_limit = size})
    {
        EXPECT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, block_size, 1, &store).ok());
        const std::vector<uint8_t> bytes(size, 42);
        EXPECT_TRUE(store->write_at(0, bytes.data(), bytes.size()).ok());
    }
};

TEST(LocalBlockGc, BoundedPassRetainsAnchorsAndReadersThenReusesDeletedHoles)
{
    TestBlockGc test;
    auto        anchor = test.ledger.recover(12288, 4096);
    ASSERT_TRUE(anchor.status.ok());
    auto reader = anchor.extent.owner;
    ASSERT_TRUE(test.ledger.activate().status.ok());
    const LocalBlockGcLimits limits{.max_blocks = 2, .max_deleted_bytes = 4096};
    auto                     first = collect_free_local_blocks(*test.store, *test.ledger.ledger, 0, limits,
                                                               test.ledger.credits.credits.reservation());
    ASSERT_TRUE(first.status.ok());
    EXPECT_EQ(first.deleted_blocks, 1U);
    EXPECT_EQ(first.next_block, 3U);
    EXPECT_FALSE(first.complete);
    uint64_t offset = 0;
    EXPECT_TRUE(test.store->file_for_offset(0, &offset));
    EXPECT_TRUE(test.store->file_for_offset(4096, &offset));
    EXPECT_FALSE(test.store->file_for_offset(8192, &offset));
    anchor      = {};
    auto second = collect_free_local_blocks(*test.store, *test.ledger.ledger, first.next_block, limits,
                                            test.ledger.credits.credits.reservation());
    ASSERT_TRUE(second.status.ok());
    EXPECT_EQ(second.protected_blocks, 1U);
    EXPECT_EQ(second.deleted_blocks, 1U);
    EXPECT_EQ(second.next_block, 5U);
    EXPECT_TRUE(test.store->file_for_offset(12288, &offset));
    reader.reset();
    auto third = collect_free_local_blocks(*test.store, *test.ledger.ledger, 3, limits,
                                           test.ledger.credits.credits.reservation());
    ASSERT_TRUE(third.status.ok());
    EXPECT_EQ(third.deleted_blocks, 1U);
    EXPECT_FALSE(test.store->file_for_offset(12288, &offset));
    auto reused = test.ledger.reserve(8192);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
    const uint8_t value = 9;
    ASSERT_TRUE(test.store->write_at(reused.extent.address, &value, 1).ok());
    uint8_t actual = 0;
    ASSERT_TRUE(test.store->read_at(reused.extent.address, &actual, 1).ok());
    EXPECT_EQ(actual, value);
}

TEST(LocalBlockGc, MetadataFailureAndPartialTailCannotDeleteUnleasedStorage)
{
    TestBlockGc test(5 * 4096, 8192);
    ASSERT_TRUE(test.ledger.activate().status.ok());
    test.ledger.credits.seal();
    auto rejected =
        collect_free_local_blocks(*test.store, *test.ledger.ledger, 0, {}, test.ledger.credits.credits.reservation());
    EXPECT_EQ(rejected.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(rejected.next_block, 1U);
    EXPECT_EQ(test.store->num_extents(), 3U);
    const LocalBlockGcLimits too_small{.max_blocks = 8, .max_deleted_bytes = 4096};
    auto                     invalid = collect_free_local_blocks(*test.store, *test.ledger.ledger, 0, too_small, {});
    EXPECT_EQ(invalid.status.code(), Code::kResourceExhausted);
    auto collected = collect_free_local_blocks(*test.store, *test.ledger.ledger, 0, {}, {});
    ASSERT_TRUE(collected.status.ok());
    EXPECT_TRUE(collected.complete);
    EXPECT_EQ(collected.deleted_blocks, 1U);
    uint64_t offset = 0;
    EXPECT_TRUE(test.store->file_for_offset(16384, &offset));
}

TEST(LocalBlockGc, PublicCheckpointReclaimsCrashTailWithoutLosingRecovery)
{
    tree_test::TempDir              dir("checkpoint_gc_");
    std::unique_ptr<BlockPageStore> opened;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 2, 3, 8192, 4096, &opened).ok());
    std::shared_ptr<BlockPageStore> device = std::move(opened);
    const uint8_t                   orphan = 37;
    ASSERT_TRUE(device->write_at((31 * 8192) - 1, &orphan, 1).ok());
    test_write::TestCheckpointStore store;
    store.device = device;
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store       = device.get();
    config.async_page_store = &store;
    config.frame_bytes      = 4096;
    std::unique_ptr<Crowdbtree> tree;
    const auto                  opened_status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(opened_status.ok()) << opened_status.to_string();
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    uint64_t coverage = 0;
    auto     status   = tree->snapshot(&coverage);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 1U);
    TreeCoreAccess::get(*tree)->operation_lifetime().drain_accepted();
    TreeCoreAccess::get(*tree)->binding()->drain();
    uint64_t offset = 0;
    EXPECT_FALSE(device->file_for_offset(30 * 8192, &offset));
    tree.reset();
    status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView value;
    ASSERT_TRUE(TreeCoreAccess::get(*tree)->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), "value");
}

TEST(LocalBlockGc, PublicAsyncCheckpointCleanupDrainsBeforeRawBackendClose)
{
    tree_test::TempDir              dir("async_checkpoint_gc_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 4, 5, 8192, 4096, &device).ok());
    const uint8_t orphan = 23;
    ASSERT_TRUE(device->write_at((31 * 8192) - 1, &orphan, 1).ok());
    Config             config;
    TreeRuntimeOptions runtime;
    runtime.workers         = 1;
    runtime.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime, &config.runtime).ok());
    config.page_store  = device.get();
    config.frame_bytes = 4096;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    std::promise<Status> completion;
    auto                 done = completion.get_future();
    core->checkpoint_async([&](Status status, uint64_t coverage) {
        EXPECT_EQ(coverage, 1U);
        completion.set_value(std::move(status));
    });
    ASSERT_EQ(done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    const auto status = done.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    core.reset();
    uint64_t offset = 0;
    EXPECT_FALSE(device->file_for_offset(30 * 8192, &offset));
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    GetView value;
    ASSERT_TRUE(core->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), "value");
}
} // namespace
} // namespace crowdb::tree
