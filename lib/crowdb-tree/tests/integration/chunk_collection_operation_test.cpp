// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_collection_operation.h"
#include "backend/chunk/chunk_runtime_page_store.h"
#include "btree/tree_core_access.h"
#include "common/test_chunk_collector.h"
#include "load_budget_test_support.h"
#include "snapshot/operation_wait.h"

#include <future>
#include <latch>

namespace crowdb::tree
{
namespace
{
struct TestCollectionCore
{
    test_read::LoadCredits                         credits;
    std::shared_ptr<detail::TestCollectorCatalog>  catalog   = std::make_shared<detail::TestCollectorCatalog>();
    std::shared_ptr<detail::TestFixedTransport>    transport = std::make_shared<detail::TestFixedTransport>(catalog);
    Config                                         config;
    std::shared_ptr<detail::ChunkRuntimePageStore> store;
    std::unique_ptr<Crowdbtree>                    tree;

    TestCollectionCore()
    {
        EXPECT_TRUE(detail::ChunkPinReleaseQueue::create(
                        credits.credits.reservation(), 32, std::make_shared<int>(1), [](void *) noexcept {}, nullptr,
                        &catalog->queue)
                        .ok());
        TreeRuntimeOptions options;
        options.workers         = 1;
        options.backend_workers = 1;
        EXPECT_TRUE(TreeRuntime::create(options, &config.runtime).ok());
        store = std::make_shared<detail::ChunkRuntimePageStore>(
            detail::ChunkRuntimeOptions{.tree            = 17,
                                        .owner_epoch     = 1,
                                        .open_generation = 0,
                                        .iu_size         = 4096,
                                        .mirrors         = 2,
                                        .max_chunk_bytes = 256 * 1024 * 1024},
            catalog, transport, credits.credits.reservation());
        config.page_store                           = store.get();
        config.backend_owner                        = store;
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 3000;
        config.runtime_binding.limits.io_operations = 1;
    }

    ~TestCollectionCore()
    {
        tree.reset();
        config.backend_owner.reset();
        store.reset();
        config.runtime.reset();
        catalog->queue->stop_admission();
        (void)catalog->queue->pump(32);
        EXPECT_TRUE(catalog->queue->drained());
    }

    Status open()
    {
        return Crowdbtree::open(config, &tree);
    }

    Status prepare(std::shared_ptr<ChunkCollectionOperation> *out) const
    {
        return TreeCoreAccess::get(*tree)->prepare_chunk_collection(out);
    }
};
} // namespace

TEST(ChunkCollectionOperation, SingleIoCoreCollectionDeletesOnlyAfterPinsLeave)
{
    TestCollectionCore test;
    auto               status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.catalog->record = detail::test_collector_stage();
    test.catalog->pins   = 65;
    std::shared_ptr<ChunkCollectionOperation> operation;
    ASSERT_TRUE(test.prepare(&operation).ok());
    status = detail::wait_snapshot_operation(operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.transport->deletions, 0U);
    EXPECT_EQ(operation->result().pins_examined, 65U);
    EXPECT_LE(test.catalog->largest_pin_window, 64U);
    operation.reset();
    test.catalog->pins = 0;
    ASSERT_TRUE(test.prepare(&operation).ok());
    status = detail::wait_snapshot_operation(operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(operation->result().stages_removed, 1U);
    EXPECT_EQ(operation->result().logical_capacity_bytes, 4096U);
    EXPECT_EQ(test.transport->drains, 1U);
    EXPECT_EQ(test.transport->deletions, 1U);
    EXPECT_FALSE(test.catalog->record);
}

TEST(ChunkCollectionOperation, CancelBeforeStartKeepsStageAndReturnsItsAdmission)
{
    TestCollectionCore test;
    ASSERT_TRUE(test.open().ok());
    test.catalog->record = detail::test_collector_stage();
    std::shared_ptr<ChunkCollectionOperation> operation;
    ASSERT_TRUE(test.prepare(&operation).ok());
    operation->request_stop();
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kUnavailable);
    EXPECT_EQ(test.transport->deletions, 0U);
    EXPECT_TRUE(test.catalog->record);
    operation.reset();
    ASSERT_TRUE(test.prepare(&operation).ok());
    EXPECT_TRUE(detail::wait_snapshot_operation(operation).ok());
    EXPECT_EQ(test.transport->deletions, 1U);
}

TEST(ChunkCollectionOperation, CloseDrainsAcceptedDeleteWithoutWaitingForCallerHandle)
{
    TestCollectionCore test;
    ASSERT_TRUE(test.open().ok());
    test.catalog->record = detail::test_collector_stage();
    std::latch entered(1);
    std::latch release(1);
    std::latch close_started(1);
    test.transport->before_drain = [&] {
        entered.count_down();
        release.wait();
    };
    std::shared_ptr<ChunkCollectionOperation> operation;
    ASSERT_TRUE(test.prepare(&operation).ok());
    auto run = std::async(std::launch::async, [&] { return detail::wait_snapshot_operation(operation); });
    entered.wait();
    auto close = std::async(std::launch::async, [&] {
        close_started.count_down();
        TreeCoreAccess::get(*test.tree)->close_and_drain();
    });
    close_started.wait();
    EXPECT_EQ(close.wait_for(std::chrono::milliseconds(1)), std::future_status::timeout);
    release.count_down();
    EXPECT_TRUE(run.get().ok());
    close.get();
    EXPECT_EQ(operation->result().stages_removed, 1U);
    EXPECT_EQ(test.transport->deletions, 1U);
    EXPECT_TRUE(operation);
}

TEST(ChunkCollectionOperation, LostDeleteAckKeepsDeletingAndAbsentRetryConverges)
{
    TestCollectionCore test;
    ASSERT_TRUE(test.open().ok());
    test.catalog->record                = detail::test_collector_stage();
    test.transport->unknown_delete      = true;
    test.transport->absent_after_delete = true;
    std::shared_ptr<ChunkCollectionOperation> operation;
    ASSERT_TRUE(test.prepare(&operation).ok());
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kUnavailable);
    ASSERT_TRUE(test.catalog->record);
    EXPECT_EQ(test.catalog->record->state, detail::ChunkStageState::Deleting);
    EXPECT_EQ(operation->result().stages_removed, 0U);
    operation.reset();
    test.transport->unknown_delete = false;
    ASSERT_TRUE(test.prepare(&operation).ok());
    EXPECT_TRUE(detail::wait_snapshot_operation(operation).ok());
    EXPECT_EQ(operation->result().stages_removed, 1U);
    EXPECT_EQ(test.transport->deletions, 2U);
    EXPECT_FALSE(test.catalog->record);
}
} // namespace crowdb::tree
