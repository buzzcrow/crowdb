// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "async_completion_adapter.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <future>

namespace crowdb::tree
{
TEST(BlockSyncGeneration, OlderBarrierCannotAcknowledgeLaterWrite)
{
    crowdb::tree_test::TempDir      dir("sync_generation_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 1, &store).ok());
    const std::array<uint8_t, 1> bytes{42};
    ASSERT_TRUE(store->write_at(0, bytes.data(), bytes.size()).ok());
    auto first = store->sync_targets();
    ASSERT_EQ(first.size(), 1U);
    ASSERT_TRUE(store->write_at(1, bytes.data(), bytes.size()).ok());
    auto second = store->sync_targets();
    ASSERT_EQ(second.size(), 1U);
    first[0].medium->acknowledge_sync(first[0].generation);
    EXPECT_EQ(store->dirty_fds().size(), 1U);
    second[0].medium->acknowledge_sync(second[0].generation);
    EXPECT_TRUE(store->dirty_fds().empty());
    // Late completion of the older barrier cannot undo a newer success.
    first[0].medium->acknowledge_sync(first[0].generation);
    EXPECT_TRUE(store->dirty_fds().empty());
}

TEST(BlockSyncGeneration, WriteCompletingAfterBarrierRemainsDirty)
{
    crowdb::tree_test::TempDir      dir("sync_inflight_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 1, &store).ok());
    ASSERT_TRUE(store->ensure_extents(0, 1).ok());
    auto barrier = store->sync_targets();
    ASSERT_EQ(barrier.size(), 1U);
    barrier[0].medium->acknowledge_sync(barrier[0].generation);
    EXPECT_TRUE(store->dirty_fds().empty());
    // A submitted write may still be in flight when that barrier completes.
    barrier[0].medium->mark_dirty();
    EXPECT_EQ(store->dirty_fds().size(), 1U);
    ASSERT_TRUE(store->sync().ok());
    EXPECT_TRUE(store->dirty_fds().empty());
}

TEST(BlockSyncGeneration, CapturedFdSurvivesDeleteAndDoesNotAcknowledgeReplacement)
{
    crowdb::tree_test::TempDir      dir("sync_incarnation_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 1, &store).ok());
    const std::array<uint8_t, 1> bytes{42};
    ASSERT_TRUE(store->write_at(4096, bytes.data(), bytes.size()).ok());
    auto old = store->sync_targets();
    ASSERT_EQ(old.size(), 1U);
    std::weak_ptr<FileMedium> retained = old[0].medium;
    ASSERT_TRUE(store->delete_block(1).ok());
    EXPECT_FALSE(retained.expired());
    ASSERT_TRUE(store->write_at(4096, bytes.data(), bytes.size()).ok());
    auto current = store->sync_targets();
    ASSERT_EQ(current.size(), 1U);
    EXPECT_NE(current[0].medium, old[0].medium);
    ASSERT_TRUE(old[0].medium->fsync().ok());
    old[0].medium->acknowledge_sync(old[0].generation);
    EXPECT_EQ(store->dirty_fds().size(), 1U);
    old.clear();
    EXPECT_TRUE(retained.expired());
    ASSERT_TRUE(store->sync().ok());
    EXPECT_TRUE(store->dirty_fds().empty());
}

TEST(BlockSyncGeneration, SingleFileSyncAcknowledgesOnlySuccessfulWrites)
{
    crowdb::tree_test::TempDir      dir("sync_single_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open(dir.path + "/store", 4096, &store).ok());
    const std::array<uint8_t, 1> bytes{42};
    EXPECT_TRUE(store->dirty_fds().empty());
    ASSERT_TRUE(store->write_at(0, bytes.data(), bytes.size()).ok());
    EXPECT_EQ(store->dirty_fds().size(), 1U);
    ASSERT_TRUE(store->sync().ok());
    EXPECT_TRUE(store->dirty_fds().empty());
}

#ifdef CROWDB_HAVE_LIBURING
TEST(BlockSyncGeneration, AsyncBarrierAcknowledgesSuccessAndRetainsFailure)
{
    crowdb::tree_test::TempDir      dir("sync_async_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    BlockAsyncPageStore       store(file.get(), &uring);
    std::array<uint8_t, 8192> bytes{};
    std::promise<Status>      written;
    auto                      write = written.get_future();
    store.submit_write(0, bytes.data(), bytes.size(), detail::own_async_completion([&written](Status status) {
                           written.set_value(std::move(status));
                       }));
    ASSERT_EQ(write.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(write.get().ok());
    ASSERT_EQ(file->dirty_fds().size(), 2U);
    std::promise<Status> synced;
    auto                 sync = synced.get_future();
    ASSERT_TRUE(store
                    .submit_fsync(
                        detail::own_async_completion([&synced](Status status) { synced.set_value(std::move(status)); }))
                    .ok());
    ASSERT_EQ(sync.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(sync.get().ok());
    EXPECT_TRUE(file->dirty_fds().empty());
    ASSERT_TRUE(file->ensure_extents(4096, 4096).ok());
    crowdb::common::DiskIOUring unavailable({});
    BlockAsyncPageStore         rejected(file.get(), &unavailable);
    size_t                      calls = 0;
    ASSERT_TRUE(rejected
                    .submit_fsync(detail::own_async_completion([&calls](Status status) {
                        EXPECT_EQ(status.code(), Code::kIoError);
                        ++calls;
                    }))
                    .ok());
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(file->dirty_fds().size(), 1U);
}
#endif
} // namespace crowdb::tree
