// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <thread>

namespace crowdb::tree
{
TEST(BlockDirectory, SparseMaximumIndexDoesNotCreateDensePlaceholders)
{
    tree_test::TempDir              dir("block_sparse_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t  value  = 7;
    const uint64_t offset = uint64_t{UINT32_MAX} * 4096;
    ASSERT_TRUE(store->write_at(offset, &value, 1).ok());
    EXPECT_EQ(store->num_extents(), 2U);
    uint8_t actual = 0;
    ASSERT_TRUE(store->read_at(offset, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    EXPECT_EQ(store->size(), offset + 1);
    ASSERT_TRUE(store->sync().ok());
    store.reset();
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 3, 4, 4096, 1, &store).ok());
    EXPECT_EQ(store->num_extents(), 2U);
    ASSERT_TRUE(store->read_at(offset, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    EXPECT_EQ(store->write_at(offset + 4096, &value, 1).code(), Code::kInvalidArgument);
    EXPECT_EQ(store->ensure_extents(UINT64_MAX, 2).code(), Code::kInvalidArgument);
}

TEST(BlockDirectory, DeletedInteriorAndLastBlockCanBeRecreatedWithoutReusingOldFd)
{
    tree_test::TempDir              dir("block_recreate_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 3, 4, 4096, 1, &store).ok());
    uint8_t value = 7;
    ASSERT_TRUE(store->write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store->write_at(4096, &value, 1).ok());
    uint64_t local = 0;
    auto     held  = store->file_for_offset(4096, &local);
    ASSERT_TRUE(held);
    ASSERT_TRUE(store->delete_block(1).ok());
    value = 9;
    ASSERT_TRUE(store->write_at(4096, &value, 1).ok());
    auto replacement = store->file_for_offset(4096, &local);
    EXPECT_NE(held, replacement);
    uint8_t old = 0;
    ASSERT_TRUE(held->pread_at(0, &old, 1).ok());
    EXPECT_EQ(old, 7);
    uint8_t current = 0;
    ASSERT_TRUE(store->read_at(4096, &current, 1).ok());
    EXPECT_EQ(current, 9);
    ASSERT_TRUE(store->delete_block(2).ok());
    ASSERT_TRUE(store->delete_block(1).ok());
    ASSERT_TRUE(store->delete_block(0).ok());
    EXPECT_EQ(store->num_extents(), 0U);
    EXPECT_EQ(store->size(), 0U);
    EXPECT_EQ(store->read_at(0, &current, 1).code(), Code::kNotFound);
    ASSERT_TRUE(store->write_at(0, &value, 1).ok());
    EXPECT_EQ(store->num_extents(), 1U);
}

TEST(BlockDirectory, ConcurrentGrowthDeletionAndEnumerationKeepReadersStable)
{
    tree_test::TempDir              dir("block_concurrent_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 37;
    ASSERT_TRUE(store->write_at(0, &value, 1).ok());
    std::barrier               start(4);
    std::array<std::thread, 3> workers;
    workers[0] = std::thread([&] {
        start.arrive_and_wait();
        for (uint64_t i = 1; i <= 64; ++i) {
            EXPECT_TRUE(store->write_at(i * 4096, &value, 1).ok());
            EXPECT_TRUE(store->delete_block(static_cast<uint32_t>(i)).ok());
        }
    });
    workers[1] = std::thread([&] {
        start.arrive_and_wait();
        for (uint64_t i = 65; i <= 128; ++i) {
            EXPECT_TRUE(store->write_at(i * 4096, &value, 1).ok());
        }
    });
    workers[2] = std::thread([&] {
        start.arrive_and_wait();
        for (size_t i = 0; i < 128; ++i) {
            const auto targets = store->sync_targets();
            for (const auto &target : targets) {
                EXPECT_GE(target.medium->fd(), 0);
            }
            (void)store->size();
            (void)store->all_extent_fds();
        }
    });
    start.arrive_and_wait();
    for (size_t i = 0; i < 256; ++i) {
        uint8_t actual = 0;
        EXPECT_TRUE(store->read_at(0, &actual, 1).ok());
        EXPECT_EQ(actual, value);
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_EQ(store->num_extents(), 65U);
    EXPECT_TRUE(store->sync().ok());
}
} // namespace crowdb::tree
