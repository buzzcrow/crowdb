// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "async_completion_adapter.h"
#include "crowdb-common/diskio_uring.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

using namespace crowdb::tree;

namespace
{
crowdb::common::DiskIOUring make_uring()
{
    crowdb::common::Topology       topology;
    crowdb::common::PipelineConfig pipeline;
    pipeline.entries = 256;
    pipeline.mode    = crowdb::common::PollingMode::Hybrid;
    topology.pipelines.push_back(pipeline);
    return crowdb::common::DiskIOUring(std::move(topology));
}

bool wait_for(const std::atomic<unsigned> &completed)
{
    for (unsigned i = 0; i < 200; ++i) {
        if (completed.load(std::memory_order_acquire) != 0) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return completed.load(std::memory_order_acquire) != 0;
}
} // namespace

TEST(BlockAsyncPageStore, CrossExtentReadAfterReopenMatchesSync)
{
    struct ReadRange
    {
        uint64_t block_size;
        uint64_t offset;
        size_t   length;
    };

    for (const auto range : {
             ReadRange{.block_size = 64 * 1024 * 1024,  .offset = 133259264, .length = 1212416},
             ReadRange{.block_size = 128 * 1024 * 1024, .offset = 133259264, .length = 1212416},
             ReadRange{.block_size = 8192,              .offset = 4096,      .length = 16384  },
             ReadRange{.block_size = 8192,              .offset = 8192,      .length = 12288  }
    }) {
        SCOPED_TRACE(::testing::Message() << range.block_size << ':' << range.offset << '+' << range.length);
        crowdb::tree_test::TempDir      directory;
        auto                            uring = make_uring();
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, range.block_size, 4096, &store).ok());
        std::vector<uint8_t> expected(range.length);
        for (size_t i = 0; i < expected.size(); ++i) {
            expected[i] = static_cast<uint8_t>(((i / 4096) + i) % 251);
        }
        ASSERT_TRUE(store->write_at(range.offset, expected.data(), expected.size()).ok());
        ASSERT_TRUE(store->sync().ok());
        store.reset();
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, range.block_size, 4096, &store).ok());
        for (int fd : store->all_extent_fds()) {
            uring.register_fd(fd);
        }
        std::unique_ptr<void, decltype(&std::free)> output(std::aligned_alloc(4096, range.length), &std::free);
        ASSERT_NE(output, nullptr);
        ASSERT_TRUE(store->read_at(range.offset, static_cast<uint8_t *>(output.get()), range.length).ok());
        ASSERT_TRUE(std::equal(expected.begin(), expected.end(), static_cast<uint8_t *>(output.get())));
        std::fill_n(static_cast<uint8_t *>(output.get()), range.length, 0);
        BlockAsyncPageStore   asynchronous(store.get(), &uring);
        std::atomic<unsigned> completed{0};
        Status                status;
        asynchronous.submit_read(range.offset, output.get(), range.length,
                                 crowdb::tree::detail::own_async_completion([&](Status result) {
                                     status = std::move(result);
                                     completed.fetch_add(1, std::memory_order_release);
                                 }));
        ASSERT_TRUE(wait_for(completed));
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(completed.load(), 1U);
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), static_cast<uint8_t *>(output.get())));
    }
}

TEST(BlockAsyncPageStore, CrossExtentReadRejectsMissingAndTruncatedTail)
{
    for (bool missing : {false, true}) {
        SCOPED_TRACE(missing);
        crowdb::tree_test::TempDir      directory;
        auto                            uring = make_uring();
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, 8192, 4096, &store).ok());
        std::vector<uint8_t> expected(12288, 0xA5);
        ASSERT_TRUE(store->write_at(4096, expected.data(), expected.size()).ok());
        if (missing) {
            ASSERT_TRUE(store->delete_block(1).ok());
        }
        else {
            uint64_t local = 0;
            ASSERT_EQ(::ftruncate(store->fd_for_offset(8192, &local), 4096), 0);
        }
        for (int fd : store->all_extent_fds()) {
            uring.register_fd(fd);
        }
        std::unique_ptr<void, decltype(&std::free)> output(std::aligned_alloc(4096, expected.size()), &std::free);
        ASSERT_NE(output, nullptr);
        BlockAsyncPageStore   asynchronous(store.get(), &uring);
        std::atomic<unsigned> completed{0};
        Status                status;
        asynchronous.submit_read(4096, output.get(), expected.size(),
                                 crowdb::tree::detail::own_async_completion([&](Status result) {
                                     status = std::move(result);
                                     completed.fetch_add(1, std::memory_order_release);
                                 }));
        ASSERT_TRUE(wait_for(completed));
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find(missing ? "no fd for offset" : "short read (4096 of 8192 bytes)"),
                  std::string::npos);
        EXPECT_EQ(completed.load(), 1U);
    }
}
