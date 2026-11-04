// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <memory>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{
class ChunkPageStagingTest : public ::testing::TestWithParam<bool>
{
};

void publish(ChunkPageStore &store, bool asynchronous)
{
    ASSERT_TRUE(store.sync().ok());
    const uint8_t anchor = 7;
    ASSERT_TRUE(store.write_at(0, &anchor, 1).ok());
    if (!asynchronous) {
        ASSERT_TRUE(store.sync().ok());
        return;
    }

    struct Completion
    {
        std::atomic<bool> done{false};
        bool              success = false;
    } completion;

    ASSERT_TRUE(store
                    .submit_fsync({.context = &completion,
                                   .complete_fn =
                                       [](void *context, Status status) {
                                           auto &result   = *static_cast<Completion *>(context);
                                           result.success = status.ok();
                                           result.done.store(true, std::memory_order_release);
                                           result.done.notify_one();
                                       }})
                    .ok());
    completion.done.wait(false, std::memory_order_acquire);
    ASSERT_TRUE(completion.success);
}

TEST_P(ChunkPageStagingTest, SmallOverwriteDoesNotMaterializeThePublishedImage)
{
    auto                         catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                         transport = std::make_shared<MemoryChunkTransport>();
    const ChunkPageStore::Config config{.tree_id = 921, .owner_epoch = 1, .pack_bytes = 65536, .iu_size = 1};
    std::vector<uint8_t>         expected(16 * 1024 * 1024, 0x53);
    {
        ChunkPageStore store(config, catalog, transport);
        ASSERT_TRUE(store.write_at(8192, expected.data(), expected.size()).ok());
        publish(store, GetParam());
    }
    {
        ChunkPageStore               store(config, catalog, transport);
        const std::array<uint8_t, 4> first{1, 2, 3, 4};
        const std::array<uint8_t, 2> last{8, 9};
        ASSERT_TRUE(store.write_at(8192 + 65535, first.data(), first.size()).ok());
        ASSERT_TRUE(store.write_at(8192 + 65536, last.data(), last.size()).ok());
        ASSERT_TRUE(store.write_at(131071, first.data(), first.size()).ok());
        ASSERT_TRUE(store.write_at(131072, last.data(), last.size()).ok());
        EXPECT_LE(store.stats().pack_reads, 2U)
            << "small staging writes must not read and retain the complete published image";
        expected[65535]         = 1;
        expected[65536]         = 8;
        expected[65537]         = 9;
        expected[65538]         = 4;
        expected[131071 - 8192] = 1;
        expected[131072 - 8192] = 8;
        expected[131073 - 8192] = 9;
        expected[131074 - 8192] = 4;
        publish(store, GetParam());
    }
    ChunkPageStore       reopened(config, catalog, transport);
    std::vector<uint8_t> actual(expected.size());
    ASSERT_TRUE(reopened.read_at(8192, actual.data(), actual.size()).ok());
    EXPECT_EQ(actual, expected);
}

INSTANTIATE_TEST_SUITE_P(PublicationPaths, ChunkPageStagingTest, ::testing::Bool());
} // namespace
} // namespace crowdb::tree::detail
