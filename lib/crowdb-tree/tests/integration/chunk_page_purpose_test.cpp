// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <memory>

namespace crowdb::tree::detail
{

TEST(ChunkPagePurpose, MixedWritesSplitPacksAndRecoverFromCatalog)
{
    auto                         catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                         transport = std::make_shared<MemoryChunkTransport>();
    const ChunkPageStore::Config config{.tree_id = 901, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1};
    const std::array<uint8_t, 3> index{1, 2, 3};
    const std::array<uint8_t, 3> page{4, 5, 6};
    {
        ChunkPageStore store(config, catalog, transport);
        ASSERT_TRUE(store.write_at(8192, page.data(), page.size()).ok());
        ASSERT_TRUE(store.write_typed_at(PagePurpose::kPageIndex, 8195, index.data(), index.size()).ok());
        ASSERT_TRUE(store.sync().ok());
        ASSERT_TRUE(store.write_typed_at(PagePurpose::kPageIndex, 0, index.data(), index.size()).ok());

        struct Completion
        {
            std::atomic<bool> done{false};
            bool              passed = false;
        } completion;

        ASSERT_TRUE(store
                        .submit_fsync({.context = &completion,
                                       .complete_fn =
                                           [](void *context, Status status) {
                                               auto &state  = *static_cast<Completion *>(context);
                                               state.passed = status.ok();
                                               state.done.store(true, std::memory_order_release);
                                               state.done.notify_one();
                                           }})
                        .ok());
        completion.done.wait(false, std::memory_order_acquire);
        EXPECT_TRUE(completion.passed);
    }
    auto manifest = catalog->load(config.tree_id);
    ASSERT_NE(manifest, nullptr);
    bool saw_index = false;
    bool saw_page  = false;
    for (const auto &pack : manifest->packs) {
        const auto purpose = chunk_purpose(pack.ref.chunk_id);
        saw_index |= purpose == PagePurpose::kPageIndex;
        saw_page |= purpose == PagePurpose::kBtreePage;
        if (pack.logical_offset <= 8192 && 8192 < pack.logical_offset + pack.ref.length) {
            EXPECT_EQ(purpose, PagePurpose::kBtreePage);
            EXPECT_LE(pack.logical_offset + pack.ref.length, 8195U);
        }
        if (pack.logical_offset <= 8195 && 8195 < pack.logical_offset + pack.ref.length) {
            EXPECT_EQ(purpose, PagePurpose::kPageIndex);
        }
    }
    EXPECT_TRUE(saw_index);
    EXPECT_TRUE(saw_page);
    ChunkPageStore         reopened(config, catalog, transport);
    std::array<uint8_t, 6> bytes{};
    ASSERT_TRUE(reopened.read_at(8192, bytes.data(), bytes.size()).ok());
    EXPECT_EQ(bytes, (std::array<uint8_t, 6>{4, 5, 6, 1, 2, 3}));
}

} // namespace crowdb::tree::detail
