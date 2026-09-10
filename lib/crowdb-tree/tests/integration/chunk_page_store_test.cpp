// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace crowdb::tree::detail
{
namespace
{

Batch put(uint64_t, std::string key, std::string value)
{
    Batch batch;
    batch.ops.push_back({.key = std::move(key), .kind = OpKind::kPut, .value = std::move(value)});
    return batch;
}

TEST(ChunkPageStore, SnapshotPublishesBoundedChecksummedPacksAndReopens)
{
    auto                   catalog = std::make_shared<MemoryRootCatalog>(7);
    ChunkPageStore::Config config{.tree_id = 42, .owner_epoch = 7, .pack_bytes = 4096, .iu_size = 1};
    {
        ChunkPageStore store(config, catalog);
        Options        options;
        options.page_store       = &store;
        options.frame_bytes      = 4096;
        options.leaf_split_bytes = 512;
        Crowdbtree tree(options);
        for (uint64_t i = 1; i <= 80; ++i) {
            ASSERT_TRUE(tree.apply(i, put(i, "key" + std::to_string(i), std::string(256, 'x'))).ok());
        }
        ASSERT_TRUE(tree.flush().ok());
        ASSERT_TRUE(tree.snapshot().ok());
        auto manifest = catalog->load(42);
        ASSERT_NE(manifest, nullptr);
        ASSERT_GT(manifest->packs.size(), 1U);
        for (const auto &pack : manifest->packs) {
            EXPECT_LE(pack.bytes.size(), 4096U);
        }
        EXPECT_EQ(store.stats().generations_published, 1U);
    }

    ChunkPageStore reopened_store(config, catalog);
    Options        reopened_options;
    reopened_options.page_store       = &reopened_store;
    reopened_options.frame_bytes      = 4096;
    reopened_options.leaf_split_bytes = 512;
    std::unique_ptr<Crowdbtree> reopened;
    ASSERT_TRUE(Crowdbtree::open(reopened_options, &reopened).ok());
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(reopened->get(Slice("key40"), &slot, &value));
    EXPECT_EQ(slot, 40U);
    EXPECT_EQ(value, std::string(256, 'x'));
}

TEST(ChunkPageStore, EpochFailureLeavesPriorRootAndAccountsOrphans)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(3);
    ChunkPageStore store({.tree_id = 9, .owner_epoch = 3, .pack_bytes = 4096, .iu_size = 1}, catalog);
    Options        options;
    options.page_store = &store;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, put(1, "a", "old")).ok());
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.snapshot().ok());
    auto prior = catalog->load(9);
    ASSERT_NE(prior, nullptr);

    ASSERT_TRUE(tree.apply(2, put(2, "a", "new")).ok());
    ASSERT_TRUE(tree.flush().ok());
    catalog->set_owner_epoch(4);
    Status failed = tree.snapshot();
    EXPECT_EQ(failed.code(), Code::kUnavailable);
    EXPECT_EQ(catalog->load(9)->generation, prior->generation);
    EXPECT_GT(store.stats().orphan_bytes, 0U);
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(tree.get(Slice("a"), &slot, &value));
    EXPECT_EQ(value, "new");
}

TEST(ChunkPageStore, AvailabilityAndCorruptionRemainDistinct)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(1);
    ChunkPageStore store({.tree_id = 5, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    const uint8_t  bytes[] = {1, 2, 3, 4};
    ASSERT_TRUE(store.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());

    uint8_t out[4] = {};
    store.inject_unavailable(true);
    EXPECT_EQ(store.read_at(8192, out, sizeof(out)).code(), Code::kUnavailable);
    store.inject_unavailable(false);
    catalog->corrupt_active_pack(2, 0);
    EXPECT_EQ(store.read_at(8192, out, sizeof(out)).code(), Code::kCorruption);
}

TEST(ChunkPageStore, CApiFactoryInjectsBackendWithoutChangingOpen)
{
    ct_root_catalog *catalog = nullptr;
    ASSERT_EQ(ct_memory_root_catalog_open(11, &catalog), 0);
    ct_chunk_page_store_options store_options = {.tree_id = 77, .owner_epoch = 11, .pack_bytes = 4096, .iu_size = 1};
    ct_page_store              *store         = nullptr;
    ASSERT_EQ(ct_chunk_page_store_open(&store_options, catalog, &store), 0);
    ct_options options  = {};
    options.page_store  = store;
    options.frame_bytes = 4096;
    ct_tree *tree       = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    ct_page_store_free(store);
    ASSERT_EQ(ct_apply_put(tree, 1, reinterpret_cast<const uint8_t *>("key"), 3,
                           reinterpret_cast<const uint8_t *>("value"), 5),
              0);
    ASSERT_EQ(ct_flush(tree), 0);
    uint64_t slot = 0;
    ASSERT_EQ(ct_snapshot(tree, &slot), 0);
    EXPECT_EQ(slot, 1U);
    ct_close(tree);
    ct_root_catalog_free(catalog);
}

} // namespace
} // namespace crowdb::tree::detail
