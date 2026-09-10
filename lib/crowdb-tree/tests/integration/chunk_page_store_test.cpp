// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
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

void publish_raw_generation(ChunkPageStore *store, uint8_t value)
{
    ASSERT_TRUE(store->write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store->sync().ok());
    ASSERT_TRUE(store->write_at(0, &value, 1).ok());
    ASSERT_TRUE(store->sync().ok());
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
        ASSERT_FALSE(manifest->reference_segments.empty());
        for (const auto &pack : manifest->packs) {
            EXPECT_LE(pack.mirrors[0].size(), 4096U);
            EXPECT_EQ(pack.mirrors[0], pack.mirrors[1]);
            EXPECT_EQ(pack.mirrors[1], pack.mirrors[2]);
        }
        EXPECT_EQ(manifest->reference_segments[0].refs.size(), manifest->packs.size());
        EXPECT_EQ(store.stats().generations_published, 1U);
        EXPECT_EQ(store.stats().mirror_write_attempts, manifest->packs.size() * 3U);
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
    ChunkPageStore corrupted({.tree_id = 5, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    EXPECT_EQ(corrupted.read_at(8192, out, sizeof(out)).code(), Code::kCorruption);
}

TEST(ChunkPageStore, MirrorRetryRequiresEveryReplicaAndHealthyFallbackReads)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(1);
    ChunkPageStore store({.tree_id = 6, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    const uint8_t  bytes[] = {5, 6, 7, 8};
    ASSERT_TRUE(store.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes, sizeof(bytes)).ok());
    store.inject_mirror_write_failures(1U << 1U);
    EXPECT_EQ(store.sync().code(), Code::kUnavailable);
    EXPECT_EQ(catalog->load(6), nullptr);
    EXPECT_EQ(store.stats().mirror_write_failures, 3U);
    EXPECT_GT(store.stats().orphan_bytes, 0U);
    EXPECT_GT(store.reclaim_orphans(), 0U);
    EXPECT_EQ(store.stats().orphan_bytes, 0U);

    store.inject_mirror_write_failures(0);
    ASSERT_TRUE(store.sync().ok());
    catalog->corrupt_active_mirror(2, 0, 0);
    ChunkPageStore reopened({.tree_id = 6, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    uint8_t        out[4] = {};
    ASSERT_TRUE(reopened.read_at(8192, out, sizeof(out)).ok());
    EXPECT_TRUE(std::equal(std::begin(bytes), std::end(bytes), std::begin(out)));
}

TEST(ChunkPageStore, ManifestPinsDelayReclamationButKeepOnlyFallback)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(1);
    ChunkPageStore store({.tree_id = 12, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    publish_raw_generation(&store, 1);
    auto pinned = catalog->load_generation(12, 1);
    ASSERT_NE(pinned, nullptr);
    publish_raw_generation(&store, 2);
    publish_raw_generation(&store, 3);
    EXPECT_EQ(catalog->retained_manifest_count(12), 3U);
    EXPECT_EQ(catalog->pinned_bytes(12), pinned->logical_size);
    EXPECT_EQ(catalog->reclaim_before(12, 4), 0U);
    EXPECT_EQ(catalog->retained_manifest_count(12), 3U);

    pinned.reset();
    EXPECT_GT(catalog->reclaim_before(12, 4), 0U);
    EXPECT_EQ(catalog->retained_manifest_count(12), 2U);
    EXPECT_NE(catalog->load_generation(12, 2), nullptr);
    EXPECT_NE(catalog->load_generation(12, 3), nullptr);
}

TEST(ChunkPageStore, LayoutCacheRefreshesAtValidityBoundary)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(1);
    ChunkPageStore writer({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    const uint8_t  bytes[] = {1, 3, 5, 7};
    ASSERT_TRUE(writer.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(writer.sync().ok());
    ASSERT_TRUE(writer.write_at(0, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(writer.sync().ok());

    ChunkPageStore cached({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    uint8_t        out[4] = {};
    ASSERT_TRUE(cached.read_at(8192, out, sizeof(out)).ok());
    ASSERT_TRUE(cached.read_at(8192, out, sizeof(out)).ok());
    EXPECT_EQ(cached.stats().layout_queries, 1U);
    EXPECT_EQ(cached.stats().cache_hits, 1U);

    ChunkPageStore uncached({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1, .layout_validity_ms = 0},
                            catalog);
    ASSERT_TRUE(uncached.read_at(8192, out, sizeof(out)).ok());
    ASSERT_TRUE(uncached.read_at(8192, out, sizeof(out)).ok());
    EXPECT_EQ(uncached.stats().layout_queries, 2U);
    EXPECT_EQ(uncached.stats().cache_hits, 0U);
}

TEST(ChunkPageStore, AsyncUnavailableRemainsTypedAndDoesNotLatchCorruption)
{
    auto           catalog = std::make_shared<MemoryRootCatalog>(1);
    ChunkPageStore store({.tree_id = 15, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog);
    Options        options;
    options.page_store       = &store;
    options.async_page_store = &store;
    options.frame_bytes      = 4096;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, put(1, "key", "value")).ok());
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.snapshot().ok());
    ASSERT_GT(tree.evict_clean_leaves(0), 0U);

    store.inject_unavailable(true);
    Status  unavailable;
    GetView missing;
    tree.get_async(Slice("key"), [&](Status status, GetView result) {
        unavailable = std::move(status);
        missing     = std::move(result);
    });
    EXPECT_EQ(unavailable.code(), Code::kUnavailable);
    EXPECT_FALSE(missing.found());
    EXPECT_FALSE(tree.io_failed());

    store.inject_unavailable(false);
    Status  recovered;
    GetView found;
    tree.get_async(Slice("key"), [&](Status status, GetView result) {
        recovered = std::move(status);
        found     = std::move(result);
    });
    EXPECT_TRUE(recovered.ok()) << recovered.to_string();
    EXPECT_TRUE(found.found());
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
    ASSERT_EQ(ct_apply_put(tree, 1, reinterpret_cast<const uint8_t *>("key"), 3,
                           reinterpret_cast<const uint8_t *>("value"), 5),
              0);
    ASSERT_EQ(ct_flush(tree), 0);
    uint64_t slot = 0;
    ASSERT_EQ(ct_snapshot(tree, &slot), 0);
    EXPECT_EQ(slot, 1U);
    ASSERT_GT(ct_evict_clean_leaves(tree, 0), 0U);
    ct_future *future = ct_get_async(tree, reinterpret_cast<const uint8_t *>("key"), 3);
    ASSERT_NE(future, nullptr);
    int32_t done  = 0;
    int32_t found = 0;
    ct_buf  value = {};
    ASSERT_EQ(ct_future_poll(future, &done, &found, &slot, &value), 0);
    EXPECT_EQ(done, 1);
    EXPECT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(value.data), value.len), "value");
    ct_future_free(future);
    ct_chunk_page_store_stats stats = {};
    ASSERT_EQ(ct_chunk_page_store_get_stats(store, &stats), 0);
    EXPECT_EQ(stats.generations_published, 1U);
    EXPECT_GT(stats.packs_written, 0U);
    EXPECT_EQ(stats.mirror_write_attempts, stats.packs_written * 3U);
    ct_page_store_free(store);
    ct_close(tree);
    ct_root_catalog_free(catalog);
}

} // namespace
} // namespace crowdb::tree::detail
