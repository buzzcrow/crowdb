// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <thread>

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

struct CompletionState
{
    std::atomic<bool> done{false};
    std::atomic<int>  code{static_cast<int>(Code::kOk)};
};

void record_completion(void *context, Status status)
{
    auto *state = static_cast<CompletionState *>(context);
    state->code.store(static_cast<int>(status.code()), std::memory_order_relaxed);
    state->done.store(true, std::memory_order_release);
    state->done.notify_one();
}

struct BlockingCompletion
{
    std::thread::id   submitter;
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
    std::atomic<bool> ran_off_submitter{false};
};

void block_completion(void *context, Status)
{
    auto *state = static_cast<BlockingCompletion *>(context);
    state->ran_off_submitter.store(std::this_thread::get_id() != state->submitter, std::memory_order_relaxed);
    state->entered.store(true, std::memory_order_release);
    state->entered.notify_one();
    state->release.wait(false, std::memory_order_acquire);
}

struct SelfDestroyCompletion
{
    std::unique_ptr<ChunkPageStore> store;
    std::atomic<bool>               entered{false};
    std::atomic<bool>               release{false};
    std::atomic<bool>               done{false};
};

void destroy_store_from_completion(void *context, Status)
{
    auto *state = static_cast<SelfDestroyCompletion *>(context);
    state->entered.store(true, std::memory_order_release);
    state->entered.notify_one();
    state->release.wait(false, std::memory_order_acquire);
    state->store.reset();
    state->done.store(true, std::memory_order_release);
    state->done.notify_one();
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
    auto                   catalog   = std::make_shared<MemoryRootCatalog>(7);
    auto                   transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore::Config config{.tree_id = 42, .owner_epoch = 7, .pack_bytes = 4096, .iu_size = 1};
    {
        ChunkPageStore store(config, catalog, transport);
        Config         options;
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
            EXPECT_LE(pack.ref.length, 4096U);
            std::vector<uint8_t> first(pack.ref.length);
            std::vector<uint8_t> second(pack.ref.length);
            std::vector<uint8_t> third(pack.ref.length);
            ASSERT_TRUE(transport->read_mirror(pack.ref.chunk_id, 0, pack.ref.offset, first.data(), first.size()).ok());
            ASSERT_TRUE(
                transport->read_mirror(pack.ref.chunk_id, 1, pack.ref.offset, second.data(), second.size()).ok());
            ASSERT_TRUE(transport->read_mirror(pack.ref.chunk_id, 2, pack.ref.offset, third.data(), third.size()).ok());
            EXPECT_EQ(first, second);
            EXPECT_EQ(second, third);
        }
        EXPECT_EQ(manifest->reference_segments[0].ref_count, manifest->packs.size());
        auto segment = catalog->load_reference_segment(42, manifest->reference_segments[0].object_id);
        ASSERT_NE(segment, nullptr);
        EXPECT_EQ(segment->refs.size(), manifest->packs.size());
        EXPECT_EQ(catalog->reference_segment_count(42), manifest->reference_segments.size());
        EXPECT_EQ(store.stats().generations_published, 1U);
        EXPECT_EQ(store.stats().mirror_write_attempts, manifest->packs.size() * 3U);
    }

    ChunkPageStore reopened_store(config, catalog, transport);
    Config         reopened_options;
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
    auto           catalog   = std::make_shared<MemoryRootCatalog>(3);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 9, .owner_epoch = 3, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    Config         options;
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
    EXPECT_GT(catalog->reference_segment_count(9), prior->reference_segments.size());
    EXPECT_GT(store.reclaim_orphans(), 0U);
    EXPECT_EQ(catalog->reference_segment_count(9), prior->reference_segments.size());
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(tree.get(Slice("a"), &slot, &value));
    EXPECT_EQ(value, "new");
}

TEST(ChunkPageStore, CorruptPersistedReferenceSegmentRejectsRead)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 16, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  bytes[] = {9, 8, 7, 6};
    ASSERT_TRUE(store.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());

    catalog->corrupt_active_reference_segment(0, 0);
    ChunkPageStore reopened({.tree_id = 16, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    uint8_t        out[4] = {};
    EXPECT_EQ(reopened.read_at(8192, out, sizeof(out)).code(), Code::kCorruption);
}

TEST(ChunkPageStore, ResolvesOrdinalsAcrossPersistedSegmentBoundary)
{
    auto                 catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                 transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore       store({.tree_id = 17, .owner_epoch = 1, .pack_bytes = 32, .page_alignment = 1, .iu_size = 1},
                               catalog, transport);
    std::vector<uint8_t> bytes(8200);
    for (size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<uint8_t>(index);
    }
    ASSERT_TRUE(store.write_at(8192, bytes.data() + 8192, bytes.size() - 8192).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes.data(), 8192).ok());
    ASSERT_TRUE(store.sync().ok());

    auto manifest = catalog->load(17);
    ASSERT_NE(manifest, nullptr);
    ASSERT_EQ(manifest->packs.size(), 257U);
    ASSERT_EQ(manifest->reference_segments.size(), 2U);
    EXPECT_EQ(manifest->reference_segments[0].ref_count, 256U);
    EXPECT_EQ(manifest->reference_segments[1].first_ordinal, 256U);
    EXPECT_EQ(catalog->reference_segment_count(17), 2U);

    std::array<uint8_t, 16> out{};
    ASSERT_TRUE(store.read_at(8184, out.data(), out.size()).ok());
    EXPECT_TRUE(std::equal(out.begin(), out.end(), bytes.begin() + 8184));
}

TEST(ChunkPageStore, AvailabilityAndCorruptionRemainDistinct)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 5, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  bytes[] = {1, 2, 3, 4};
    ASSERT_TRUE(store.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(store.sync().ok());

    uint8_t out[4] = {};
    store.inject_unavailable(true);
    EXPECT_EQ(store.read_at(8192, out, sizeof(out)).code(), Code::kUnavailable);
    store.inject_unavailable(false);
    transport->inject_unavailable(true);
    EXPECT_EQ(store.read_at(8192, out, sizeof(out)).code(), Code::kUnavailable);
    transport->inject_unavailable(false);
    auto manifest = catalog->load(5);
    ASSERT_NE(manifest, nullptr);
    for (uint32_t mirror = 0; mirror < 3; ++mirror) {
        transport->corrupt_mirror(manifest->packs[2].ref.chunk_id, mirror, manifest->packs[2].ref.offset);
    }
    ChunkPageStore corrupted({.tree_id = 5, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    EXPECT_EQ(corrupted.read_at(8192, out, sizeof(out)).code(), Code::kCorruption);
}

TEST(ChunkPageStore, RotatesWholePacksAndReopenAllocatesFreshChunk)
{
    auto                   catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                   transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore::Config config{
        .tree_id = 18, .owner_epoch = 1, .pack_bytes = 64, .max_chunk_bytes = 128, .page_alignment = 1, .iu_size = 1};
    ChunkPageStore       store(config, catalog, transport);
    std::vector<uint8_t> bytes(8196, 3);
    ASSERT_TRUE(store.write_at(8192, bytes.data() + 8192, 4).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes.data(), 8192).ok());
    ASSERT_TRUE(store.sync().ok());

    auto first = catalog->load(18);
    ASSERT_NE(first, nullptr);
    std::vector<ChunkId> chunk_ids;
    for (const ChunkPagePack &pack : first->packs) {
        EXPECT_LE(pack.ref.length, config.pack_bytes);
        EXPECT_LE(pack.ref.offset + pack.ref.length, config.max_chunk_bytes);
        if (std::find(chunk_ids.begin(), chunk_ids.end(), pack.ref.chunk_id) == chunk_ids.end()) {
            chunk_ids.push_back(pack.ref.chunk_id);
        }
    }
    ASSERT_GT(chunk_ids.size(), 1U);
    const ChunkId abandoned_chunk_id = first->packs.back().ref.chunk_id;
    ChunkLayout   abandoned;
    ASSERT_TRUE(transport->query_chunk(abandoned_chunk_id, &abandoned).ok());
    EXPECT_FALSE(abandoned.sealed);

    ChunkPageStore reopened(config, catalog, transport);
    const uint8_t  changed = 7;
    ASSERT_TRUE(reopened.write_at(8192, &changed, 1).ok());
    ASSERT_TRUE(reopened.sync().ok());
    ASSERT_TRUE(reopened.write_at(0, &changed, 1).ok());
    ASSERT_TRUE(reopened.sync().ok());
    auto second = catalog->load(18);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(second->packs.front().ref.chunk_id, abandoned_chunk_id);

    std::array<uint8_t, 4> old_bytes{};
    ASSERT_TRUE(transport
                    ->read_mirror(abandoned_chunk_id, 0, first->packs.back().ref.offset, old_bytes.data(),
                                  first->packs.back().ref.length)
                    .ok());
}

TEST(ChunkPageStore, PadsPackTailWithoutChangingLogicalChecksum)
{
    auto                 catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                 transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore       store({.tree_id         = 19,
                                .owner_epoch     = 1,
                                .pack_bytes      = 16U * 1024U,
                                .max_chunk_bytes = 64U * 1024U,
                                .page_alignment  = 64U * 1024U,
                                .iu_size         = 64U * 1024U},
                               catalog, transport);
    std::vector<uint8_t> bytes(8196, 4);
    ASSERT_TRUE(store.write_at(8192, bytes.data() + 8192, 4).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes.data(), 8192).ok());
    ASSERT_TRUE(store.sync().ok());

    auto first = catalog->load(19);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->packs.size(), 1U);
    EXPECT_EQ(first->packs[0].ref.offset, 0U);
    EXPECT_EQ(first->packs[0].ref.length, bytes.size());
    ChunkLayout layout;
    ASSERT_TRUE(transport->query_chunk(first->packs[0].ref.chunk_id, &layout).ok());
    EXPECT_EQ(layout.acknowledged_bytes, 64U * 1024U);

    transport->corrupt_mirror(first->packs[0].ref.chunk_id, 0, first->packs[0].ref.length);
    std::array<uint8_t, 4> out{};
    ASSERT_TRUE(store.read_at(8192, out.data(), out.size()).ok());
    EXPECT_TRUE(std::all_of(out.begin(), out.end(), [](uint8_t value) { return value == 4; }));
}

TEST(ChunkPageStore, MirrorRetryRequiresEveryReplicaAndHealthyFallbackReads)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 6, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
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
    auto manifest = catalog->load(6);
    ASSERT_NE(manifest, nullptr);
    transport->corrupt_mirror(manifest->packs[2].ref.chunk_id, 0, manifest->packs[2].ref.offset);
    ChunkPageStore reopened({.tree_id = 6, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    uint8_t        out[4] = {};
    ASSERT_TRUE(reopened.read_at(8192, out, sizeof(out)).ok());
    EXPECT_TRUE(std::equal(std::begin(bytes), std::end(bytes), std::begin(out)));
}

TEST(ChunkPageStore, ManifestPinsDelayReclamationButKeepOnlyFallback)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 12, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
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
    EXPECT_EQ(catalog->reference_segment_count(12), 2U);
    EXPECT_NE(catalog->load_generation(12, 2), nullptr);
    EXPECT_NE(catalog->load_generation(12, 3), nullptr);
}

TEST(ChunkPageStore, LayoutCacheRefreshesAtValidityBoundary)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore writer({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  bytes[] = {1, 3, 5, 7};
    ASSERT_TRUE(writer.write_at(8192, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(writer.sync().ok());
    ASSERT_TRUE(writer.write_at(0, bytes, sizeof(bytes)).ok());
    ASSERT_TRUE(writer.sync().ok());

    ChunkPageStore cached({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    uint8_t        out[4] = {};
    ASSERT_TRUE(cached.read_at(8192, out, sizeof(out)).ok());
    ASSERT_TRUE(cached.read_at(8192, out, sizeof(out)).ok());
    EXPECT_EQ(cached.stats().layout_queries, 1U);
    EXPECT_EQ(cached.stats().cache_hits, 1U);

    ChunkPageStore uncached({.tree_id = 8, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1, .layout_validity_ms = 0},
                            catalog, transport);
    ASSERT_TRUE(uncached.read_at(8192, out, sizeof(out)).ok());
    ASSERT_TRUE(uncached.read_at(8192, out, sizeof(out)).ok());
    EXPECT_EQ(uncached.stats().layout_queries, 2U);
    EXPECT_EQ(uncached.stats().cache_hits, 0U);
}

TEST(ChunkPageStore, AsyncUnavailableRemainsTypedAndDoesNotLatchCorruption)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 15, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    Config         options;
    options.page_store       = &store;
    options.async_page_store = &store;
    options.frame_bytes      = 4096;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, put(1, "key", "value")).ok());
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.snapshot().ok());
    ASSERT_GT(tree.evict_clean_leaves(0), 0U);

    store.inject_unavailable(true);
    Status            unavailable;
    GetView           missing;
    std::atomic<bool> unavailable_done{false};
    tree.get_async(Slice("key"), [&](Status status, GetView result) {
        unavailable = std::move(status);
        missing     = std::move(result);
        unavailable_done.store(true, std::memory_order_release);
        unavailable_done.notify_one();
    });
    unavailable_done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(unavailable.code(), Code::kUnavailable);
    EXPECT_FALSE(missing.found());
    EXPECT_FALSE(tree.io_failed());

    store.inject_unavailable(false);
    Status            recovered;
    GetView           found;
    std::atomic<bool> recovered_done{false};
    tree.get_async(Slice("key"), [&](Status status, GetView result) {
        recovered = std::move(status);
        found     = std::move(result);
        recovered_done.store(true, std::memory_order_release);
        recovered_done.notify_one();
    });
    recovered_done.wait(false, std::memory_order_acquire);
    EXPECT_TRUE(recovered.ok()) << recovered.to_string();
    EXPECT_TRUE(found.found());
}

TEST(ChunkPageStore, AsyncQueueIsBoundedAndRunsOffSubmitter)
{
    auto               catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto               transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore     store({.tree_id = 20, .owner_epoch = 1, .iu_size = 1, .max_pending_ops = 2}, catalog, transport);
    BlockingCompletion blocked{.submitter = std::this_thread::get_id()};
    const uint8_t      first = 1;
    ASSERT_NE(store.submit_write(0, &first, 1, {.context = &blocked, .complete_fn = &block_completion}), 0U);
    blocked.entered.wait(false, std::memory_order_acquire);

    CompletionState queued;
    CompletionState fsync_exhausted;
    CompletionState exhausted;
    const uint8_t   second = 2;
    const uint8_t   third  = 3;
    EXPECT_NE(store.submit_write(1, &second, 1, {.context = &queued, .complete_fn = &record_completion}), 0U);
    EXPECT_TRUE(store.submit_fsync({.context = &fsync_exhausted, .complete_fn = &record_completion}).ok());
    EXPECT_TRUE(fsync_exhausted.done.load(std::memory_order_acquire));
    EXPECT_EQ(fsync_exhausted.code.load(std::memory_order_relaxed), static_cast<int>(Code::kResourceExhausted));
    EXPECT_EQ(store.submit_write(2, &third, 1, {.context = &exhausted, .complete_fn = &record_completion}), 0U);
    EXPECT_TRUE(exhausted.done.load(std::memory_order_acquire));
    EXPECT_EQ(exhausted.code.load(std::memory_order_relaxed), static_cast<int>(Code::kResourceExhausted));
    EXPECT_TRUE(blocked.ran_off_submitter.load(std::memory_order_relaxed));

    blocked.release.store(true, std::memory_order_release);
    blocked.release.notify_one();
    queued.done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(queued.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
}

TEST(ChunkPageStore, CancelledQueuedOperationCompletesAndShutdownDrains)
{
    auto               catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto               transport = std::make_shared<MemoryChunkTransport>();
    BlockingCompletion blocked{.submitter = std::this_thread::get_id()};
    CompletionState    cancelled;
    const uint8_t      first  = 1;
    const uint8_t      second = 2;
    {
        ChunkPageStore store({.tree_id = 21, .owner_epoch = 1, .iu_size = 1, .max_pending_ops = 2}, catalog, transport);
        ASSERT_NE(store.submit_write(0, &first, 1, {.context = &blocked, .complete_fn = &block_completion}), 0U);
        blocked.entered.wait(false, std::memory_order_acquire);
        const uint64_t operation_id =
            store.submit_write(1, &second, 1, {.context = &cancelled, .complete_fn = &record_completion});
        ASSERT_NE(operation_id, 0U);
        store.cancel(operation_id);
        blocked.release.store(true, std::memory_order_release);
        blocked.release.notify_one();
    }
    EXPECT_TRUE(cancelled.done.load(std::memory_order_acquire));
    EXPECT_EQ(cancelled.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
}

TEST(ChunkPageStore, CompletionMayReleaseFinalStoreOwner)
{
    auto                  catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                  transport = std::make_shared<MemoryChunkTransport>();
    SelfDestroyCompletion completion;
    completion.store = std::make_unique<ChunkPageStore>(
        ChunkPageStore::Config{.tree_id = 22, .owner_epoch = 1, .iu_size = 1}, catalog, transport);
    ChunkPageStore *store = completion.store.get();
    const uint8_t   value = 1;
    ASSERT_NE(
        store->submit_write(0, &value, 1, {.context = &completion, .complete_fn = &destroy_store_from_completion}), 0U);
    completion.entered.wait(false, std::memory_order_acquire);
    completion.release.store(true, std::memory_order_release);
    completion.release.notify_one();
    completion.done.wait(false, std::memory_order_acquire);
}

TEST(ChunkPageStore, CApiFactoryInjectsBackendWithoutChangingOpen)
{
    ct_chunk_transport            *invalid_transport = nullptr;
    ct_chunk_rpc_transport_options invalid_options   = {};
    EXPECT_EQ(ct_rpc_chunk_transport_open(&invalid_options, &invalid_transport),
              static_cast<ct_status>(Code::kInvalidArgument));

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
    int32_t    done          = 0;
    int32_t    found         = 0;
    ct_buf     value         = {};
    ct_status  future_status = 0;
    const auto deadline      = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        future_status = ct_future_poll(future, &done, &found, &slot, &value);
        if (done == 0) {
            std::this_thread::yield();
        }
    } while (done == 0 && std::chrono::steady_clock::now() < deadline);
    ASSERT_EQ(future_status, 0);
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
