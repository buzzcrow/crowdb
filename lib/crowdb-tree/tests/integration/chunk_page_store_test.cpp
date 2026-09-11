// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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

class BlockingReadTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t capacity, uint64_t owner_epoch, ChunkId *chunk_id) override
    {
        return inner_.allocate_mirror_chunk(capacity, owner_epoch, chunk_id);
    }

    Status write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        return inner_.write_mirror(chunk_id, mirror, offset, data, length);
    }

    Status advance_write(ChunkId chunk_id, uint64_t expected, uint64_t acknowledged) override
    {
        return inner_.advance_write(chunk_id, expected, acknowledged);
    }

    Status read_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        read_entered_.store(true, std::memory_order_release);
        read_entered_.notify_all();
        if (hold_reads_.load(std::memory_order_acquire)) {
            release_reads_.wait(false, std::memory_order_acquire);
        }
        return inner_.read_mirror(chunk_id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override
    {
        return inner_.query_chunk(chunk_id, layout);
    }

    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged) override
    {
        return inner_.seal_chunk(chunk_id, owner_epoch, acknowledged);
    }

    void hold_reads()
    {
        read_entered_.store(false, std::memory_order_release);
        release_reads_.store(false, std::memory_order_release);
        hold_reads_.store(true, std::memory_order_release);
    }

    void release_reads()
    {
        hold_reads_.store(false, std::memory_order_release);
        release_reads_.store(true, std::memory_order_release);
        release_reads_.notify_all();
    }

    void wait_for_read() const
    {
        read_entered_.wait(false, std::memory_order_acquire);
    }

  private:
    MemoryChunkTransport      inner_;
    mutable std::atomic<bool> hold_reads_{false};
    mutable std::atomic<bool> release_reads_{false};
    mutable std::atomic<bool> read_entered_{false};
};

class BlockingWriteTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t capacity, uint64_t owner_epoch, ChunkId *chunk_id) override
    {
        return inner_.allocate_mirror_chunk(capacity, owner_epoch, chunk_id);
    }

    Status write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        const uint32_t active   = active_writes_.fetch_add(1, std::memory_order_acq_rel) + 1;
        uint32_t       observed = max_active_writes_.load(std::memory_order_relaxed);
        while (observed < active && !max_active_writes_.compare_exchange_weak(observed, active)) {
        }
        active_writes_.notify_all();
        if (hold_writes_.load(std::memory_order_acquire)) {
            release_writes_.wait(false, std::memory_order_acquire);
        }
        Status status = inner_.write_mirror(chunk_id, mirror, offset, data, length);
        active_writes_.fetch_sub(1, std::memory_order_acq_rel);
        return status;
    }

    Status advance_write(ChunkId chunk_id, uint64_t expected, uint64_t acknowledged) override
    {
        return inner_.advance_write(chunk_id, expected, acknowledged);
    }

    Status read_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        return inner_.read_mirror(chunk_id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override
    {
        return inner_.query_chunk(chunk_id, layout);
    }

    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged) override
    {
        return inner_.seal_chunk(chunk_id, owner_epoch, acknowledged);
    }

    void hold_writes()
    {
        release_writes_.store(false, std::memory_order_release);
        hold_writes_.store(true, std::memory_order_release);
    }

    void wait_for_concurrent_writes(uint32_t count)
    {
        uint32_t active = active_writes_.load(std::memory_order_acquire);
        while (active < count) {
            active_writes_.wait(active, std::memory_order_acquire);
            active = active_writes_.load(std::memory_order_acquire);
        }
    }

    void release_writes()
    {
        hold_writes_.store(false, std::memory_order_release);
        release_writes_.store(true, std::memory_order_release);
        release_writes_.notify_all();
    }

    [[nodiscard]] uint32_t max_active_writes() const
    {
        return max_active_writes_.load(std::memory_order_acquire);
    }

  private:
    MemoryChunkTransport  inner_;
    std::atomic<bool>     hold_writes_{false};
    std::atomic<bool>     release_writes_{false};
    std::atomic<uint32_t> active_writes_{0};
    std::atomic<uint32_t> max_active_writes_{0};
};

class InlineWriteTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t capacity, uint64_t owner_epoch, ChunkId *chunk_id) override
    {
        return inner_.allocate_mirror_chunk(capacity, owner_epoch, chunk_id);
    }

    Status write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        return inner_.write_mirror(chunk_id, mirror, offset, data, length);
    }

    void submit_write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length,
                             ChunkTransportCompletion completion) override
    {
        completion.complete(write_mirror(chunk_id, mirror, offset, data, length));
    }

    Status advance_write(ChunkId chunk_id, uint64_t expected, uint64_t acknowledged) override
    {
        return inner_.advance_write(chunk_id, expected, acknowledged);
    }

    Status read_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        return inner_.read_mirror(chunk_id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override
    {
        return inner_.query_chunk(chunk_id, layout);
    }

    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged) override
    {
        return inner_.seal_chunk(chunk_id, owner_epoch, acknowledged);
    }

  private:
    MemoryChunkTransport inner_;
};

class FailingAdvanceTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t capacity, uint64_t owner_epoch, ChunkId *chunk_id) override
    {
        return inner_.allocate_mirror_chunk(capacity, owner_epoch, chunk_id);
    }

    Status write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        return inner_.write_mirror(chunk_id, mirror, offset, data, length);
    }

    Status advance_write(ChunkId chunk_id, uint64_t expected, uint64_t acknowledged) override
    {
        const uint32_t call = advance_calls_.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (fail_on_call_.load(std::memory_order_acquire) == call) {
            return Status::unavailable("injected advance failure");
        }
        return inner_.advance_write(chunk_id, expected, acknowledged);
    }

    Status read_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        return inner_.read_mirror(chunk_id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override
    {
        return inner_.query_chunk(chunk_id, layout);
    }

    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged) override
    {
        return inner_.seal_chunk(chunk_id, owner_epoch, acknowledged);
    }

    void fail_advance_call(uint32_t call)
    {
        advance_calls_.store(0, std::memory_order_release);
        fail_on_call_.store(call, std::memory_order_release);
    }

    void clear_failure()
    {
        fail_on_call_.store(0, std::memory_order_release);
    }

  private:
    MemoryChunkTransport  inner_;
    std::atomic<uint32_t> advance_calls_{0};
    std::atomic<uint32_t> fail_on_call_{0};
};

class AmbiguousAdvanceTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t capacity, uint64_t owner_epoch, ChunkId *chunk_id) override
    {
        return inner_.allocate_mirror_chunk(capacity, owner_epoch, chunk_id);
    }

    Status write_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        return inner_.write_mirror(chunk_id, mirror, offset, data, length);
    }

    Status advance_write(ChunkId chunk_id, uint64_t expected, uint64_t acknowledged) override
    {
        Status status = inner_.advance_write(chunk_id, expected, acknowledged);
        if (status.ok() && fail_next_.exchange(false, std::memory_order_acq_rel)) {
            failed_chunk_ = chunk_id;
            return Status::unavailable("injected lost advance response");
        }
        return status;
    }

    Status read_mirror(ChunkId chunk_id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        return inner_.read_mirror(chunk_id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override
    {
        return inner_.query_chunk(chunk_id, layout);
    }

    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged) override
    {
        return inner_.seal_chunk(chunk_id, owner_epoch, acknowledged);
    }

    void fail_next_advance()
    {
        fail_next_.store(true, std::memory_order_release);
    }

    [[nodiscard]] ChunkId failed_chunk() const
    {
        return failed_chunk_;
    }

  private:
    MemoryChunkTransport inner_;
    std::atomic<bool>    fail_next_{false};
    ChunkId              failed_chunk_;
};

TEST(ChunkPageStore, AsyncPackPipelineFansOutMirrorWrites)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<BlockingWriteTransport>();
    ChunkPageStore store({.tree_id = 29, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1, .max_concurrent_packs = 1},
                         catalog, transport);
    const uint8_t  value = 7;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());

    transport->hold_writes();
    CompletionState completed;
    ASSERT_TRUE(store.submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    transport->wait_for_concurrent_writes(3);
    EXPECT_GE(transport->max_active_writes(), 3U);
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire));
    transport->release_writes();
    completed.done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    EXPECT_LE(transport->max_active_writes(), 3U);
    ASSERT_NE(catalog->load(29), nullptr);
}

TEST(ChunkPageStore, InlineAsyncMirrorCompletionStillPublishesManifest)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<InlineWriteTransport>();
    ChunkPageStore store({.tree_id = 30, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  value = 9;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    CompletionState completed;
    ASSERT_TRUE(store.submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    completed.done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    ASSERT_NE(catalog->load(30), nullptr);
}

TEST(ChunkPageStore, AsyncMirrorFailureStopsPipelineWithoutPublishing)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 31, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  value = 4;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    store.inject_mirror_write_failures(1);
    CompletionState completed;
    ASSERT_TRUE(store.submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    completed.done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
    EXPECT_EQ(catalog->load(31), nullptr);
    EXPECT_GT(store.stats().orphan_bytes, 0U);
}

TEST(ChunkPageStore, AsyncRotationSealsPreexistingPartialChunk)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id         = 32,
                          .owner_epoch     = 1,
                          .pack_bytes      = 8192,
                          .max_chunk_bytes = 16384,
                          .page_alignment  = 1,
                          .iu_size         = 1},
                         catalog, transport);
    publish_raw_generation(&store, 1);
    auto first = catalog->load(32);
    ASSERT_NE(first, nullptr);
    const ChunkId old_chunk = first->packs.front().ref.chunk_id;

    const uint8_t value = 2;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    CompletionState completed;
    ASSERT_TRUE(store.submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    completed.done.wait(false, std::memory_order_acquire);
    ASSERT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    ChunkLayout old_layout;
    ASSERT_TRUE(transport->query_chunk(old_chunk, &old_layout).ok());
    EXPECT_TRUE(old_layout.sealed);
}

TEST(ChunkPageStore, PartialAdvanceFailureRetriesOnFreshChunk)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<FailingAdvanceTransport>();
    ChunkPageStore store({.tree_id = 33, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  value = 6;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());

    transport->fail_advance_call(2);
    CompletionState failed;
    ASSERT_TRUE(store.submit_fsync({.context = &failed, .complete_fn = &record_completion}).ok());
    failed.done.wait(false, std::memory_order_acquire);
    ASSERT_EQ(failed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
    EXPECT_EQ(catalog->load(33), nullptr);

    transport->clear_failure();
    CompletionState retried;
    ASSERT_TRUE(store.submit_fsync({.context = &retried, .complete_fn = &record_completion}).ok());
    retried.done.wait(false, std::memory_order_acquire);
    EXPECT_EQ(retried.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    ASSERT_NE(catalog->load(33), nullptr);
}

TEST(ChunkPageStore, ShutdownCancelsAndDrainsBlockedPackWrites)
{
    auto catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto transport = std::make_shared<BlockingWriteTransport>();
    auto store     = std::make_unique<ChunkPageStore>(
        ChunkPageStore::Config{.tree_id = 34, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t value = 8;
    ASSERT_TRUE(store->write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store->sync().ok());
    ASSERT_TRUE(store->write_at(0, &value, 1).ok());
    transport->hold_writes();
    CompletionState completed;
    ASSERT_TRUE(store->submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    transport->wait_for_concurrent_writes(3);
    std::thread closer([&store] { store.reset(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    transport->release_writes();
    closer.join();
    EXPECT_TRUE(completed.done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
    EXPECT_EQ(catalog->load(34), nullptr);
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

TEST(ChunkPageStore, CheckpointCowsOnlyChangedLogicalPacks)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 35, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    publish_raw_generation(&store, 1);
    auto first = catalog->load(35);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->packs.size(), 3U);

    const uint8_t changed = 2;
    ASSERT_TRUE(store.write_at(8192, &changed, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &changed, 1).ok());
    ASSERT_TRUE(store.sync().ok());

    auto second = catalog->load(35);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(second->packs.size(), 3U);
    EXPECT_EQ(second->packs[1].ref.chunk_id, first->packs[1].ref.chunk_id);
    EXPECT_EQ(second->packs[1].ref.offset, first->packs[1].ref.offset);
    EXPECT_NE(second->packs[0].ref.checksum, first->packs[0].ref.checksum);
    EXPECT_NE(second->packs[2].ref.checksum, first->packs[2].ref.checksum);
    EXPECT_EQ(second->packs_reused, 1U);
    EXPECT_EQ(second->pack_bytes_reused, 4096U);
    EXPECT_EQ(store.stats().packs_reused, 1U);
    EXPECT_EQ(store.stats().pack_bytes_reused, 4096U);
}

TEST(ChunkPageStore, CheckpointReusesUnchangedReferenceSegmentImage)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 48, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    publish_raw_generation(&store, 1);
    auto first = catalog->load(48);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->reference_segments.size(), 1U);
    const uint64_t object_id = first->reference_segments[0].object_id;

    publish_raw_generation(&store, 1);
    auto second = catalog->load(48);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(second->reference_segments.size(), 1U);
    EXPECT_EQ(second->reference_segments[0].owner_tree_id, 48U);
    EXPECT_EQ(second->reference_segments[0].object_id, object_id);
    EXPECT_TRUE(second->reference_segments[0].reused);
    EXPECT_EQ(catalog->reference_segment_count(48), 1U);
}

TEST(ChunkPageStore, ChildSharesThenCowsImmutableReferenceSegmentImage)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 49, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 1);
    auto source_manifest = catalog->load(49);
    ASSERT_NE(source_manifest, nullptr);
    ASSERT_EQ(source_manifest->reference_segments.size(), 1U);

    ChunkPageStore child({.tree_id = 50, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 1);
    auto shared = catalog->load(50);
    ASSERT_NE(shared, nullptr);
    ASSERT_EQ(shared->reference_segments.size(), 1U);
    EXPECT_EQ(shared->reference_segments[0].owner_tree_id, 49U);
    EXPECT_EQ(shared->reference_segments[0].object_id, source_manifest->reference_segments[0].object_id);
    EXPECT_TRUE(shared->reference_segments[0].reused);
    EXPECT_EQ(catalog->reference_segment_count(50), 0U);

    publish_raw_generation(&child, 2);
    auto cow = catalog->load(50);
    ASSERT_NE(cow, nullptr);
    ASSERT_EQ(cow->reference_segments.size(), 1U);
    EXPECT_EQ(cow->reference_segments[0].owner_tree_id, 50U);
    EXPECT_NE(cow->reference_segments[0].object_id, shared->reference_segments[0].object_id);
    EXPECT_FALSE(cow->reference_segments[0].reused);
    EXPECT_EQ(catalog->reference_segment_count(50), 1U);

    std::array<uint8_t, 1> source_value{};
    ASSERT_TRUE(source.read_at(8192, source_value.data(), source_value.size()).ok());
    EXPECT_EQ(source_value[0], 1U);
}

TEST(ChunkPageStore, TreeZeroCanShareReferenceSegmentsWithAnotherLineage)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 0, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 4);

    ChunkPageStore child({.tree_id = 51, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 4);
    auto manifest = catalog->load(51);
    ASSERT_NE(manifest, nullptr);
    ASSERT_EQ(manifest->reference_segments.size(), 1U);
    EXPECT_EQ(manifest->reference_segments[0].owner_tree_id, 0U);

    std::array<uint8_t, 1> value{};
    ASSERT_TRUE(child.read_at(8192, value.data(), value.size()).ok());
    EXPECT_EQ(value[0], 4U);
}

TEST(ChunkPageStore, BoundedMaterializationMakesChildPacksExclusive)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 53, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 3);

    ChunkPageStore child({.tree_id                        = 54,
                          .owner_epoch                    = 1,
                          .pack_bytes                     = 4096,
                          .page_alignment                 = 1,
                          .iu_size                        = 1,
                          .materialization_bytes_per_pass = 4096},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 3);
    EXPECT_GT(child.stats().shared_packs, 0U);

    uint64_t total_written    = 0;
    bool     complete         = false;
    uint64_t prior_generation = catalog->load(54)->generation;
    while (!complete) {
        uint64_t written = 0;
        ASSERT_TRUE(child.materialize_ownership(&written, &complete).ok());
        EXPECT_GT(written, 0U);
        EXPECT_LE(written, 4096U);
        total_written += written;
        auto current = catalog->load(54);
        ASSERT_NE(current, nullptr);
        EXPECT_EQ(current->generation, ++prior_generation);
    }
    auto exclusive = catalog->load(54);
    ASSERT_NE(exclusive, nullptr);
    EXPECT_EQ(total_written, exclusive->logical_size);
    EXPECT_TRUE(std::all_of(exclusive->packs.begin(), exclusive->packs.end(),
                            [](const ChunkPagePack &pack) { return pack.owner_tree_id == 54; }));
    EXPECT_TRUE(std::all_of(exclusive->reference_segments.begin(), exclusive->reference_segments.end(),
                            [](const ChunkReferenceSegment &segment) { return segment.owner_tree_id == 54; }));
    const auto stats = child.stats();
    EXPECT_EQ(stats.shared_packs, 0U);
    EXPECT_EQ(stats.materialization_bytes_written, total_written);
    EXPECT_EQ(stats.materialization_packs_written, exclusive->packs.size());
    EXPECT_GT(stats.materialization_passes, 0U);
    EXPECT_EQ(stats.materialization_failures, 0U);

    std::array<uint8_t, 1> source_value{};
    ASSERT_TRUE(source.read_at(8192, source_value.data(), source_value.size()).ok());
    EXPECT_EQ(source_value[0], 3U);
}

TEST(ChunkPageStore, FailedMaterializationKeepsPriorGenerationRetryable)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 55, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 5);
    ChunkPageStore child({.tree_id = 56, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 5);
    auto prior = catalog->load(56);
    ASSERT_NE(prior, nullptr);

    child.inject_mirror_write_failures(1);
    uint64_t written  = 0;
    bool     complete = false;
    EXPECT_EQ(child.materialize_ownership(&written, &complete).code(), Code::kUnavailable);
    EXPECT_EQ(catalog->load(56), prior);
    EXPECT_GT(child.stats().orphan_bytes, 0U);
    EXPECT_EQ(child.stats().materialization_failures, 1U);

    child.inject_mirror_write_failures(0);
    ASSERT_TRUE(child.materialize_ownership(&written, &complete).ok());
    EXPECT_GT(written, 0U);
    EXPECT_GT(catalog->load(56)->generation, prior->generation);
}

TEST(ChunkPageStore, AmbiguousMaterializationAdvanceRetriesOnFreshChunk)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<AmbiguousAdvanceTransport>();
    ChunkPageStore source({.tree_id = 59, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 8);
    ChunkPageStore child({.tree_id = 60, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 8);

    transport->fail_next_advance();
    uint64_t written  = 0;
    bool     complete = false;
    EXPECT_EQ(child.materialize_ownership(&written, &complete).code(), Code::kUnavailable);
    const ChunkId abandoned = transport->failed_chunk();
    ASSERT_FALSE(abandoned.empty());

    ASSERT_TRUE(child.materialize_ownership(&written, &complete).ok());
    ASSERT_GT(written, 0U);
    auto manifest = catalog->load(60);
    ASSERT_NE(manifest, nullptr);
    auto owned = std::find_if(manifest->packs.begin(), manifest->packs.end(),
                              [](const ChunkPagePack &pack) { return pack.owner_tree_id == 60; });
    ASSERT_NE(owned, manifest->packs.end());
    EXPECT_NE(owned->ref.chunk_id, abandoned);
}

TEST(ChunkPageStore, MaterializationBudgetIsAtLeastOneConfiguredPack)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 61, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 2);
    ChunkPageStore child({.tree_id                        = 62,
                          .owner_epoch                    = 1,
                          .pack_bytes                     = 4096,
                          .page_alignment                 = 1,
                          .iu_size                        = 1,
                          .materialization_bytes_per_pass = 1},
                         catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 2);

    uint64_t written  = 0;
    bool     complete = false;
    ASSERT_TRUE(child.materialize_ownership(&written, &complete).ok());
    EXPECT_EQ(written, 4096U);
    EXPECT_FALSE(complete);
}

TEST(ChunkPageStore, SnapshotInheritanceRejectsMismatchedStorageGeometry)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 63, .owner_epoch = 1, .pack_bytes = 8192, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 2);

    ChunkPageStore child({.tree_id = 64, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    EXPECT_EQ(child.inherit_snapshot_from(source).code(), Code::kInvalidArgument);
}

TEST(ChunkPageStore, StaleMaterializationCannotReplaceForegroundCheckpoint)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 57, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    publish_raw_generation(&source, 7);
    ChunkPageStore materializer(
        {.tree_id = 58, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1}, catalog, transport);
    ASSERT_TRUE(materializer.inherit_snapshot_from(source).ok());
    publish_raw_generation(&materializer, 7);

    ChunkPageStore winner({.tree_id = 58, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                          catalog, transport);
    const uint8_t  winner_value = 9;
    ASSERT_TRUE(winner.write_at(8192, &winner_value, 1).ok());
    ASSERT_TRUE(winner.sync().ok());
    ASSERT_TRUE(winner.write_at(0, &winner_value, 1).ok());

    catalog->block_next_publish_for_tests();
    Status      materialize_status;
    uint64_t    materialized_bytes = 0;
    bool        complete           = false;
    std::thread cleanup(
        [&] { materialize_status = materializer.materialize_ownership(&materialized_bytes, &complete); });
    catalog->wait_for_blocked_publish_for_tests();
    ASSERT_TRUE(winner.sync().ok());
    auto published_winner = catalog->load(58);
    ASSERT_NE(published_winner, nullptr);
    catalog->release_blocked_publish_for_tests();
    cleanup.join();

    EXPECT_EQ(materialize_status.code(), Code::kUnavailable);
    EXPECT_EQ(catalog->load(58), published_winner);
    EXPECT_GT(materializer.stats().orphan_bytes, 0U);
}

TEST(ChunkPageStore, ReopensLegacyManifestChecksumWithoutOwnerFields)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 52, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                         catalog, transport);
    publish_raw_generation(&store, 6);
    catalog->downgrade_active_manifest_for_tests(52);

    ChunkPageStore reopened({.tree_id = 52, .owner_epoch = 1, .pack_bytes = 4096, .page_alignment = 1, .iu_size = 1},
                            catalog, transport);
    std::array<uint8_t, 1> value{};
    ASSERT_TRUE(reopened.read_at(8192, value.data(), value.size()).ok());
    EXPECT_EQ(value[0], 6U);
}

TEST(ChunkPageStore, CatalogPublishesIndependentTreeLineagesConcurrently)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore left({.tree_id = 36, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    ChunkPageStore right({.tree_id = 37, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);

    auto        publish = [](ChunkPageStore *store, uint8_t value) { publish_raw_generation(store, value); };
    std::thread left_worker(publish, &left, 1);
    std::thread right_worker(publish, &right, 2);
    left_worker.join();
    right_worker.join();

    auto left_manifest  = catalog->load(36);
    auto right_manifest = catalog->load(37);
    ASSERT_NE(left_manifest, nullptr);
    ASSERT_NE(right_manifest, nullptr);
    EXPECT_EQ(left_manifest->tree_id, 36U);
    EXPECT_EQ(right_manifest->tree_id, 37U);
    EXPECT_EQ(left_manifest->generation, 1U);
    EXPECT_EQ(right_manifest->generation, 1U);
}

TEST(ChunkPageStore, CatalogPublishAndReclaimPreserveConcurrentHistory)
{
    MemoryRootCatalog catalog(1);
    auto              publish_generation = [&catalog](uint64_t tree_id, uint64_t generation) {
        auto manifest             = std::make_shared<ChunkManifest>();
        manifest->tree_id         = tree_id;
        manifest->owner_epoch     = 1;
        manifest->generation      = generation;
        manifest->published_at_ms = generation;
        ASSERT_TRUE(catalog.publish(tree_id, generation - 1, 1, std::move(manifest)).ok());
    };
    for (uint64_t generation = 1; generation <= 3; ++generation) {
        publish_generation(41, generation);
    }

    std::atomic<bool> start{false};
    std::thread       publisher([&] {
        start.wait(false, std::memory_order_acquire);
        for (uint64_t generation = 1; generation <= 100; ++generation) {
            publish_generation(40, generation);
        }
    });
    std::thread       cross_tree_reclaimer([&] {
        start.wait(false, std::memory_order_acquire);
        for (uint64_t iteration = 0; iteration < 100; ++iteration) {
            catalog.reclaim_before(41, 4);
        }
    });
    start.store(true, std::memory_order_release);
    start.notify_all();
    publisher.join();
    cross_tree_reclaimer.join();
    EXPECT_EQ(catalog.retained_manifest_count(40), 100U);
    for (uint64_t generation = 1; generation <= 100; ++generation) {
        EXPECT_NE(catalog.load_generation(40, generation), nullptr);
    }

    std::thread same_tree_publisher([&] {
        for (uint64_t generation = 101; generation <= 200; ++generation) {
            publish_generation(40, generation);
        }
    });
    std::thread same_tree_reclaimer([&] {
        for (uint64_t iteration = 0; iteration < 100; ++iteration) {
            catalog.reclaim_before(40, 201);
        }
    });
    same_tree_publisher.join();
    same_tree_reclaimer.join();
    catalog.reclaim_before(40, 201);
    EXPECT_EQ(catalog.retained_manifest_count(40), 2U);
    EXPECT_NE(catalog.load_generation(40, 199), nullptr);
    EXPECT_NE(catalog.load_generation(40, 200), nullptr);
}

TEST(ChunkPageStore, SharedChildPacksAreNotReclaimedWithSourceHistory)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 38, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    publish_raw_generation(&source, 1);
    auto source_first = catalog->load(38);
    ASSERT_NE(source_first, nullptr);

    ChunkPageStore child({.tree_id = 39, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    publish_raw_generation(&child, 1);
    auto child_first = catalog->load(39);
    ASSERT_NE(child_first, nullptr);
    ASSERT_EQ(child_first->packs_reused, source_first->packs.size());

    publish_raw_generation(&source, 2);
    publish_raw_generation(&source, 3);
    const uint64_t source_reference_segments = catalog->reference_segment_count(38);
    source_first.reset();
    EXPECT_EQ(catalog->reclaim_before(38, 4), 0U);
    EXPECT_EQ(catalog->reference_segment_count(38), source_reference_segments);

    std::array<uint8_t, 1> child_value{};
    ASSERT_TRUE(child.read_at(8192, child_value.data(), child_value.size()).ok());
    EXPECT_EQ(child_value[0], 1U);
}

TEST(ChunkPageStore, InheritanceRejectsSelfConsistentManifestCoverageGap)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 42, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1, .layout_validity_ms = 0},
                          catalog, transport);
    publish_raw_generation(&source, 1);
    catalog->corrupt_active_pack_layout_for_tests(42);

    ChunkPageStore child({.tree_id = 43, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    EXPECT_EQ(child.inherit_snapshot_from(source).code(), Code::kCorruption);
    uint8_t value = 0;
    EXPECT_EQ(source.read_at(8192, &value, 1).code(), Code::kCorruption);
}

TEST(ChunkPageStore, ShutdownCancelsBlockedAsyncReuseVerification)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<BlockingReadTransport>();
    ChunkPageStore source({.tree_id = 44, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    publish_raw_generation(&source, 1);

    auto child = std::make_unique<ChunkPageStore>(
        ChunkPageStore::Config{.tree_id = 45, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    ASSERT_TRUE(child->inherit_snapshot_from(source).ok());
    const uint8_t value = 1;
    ASSERT_TRUE(child->write_at(8192, &value, 1).ok());
    ASSERT_TRUE(child->sync().ok());
    ASSERT_TRUE(child->write_at(0, &value, 1).ok());

    transport->hold_reads();
    CompletionState completed;
    ASSERT_TRUE(child->submit_fsync({.context = &completed, .complete_fn = &record_completion}).ok());
    transport->wait_for_read();
    std::thread closer([&child] { child.reset(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    transport->release_reads();
    closer.join();
    EXPECT_TRUE(completed.done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
    EXPECT_EQ(catalog->load(45), nullptr);
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

TEST(ChunkPageStore, FailedAllReusedPublicationDoesNotAccountSourceBytesAsOrphans)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore source({.tree_id = 46, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    publish_raw_generation(&source, 1);

    ChunkPageStore child({.tree_id = 47, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    ASSERT_TRUE(child.inherit_snapshot_from(source).ok());
    const uint8_t same = 1;
    ASSERT_TRUE(child.write_at(8192, &same, 1).ok());
    ASSERT_TRUE(child.sync().ok());
    ASSERT_TRUE(child.write_at(0, &same, 1).ok());
    catalog->set_owner_epoch(2);

    EXPECT_EQ(child.sync().code(), Code::kUnavailable);
    EXPECT_EQ(child.stats().orphan_bytes, 0U);
    EXPECT_EQ(catalog->load(47), nullptr);
    EXPECT_EQ(catalog->reference_segment_count(47), 0U);
    EXPECT_EQ(child.reclaim_orphans(), 0U);
    EXPECT_EQ(catalog->reference_segment_count(47), 0U);
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

TEST(ChunkPageStore, HardCapsConfiguredChunkCapacityAt256MiB)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id         = 26,
                          .owner_epoch     = 1,
                          .pack_bytes      = 1U * 1024U * 1024U,
                          .max_chunk_bytes = 512U * 1024U * 1024U,
                          .page_alignment  = 1,
                          .iu_size         = 1},
                         catalog, transport);
    const uint8_t  value = 2;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());

    auto manifest = catalog->load(26);
    ASSERT_NE(manifest, nullptr);
    ASSERT_FALSE(manifest->packs.empty());
    ChunkLayout layout;
    ASSERT_TRUE(transport->query_chunk(manifest->packs.front().ref.chunk_id, &layout).ok());
    EXPECT_EQ(layout.logical_capacity, 256U * 1024U * 1024U);
}

TEST(ChunkPageStore, CatalogRejectsManifestGenerationOutsidePublicationFence)
{
    MemoryRootCatalog catalog(1);
    auto              skipped = std::make_shared<ChunkManifest>();
    skipped->tree_id          = 27;
    skipped->owner_epoch      = 1;
    skipped->generation       = 2;
    EXPECT_EQ(catalog.publish(27, 0, 1, skipped).code(), Code::kInvalidArgument);
    EXPECT_EQ(catalog.load(27), nullptr);

    auto first         = std::make_shared<ChunkManifest>();
    first->tree_id     = 27;
    first->owner_epoch = 1;
    first->generation  = 1;
    EXPECT_TRUE(catalog.publish(27, 0, 1, first).ok());
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

TEST(ChunkPageStore, ReusesChecksummedPackForAdjacentReads)
{
    auto                 catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                 transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore       store({.tree_id = 24, .owner_epoch = 1, .pack_bytes = 16U * 1024U, .iu_size = 1}, catalog,
                               transport);
    std::vector<uint8_t> bytes(8196, 6);
    ASSERT_TRUE(store.write_at(8192, bytes.data() + 8192, 4).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, bytes.data(), 8192).ok());
    ASSERT_TRUE(store.sync().ok());

    std::array<uint8_t, 2> first{};
    std::array<uint8_t, 2> second{};
    CompletionState        first_done;
    CompletionState        second_done;
    ASSERT_NE(store.submit_read(8192, first.data(), first.size(),
                                {.context = &first_done, .complete_fn = &record_completion}),
              0U);
    first_done.done.wait(false, std::memory_order_acquire);
    ASSERT_EQ(first_done.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    ASSERT_NE(store.submit_read(8194, second.data(), second.size(),
                                {.context = &second_done, .complete_fn = &record_completion}),
              0U);
    second_done.done.wait(false, std::memory_order_acquire);
    ASSERT_EQ(second_done.code.load(std::memory_order_relaxed), static_cast<int>(Code::kOk));
    EXPECT_EQ(store.stats().pack_reads, 1U);
    EXPECT_TRUE(std::all_of(first.begin(), first.end(), [](uint8_t value) { return value == 6; }));
    EXPECT_TRUE(std::all_of(second.begin(), second.end(), [](uint8_t value) { return value == 6; }));
}

TEST(ChunkPageStore, StaleCheckpointCannotPublishOverConcurrentWinner)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore stale({.tree_id = 28, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    ChunkPageStore winner({.tree_id = 28, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  stale_value  = 3;
    const uint8_t  winner_value = 7;
    ASSERT_TRUE(stale.write_at(8192, &stale_value, 1).ok());
    ASSERT_TRUE(stale.sync().ok());
    ASSERT_TRUE(stale.write_at(0, &stale_value, 1).ok());
    ASSERT_TRUE(winner.write_at(8192, &winner_value, 1).ok());
    ASSERT_TRUE(winner.sync().ok());
    ASSERT_TRUE(winner.write_at(0, &winner_value, 1).ok());

    catalog->block_next_publish_for_tests();
    CompletionState stale_done;
    ASSERT_TRUE(stale.submit_fsync({.context = &stale_done, .complete_fn = &record_completion}).ok());
    catalog->wait_for_blocked_publish_for_tests();
    ASSERT_TRUE(winner.sync().ok());
    auto published_winner = catalog->load(28);
    ASSERT_NE(published_winner, nullptr);
    catalog->release_blocked_publish_for_tests();
    stale_done.done.wait(false, std::memory_order_acquire);

    EXPECT_EQ(stale_done.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
    EXPECT_EQ(catalog->load(28), published_winner);
    EXPECT_EQ(published_winner->generation, 1U);
}

TEST(ChunkPageStore, CancelsReadAfterTransportSubmission)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<BlockingReadTransport>();
    ChunkPageStore store({.tree_id = 25, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    const uint8_t  value = 9;
    ASSERT_TRUE(store.write_at(8192, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());

    transport->hold_reads();
    CompletionState completed;
    uint8_t         out = 0;
    const uint64_t  operation_id =
        store.submit_read(8192, &out, 1, {.context = &completed, .complete_fn = &record_completion});
    ASSERT_NE(operation_id, 0U);
    transport->wait_for_read();
    store.cancel(operation_id);
    transport->release_reads();
    completed.done.wait(false, std::memory_order_acquire);

    EXPECT_EQ(completed.code.load(std::memory_order_relaxed), static_cast<int>(Code::kUnavailable));
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
    ct_chunk_page_store_options store_options = {.tree_id                        = 77,
                                                 .owner_epoch                    = 11,
                                                 .pack_bytes                     = 4096,
                                                 .iu_size                        = 1,
                                                 .max_concurrent_packs           = 2,
                                                 .materialization_bytes_per_pass = 4096};
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
    uint64_t materialized_bytes = 1;
    int32_t  materialized       = 0;
    ASSERT_EQ(ct_materialize_ownership(tree, &materialized_bytes, &materialized), 0);
    EXPECT_EQ(materialized_bytes, 0U);
    EXPECT_EQ(materialized, 1);
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
