// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/backing_io.h"
#include "btree/page_cache_identity.h"
#include "common/test_chunk_backing.h"
#include "page_writeback_test_support.h"

#include <future>

namespace crowdb::tree
{
using test_chunk::TestChunkBackend;

TEST(ChunkBacking, SealedWriteCreditProducesDirectLeafReloadWithoutActiveManifest)
{
    TestChunkBackend                  test;
    std::shared_ptr<ChunkPackBacking> pack;
    ASSERT_TRUE(test.prepare(8192, &pack).ok());
    PageWriteLimits limits;
    limits.max_frame_bytes = 4096;
    limits.compression     = compress_algo::kNone;
    limits.chunk_backed    = true;
    std::shared_ptr<PageWriteOperation> write;
    ASSERT_TRUE(PageWriteOperation::create(
                    test.scheduled, test.binding->cpu(), test.binding->lane(), test_write::write_input(), limits,
                    test.credits.credits,
                    [pack](uint64_t length, PageWriteOperation::ExtentComplete complete) {
                        complete(Status::Ok(), {.backend_id       = 17,
                                                .address          = 0,
                                                .length           = length,
                                                .content_id       = 0,
                                                .generation       = 1,
                                                .owner            = pack,
                                                .physical_metrics = {},
                                                .chunk            = pack});
                    },
                    &write)
                    .ok());
    test.credits.seal();
    ASSERT_TRUE(stdexec::sync_wait(write->sender()));
    test.binding->drain();
    const auto backing = write->result().backing;
    ASSERT_TRUE(backing);
    EXPECT_EQ(backing->storage, BackingStorage::kChunkPack);
    EXPECT_TRUE(backing->reloadable());
    EXPECT_EQ(test.empty_device.size(), 0U);
    EXPECT_EQ(test.catalog->manifest_loads, 0U);
    test_read::LoadCredits reads;
    PageLoadLimits         read_limits;
    read_limits.max_blob_bytes       = 8192;
    read_limits.max_frame_bytes      = 4096;
    read_limits.max_chunk_blob_bytes = 8192;
    AsyncStorePageLoader            loader(test.scheduled, test.binding->cpu(), test.binding->lane(), read_limits, {},
                                           reads.credits);
    std::promise<Status>            completed;
    std::shared_ptr<const LeafBase> loaded;
    loader.load_leaf({.identity = 7, .max_slot = 3, .backing = backing}, [&](Status status, auto leaf) {
        loaded = std::move(leaf);
        completed.set_value(std::move(status));
    });
    ASSERT_TRUE(completed.get_future().get().ok());
    test.binding->drain();
    ASSERT_TRUE(loaded);
    EXPECT_EQ(CellView(loaded->cell(0)).value(), Slice("value"));
    EXPECT_EQ(test.catalog->manifest_loads, 0U);
    EXPECT_EQ(test.catalog->reader_checks, 4U);
}

TEST(ChunkBacking, ProcessFenceAfterIoRejectsTheReadBeforeExposingBytes)
{
    TestChunkBackend                  test;
    std::shared_ptr<ChunkPackBacking> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    std::array<uint8_t, 32> input{};
    ASSERT_TRUE(write_chunk_backing(*pack, input, test.credits.credits.bytes).ok());
    test.catalog->fence_at = 4;
    auto backing           = std::make_shared<BackingRef>();
    backing->backend_id    = 17;
    backing->length        = input.size();
    backing->storage       = BackingStorage::kChunkPack;
    backing->chunk         = pack;
    backing->owner         = pack;
    std::array<uint8_t, 32> output{};
    std::promise<Status>    completed;
    submit_backing_read(
        *test.scheduled, backing, output.data(), output.size(), test.credits.credits.bytes,
        {.context     = &completed,
         .complete_fn = [](void  *raw,
                           Status status) { static_cast<std::promise<Status> *>(raw)->set_value(std::move(status)); },
         .owner       = backing});
    EXPECT_EQ(completed.get_future().get().code(), Code::kUnavailable);
    test.binding->drain();
    EXPECT_EQ(test.catalog->reader_checks, 4U);
}

TEST(ChunkBacking, UnknownAllocationRetainsItsExactPaidPinUntilHostReleaseSucceeds)
{
    TestChunkBackend                  test;
    std::shared_ptr<ChunkPackBacking> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    test.transport->unknown_allocate = true;
    std::array<uint8_t, 32> input{};
    EXPECT_EQ(write_chunk_backing(*pack, input, test.credits.credits.bytes).code(), Code::kUnavailable);
    if (!test.catalog->record) {
        FAIL() << "unknown Allocate lost its StageReserved record";
    }
    EXPECT_EQ(test.catalog->record->state, detail::ChunkStageState::Reserved);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 1U);
    test.catalog->fail_unpin = true;
    pack.reset();
    EXPECT_EQ(test.catalog->queue->pump(1).failed, 1U);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 1U);
    EXPECT_EQ(test.transport->allocations, 1U);
    test.catalog->fail_unpin = false;
    EXPECT_EQ(test.catalog->queue->pump(1).completed, 1U);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 0U);
}

TEST(ChunkBacking, CacheIdentityIncludesExactChunkAndStageInsteadOfSyntheticAddress)
{
    TestChunkBackend                  test;
    std::shared_ptr<ChunkPackBacking> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    auto a         = std::make_shared<BackingRef>();
    a->storage     = BackingStorage::kChunkPack;
    a->backend_id  = 17;
    a->length      = 32;
    a->owner       = pack;
    a->chunk       = pack;
    auto other     = std::make_shared<ChunkPackBacking>(*pack);
    auto b         = std::make_shared<BackingRef>(*a);
    b->chunk       = other;
    const auto key = page_cache_key(PageCacheKind::Leaf, {.identity = 7, .backing = a});
    EXPECT_EQ(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .backing = b}));
    ++other->pack.chunk.low;
    EXPECT_NE(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .backing = b}));
    other->pack = pack->pack;
    ++other->pack.stage.operation_high;
    EXPECT_NE(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .backing = b}));
    EXPECT_FALSE((BackingExclusion{.address = 0, .length = 4096}).overlaps(*a));
}
} // namespace crowdb::tree
