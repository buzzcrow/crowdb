// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "btree/tree_core_access.h"
#include "common/test_tree_core_chunk.h"
#include "snapshot/chunk_root_recovery.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
using test_chunk::TestCoreChunk;

TEST(TreeCoreChunk, SingleIoCheckpointColdReadAndReopenKeepWalCoveragePair)
{
    TestCoreChunk test;
    auto          status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(20000, 'v');
    ASSERT_TRUE(test.put(1, value).ok());
    test.store->request_wal_offset(8192);
    uint64_t coverage = 0;
    uint64_t sequence = 0;
    status            = test.tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 1U);
    EXPECT_EQ(sequence, 1U);
    EXPECT_EQ(test.store->durable_wal_offset(), 8192U);
    EXPECT_EQ(test.store->manifest_generation(), 1U);
    auto held = test.tree->get_view("key");
    ASSERT_TRUE(held.found());
    EXPECT_EQ(held.value().to_string(), value);
    test.tree.reset();
    EXPECT_EQ(held.value().to_string(), value);
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto recovered_limits = TreeCoreAccess::get(*test.tree)->load_limits();
    EXPECT_EQ(recovered_limits.max_frame_bytes, test.config.frame_bytes);
    EXPECT_EQ(recovered_limits.max_blob_bytes, 8192U);
    GetView reopened;
    status = TreeCoreAccess::get(*test.tree)->get("key", &reopened);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(reopened.found());
    EXPECT_EQ(reopened.value().to_string(), value);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
    ASSERT_TRUE(test.put(2, "next").ok());
    test.store->request_wal_offset(12288);
    status = test.tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 2U);
    EXPECT_EQ(sequence, 2U);
    EXPECT_EQ(test.store->durable_wal_offset(), 12288U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "next");
}

TEST(TreeCoreChunk, LostCommitReplyResolvesOneRootWithoutDuplicateWindows)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "value").ok());
    test.backend.catalog->lose_commit = true;
    auto status                       = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.backend.catalog->commit_calls, 1U);
    EXPECT_EQ(test.backend.catalog->prepare_calls, 1U);
    EXPECT_EQ(test.backend.catalog->abort_calls, 0U);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
}

TEST(TreeCoreChunk, ANewWalRequestCannotChangeThePairOfAnAcceptedCheckpoint)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "value").ok());
    test.store->request_wal_offset(4096);
    test.backend.transport->before_allocate = [&] { test.store->request_wal_offset(8192); };
    auto status                             = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.store->requested_wal_offset(), 8192U);
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
}

TEST(TreeCoreChunk, UnqueryableCommitBlocksAnotherCheckpointUntilValidatedReopen)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "value").ok());
    test.store->request_wal_offset(4096);
    test.backend.catalog->lose_commit = true;
    test.backend.catalog->fail_query  = true;
    EXPECT_EQ(test.tree->snapshot().code(), Code::kUnavailable);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 0U);
    EXPECT_EQ(test.store->durable_wal_offset(), 0U);
    EXPECT_EQ(test.tree->snapshot().code(), Code::kUnavailable);
    EXPECT_EQ(test.backend.catalog->begin_calls, 1U);
    test.tree.reset();
    test.backend.catalog->fail_query = false;
    auto status                      = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "value");
}

TEST(TreeCoreChunk, ASecondWriterCannotClaimTheSameOpenFacade)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    std::unique_ptr<Crowdbtree> second;
    EXPECT_EQ(Crowdbtree::open(test.config, &second).code(), Code::kUnavailable);
    EXPECT_FALSE(second);
    test.tree.reset();
    EXPECT_EQ(Crowdbtree::open(test.config, &second).code(), Code::kUnavailable);
    ASSERT_TRUE(test.open().ok());
}

TEST(TreeCoreChunk, DetachedNativeCursorReadsExactPacksAfterTreeClose)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, std::string(20000, 's')).ok());
    auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::unique_ptr<NativeFrameIterator> cursor;
    status = test.tree->open_native_frame_iterator(nullptr, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto info = cursor->stream_info();
    test.tree.reset();
    size_t count = 0;
    bool   done  = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        status = cursor->next_owned(&frame, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (frame) {
            ++count;
            EXPECT_TRUE(frame_validate(frame->bytes.data(), frame->bytes.size()));
        }
    }
    EXPECT_GT(count, 1U);
    EXPECT_EQ(count, info.frame_count);
}

TEST(TreeCoreChunk, ReopenRejectsCorruptPhysicalPackEvenWhenCatalogMetadataIsValid)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, std::string(12000, 'x')).ok());
    auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.tree.reset();
    const auto found =
        std::find_if(test.backend.catalog->records.begin(), test.backend.catalog->records.end(), [](const auto &item) {
            return item.second.purpose == static_cast<uint32_t>(PagePurpose::kBtreePage);
        });
    ASSERT_NE(found, test.backend.catalog->records.end());
    const auto physical = test.backend.transport->ids.at({found->second.chunk_high, found->second.chunk_low});
    test.backend.transport->memory.corrupt_mirror(physical, 0, 64);
    test.backend.transport->memory.corrupt_mirror(physical, 1, 64);
    status = test.open();
    EXPECT_EQ(status.code(), Code::kCorruption) << status.to_string();
    EXPECT_FALSE(test.tree);
}

TEST(TreeCoreChunk, ExactBootstrapReadsOnlyNamedRootAndCannotPublishADivergentSuccessor)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "first").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    ASSERT_TRUE(test.put(2, "second").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    auto status = test.open(1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.store->manifest_generation(), 1U);
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "first");
    ASSERT_TRUE(test.put(2, "branch").ok());
    EXPECT_EQ(test.tree->snapshot().code(), Code::kUnavailable);
    EXPECT_EQ(test.backend.catalog->current_generation, 2U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "second");
}

TEST(TreeCoreChunk, ClosedWriterQueuesQuiescenceWhileIndependentReaderRemainsUsable)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "value").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    std::unique_ptr<NativeFrameIterator> cursor;
    ASSERT_TRUE(test.tree->open_native_frame_iterator(nullptr, &cursor).ok());
    auto owner = test.backend.catalog;
    TreeCoreAccess::get(*test.tree)->close_and_drain();
    EXPECT_EQ(owner->writer_releases, 0U);
    EXPECT_GT(owner->queue->snapshot().pending, 0U);
    owner->fail_writer_release = true;
    const auto failed          = owner->queue->pump(64);
    EXPECT_EQ(failed.failed, 1U);
    owner->fail_writer_release = false;
    EXPECT_GT(owner->queue->pump(64).completed, 0U);
    EXPECT_EQ(owner->writer_releases, 2U);
    std::optional<NativeFrameView> frame;
    bool                           done = false;
    ASSERT_TRUE(cursor->next_owned(&frame, &done).ok());
    ASSERT_TRUE(frame);
    if (frame) {
        EXPECT_TRUE(frame_validate(frame->bytes.data(), frame->bytes.size()));
    }
}

} // namespace crowdb::tree
