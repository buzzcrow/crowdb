// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_writeback_test_support.h"
#include "snapshot/captured_frames.h"
#include "test_tmp.h"

#include <algorithm>

namespace crowdb::tree
{
using namespace test_write;

TEST(PageWriteback, HeldCreditPaysAlignedCodecAndIoAfterSealAndReturnsReloadableBacking)
{
    TestWriter test;
    ASSERT_TRUE(test.prepare().ok());
    const auto bound = PageWriteOperation::allocation_bound({});
    if (!bound) {
        ADD_FAILURE() << "write allocation bound missing";
        return;
    }
    EXPECT_LE(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], bound.value());
    test.credits.seal();
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    const auto backing = test.operation->result().backing;
    ASSERT_TRUE(backing);
    EXPECT_TRUE(backing->reloadable());
    EXPECT_EQ(backing->durability, BackingDurability::kReloadable);
    EXPECT_EQ(backing->address, 8192U);
    EXPECT_EQ(test.store->purpose, PagePurpose::kBtreePage);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    std::vector<uint8_t> blob(backing->length);
    std::vector<uint8_t> frame(4096);
    ASSERT_TRUE(test.store->device->read_at(backing->address, blob.data(), blob.size()).ok());
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), frame.data(), frame.size()).ok());
    LeafFrameView decoded(frame.data(), frame.size());
    ASSERT_EQ(decoded.count(), 1U);
    EXPECT_EQ(CellView(decoded.cell(0)).value().to_string(), "value");
    EXPECT_TRUE(
        std::all_of(blob.begin() + test.operation->result().logical_bytes, blob.end(), [](auto b) { return b == 0; }));
}

TEST(PageWriteback, CancellationWaitsForWriteAndKeepsItsPermitAndExtentAlive)
{
    TestWriter test;
    test.store->deferred = true;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_EQ(test.store->writes, 1U);
    test.operation->request_stop();
    EXPECT_EQ(done.stopped.load(), 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
    EXPECT_EQ(test.credits.resources->snapshot().write_inflight_bytes,
              test.credits.account->snapshot().physical[static_cast<size_t>(AllocationKind::kIo)]);
    EXPECT_GT(test.credits.resources->snapshot().write_inflight_bytes, test.store->bytes);
    test.store->finish();
    EXPECT_EQ(test.credits.resources->snapshot().write_inflight_bytes, 0U);
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.operation->result().backing);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageWriteback, UnknownSubmissionOutcomeKeepsOwnedCallbackBufferUntilActualCompletion)
{
    TestWriter test;
    test.store->throw_after_capture = true;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kIoError);
    EXPECT_FALSE(test.operation->result().backing);
    test.operation.reset();
    test.credits.seal();
    EXPECT_GT(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
    EXPECT_EQ(test.credits.resources->snapshot().write_inflight_bytes,
              test.credits.account->snapshot().physical[static_cast<size_t>(AllocationKind::kIo)]);
    EXPECT_GT(test.credits.resources->snapshot().write_inflight_bytes, test.store->bytes);
    test.store->finish();
    EXPECT_EQ(test.credits.resources->snapshot().write_inflight_bytes, 0U);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(PageWriteback, BackendCapabilityAndMissingProgressPermitRejectBeforeWrite)
{
    TestWriter test;
    test.store->reloadable = false;
    EXPECT_EQ(test.prepare().code(), Code::kNotSupported);
    EXPECT_EQ(test.store->writes, 0U);
    test.store->reloadable = true;
    AllocationLease occupied(test.credits.credits.io, 1);
    EXPECT_EQ(test.prepare().code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.operation);
    EXPECT_EQ(test.store->writes, 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageWriteback, OverflowDependencyRetainsBackingWithoutRetainingRawInput)
{
    TestWriter                test;
    auto                      input      = write_input(4096, true);
    std::weak_ptr<const void> raw        = input.owner;
    auto                      protection = std::make_shared<int>(2);
    std::weak_ptr<int>        retained   = protection;
    input.reload_dependencies            = protection;
    ASSERT_TRUE(test.prepare(std::move(input)).ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    auto backing = test.operation->result().backing;
    ASSERT_TRUE(backing);
    EXPECT_TRUE(raw.expired());
    test.operation.reset();
    protection.reset();
    EXPECT_FALSE(retained.expired());
    backing.reset();
    EXPECT_TRUE(retained.expired());
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageWriteback, MissingOverflowProtectionAndCorruptInputNeverAllocateOrWrite)
{
    for (const bool overflow : {false, true}) {
        TestWriter test;
        auto       input = write_input(4096, overflow);
        if (!overflow) {
            ++input.info.page_id;
        }
        ASSERT_TRUE(test.prepare(std::move(input)).ok());
        Completion done;
        auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        test.runtime->drain();
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), overflow ? Code::kInvalidArgument : Code::kCorruption);
        EXPECT_EQ(test.next_address, 8192U);
        EXPECT_EQ(test.store->writes, 0U);
    }
}

TEST(PageWriteback, WriteErrorNeverProducesBackingAndDropsAllOperationCharges)
{
    TestWriter test;
    test.store->deferred = true;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    test.store->finish(Status::io_error());
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(test.operation->result().backing);
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(PageWriteback, CapturedFrameWritesToFileAndReloadsThroughExistingDurableCodec)
{
    crowdb::tree_test::TempDir dir("captured_write_");
    ASSERT_FALSE(dir.path.empty());
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 64 * 1024, 4096, &file).ok());
    TestWriter test;
    test.store->device     = std::shared_ptr<PageStore>(std::move(file));
    auto captured          = std::make_shared<PublishedTreeState>(*view(page({
                                                                            {.key = "a", .value = "captured"}
    }),
                                                                        {}, 10));
    captured->next_page_id = 1;
    std::shared_ptr<CapturedFrameStream> stream;
    ASSERT_TRUE(CapturedFrameStream::create(captured, {}, test.runtime, test.lane, {.encode = {.frame_bytes = 4096}},
                                            {}, &stream)
                    .ok());
    ASSERT_TRUE(stdexec::sync_wait(stream->sender()));
    test.runtime->drain();
    const auto &frame = stream->result().frame;
    if (!frame) {
        FAIL() << "captured frame missing";
    }
    ASSERT_TRUE(
        test.prepare(
                {.info = frame->info, .frame = frame->buffer->bytes, .owner = frame->buffer, .reload_dependencies = {}})
            .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    ASSERT_TRUE(test.store->device->sync().ok());
    auto backing = test.operation->result().backing;
    ASSERT_TRUE(backing);
    // This is page persistence/reload evidence, not an anchor checkpoint.
    std::unique_ptr<BlockPageStore> reopened;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 64 * 1024, 4096, &reopened).ok());
    auto reader    = std::make_shared<TestWriteStore>();
    reader->device = std::shared_ptr<PageStore>(std::move(reopened));
    AsyncStorePageLoader            loader(reader, test.runtime, test.lane);
    std::shared_ptr<const LeafBase> loaded;
    loader.load_leaf({.identity = 0, .backing = backing}, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        loaded = std::move(leaf);
    });
    test.runtime->drain();
    ASSERT_TRUE(loaded);
    EXPECT_EQ(CellView(loaded->cell(0)).value().to_string(), "captured");
}

} // namespace crowdb::tree
