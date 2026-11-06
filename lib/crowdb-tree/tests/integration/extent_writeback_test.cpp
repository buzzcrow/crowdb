// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/extent_writeback.h"
#include "extent_ledger_test_support.h"
#include "page_writeback_test_support.h"
#include "test_tmp.h"

namespace crowdb::tree
{
namespace
{
using namespace test_write;

struct LedgerWrites : TestWriter
{
    std::shared_ptr<ExtentLedger> ledger;

    LedgerWrites()
    {
        EXPECT_TRUE(
            ExtentLedger::create({.backend_id = 3, .address_limit = 65536}, credits.credits.reservation(), &ledger)
                .ok());
        EXPECT_TRUE(await_ledger([&](auto done) {
                        return ledger->activate(credits.credits.reservation(), std::move(done));
                    }).status.ok());
    }

    ExtentLedgerResult allocate(uint64_t bytes)
    {
        return await_ledger(
            [&](auto done) { return ledger->reserve(bytes, credits.credits.reservation(), std::move(done)); });
    }
};

ReservedWriteExtent for_write(ProtectedExtent extent)
{
    return {.backend_id       = extent.backend_id,
            .address          = extent.address,
            .length           = extent.length,
            .content_id       = extent.identity,
            .generation       = extent.generation,
            .owner            = std::move(extent.owner),
            .physical_metrics = extent.physical_metrics,
            .chunk            = {}};
}

#ifdef CROWDB_HAVE_LIBURING
TEST(LedgerWriteback, RealBlockPageStaysReadableAndCannotBeReusedUntilLastBackingOwner)
{
    crowdb::tree_test::TempDir      dir("ledger_write_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    auto         store = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    LedgerWrites test;
    ASSERT_TRUE(PageWriteOperation::create(
                    store, test.runtime, test.lane, write_input(),
                    {.max_frame_bytes = 4096, .compression = compress_algo::kNone}, test.credits.credits,
                    extent_write_reservation(test.ledger, test.credits.credits.reservation()), &test.operation)
                    .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    auto reader = test.operation->result().backing;
    ASSERT_TRUE(reader);
    EXPECT_EQ(reader->address, 8192U);
    EXPECT_EQ(reader->length, 8192U);
    const auto identity = reader->content_id;
    test.operation.reset();
    auto later = test.allocate(8192);
    ASSERT_TRUE(later.status.ok());
    EXPECT_EQ(later.extent.address, 16384U);
    std::vector<uint8_t> replacement(8192, 0xCC);
    ASSERT_TRUE(file->write_at(later.extent.address, replacement.data(), replacement.size()).ok());
    std::vector<uint8_t> blob(reader->length);
    std::vector<uint8_t> frame(4096);
    ASSERT_TRUE(file->read_at(reader->address, blob.data(), blob.size()).ok());
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), frame.data(), frame.size()).ok());
    LeafFrameView leaf(frame.data(), frame.size());
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "value");
    reader.reset();
    auto reused = test.allocate(8192);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
    EXPECT_NE(reused.extent.identity, identity);
    later  = {};
    reused = {};
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
#endif

TEST(LedgerWriteback, PendingAllocationLeavesCpuWorkerFreeAndCancellationDrainsItsOwner)
{
    LedgerWrites                       test;
    PageWriteOperation::ExtentComplete pending;
    uint64_t                           bytes = 0;
    ASSERT_TRUE(PageWriteOperation::create(
                    test.store, test.runtime, test.lane, write_input(), {}, test.credits.credits,
                    [&](uint64_t length, PageWriteOperation::ExtentComplete done) {
                        bytes   = length;
                        pending = std::move(done);
                    },
                    &test.operation)
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_TRUE(pending);
    std::atomic<bool> progressed{false};
    ASSERT_TRUE(test.runtime
                    ->try_submit(
                        test.lane,
                        [](void *context) noexcept { static_cast<std::atomic<bool> *>(context)->store(true); },
                        &progressed)
                    .ok());
    test.runtime->drain();
    EXPECT_TRUE(progressed.load());
    test.operation->request_stop();
    auto allocated = test.allocate(bytes);
    ASSERT_TRUE(allocated.status.ok());
    pending(Status::Ok(), for_write(std::move(allocated.extent)));
    pending = {};
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_EQ(test.store->writes, 0U);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(LedgerWriteback, UnknownAllocationSubmissionDropsLateExtentWithoutIssuingWrite)
{
    LedgerWrites                       test;
    PageWriteOperation::ExtentComplete pending;
    uint64_t                           bytes = 0;
    ASSERT_TRUE(PageWriteOperation::create(
                    test.store, test.runtime, test.lane, write_input(), {}, test.credits.credits,
                    [&](uint64_t length, PageWriteOperation::ExtentComplete complete) {
                        bytes   = length;
                        pending = std::move(complete);
                        throw std::runtime_error("unknown allocation submission");
                    },
                    &test.operation)
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.store->writes, 0U);
    test.operation.reset();
    auto allocated = test.allocate(bytes);
    ASSERT_TRUE(allocated.status.ok());
    pending(Status::Ok(), for_write(std::move(allocated.extent)));
    pending = {};
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.store->writes, 0U);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(LedgerWriteback, InlineAllocationSuccessThenThrowDoesNotStartIo)
{
    LedgerWrites test;
    auto         allocated = test.allocate(8192);
    ASSERT_TRUE(allocated.status.ok());
    ASSERT_TRUE(PageWriteOperation::create(
                    test.store, test.runtime, test.lane, write_input(),
                    {.max_frame_bytes = 4096, .compression = compress_algo::kNone}, test.credits.credits,
                    [extent = std::make_shared<ProtectedExtent>(allocated.extent)](
                        uint64_t, PageWriteOperation::ExtentComplete complete) {
                        complete(Status::Ok(), for_write(*extent));
                        throw std::runtime_error("after inline allocation");
                    },
                    &test.operation)
                    .ok());
    allocated = {};
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.store->writes, 0U);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
}
} // namespace
} // namespace crowdb::tree
