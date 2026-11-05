// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "async_completion_adapter.h"
#include "page_writeback_test_support.h"
#include "test_tmp.h"

#include <future>

namespace crowdb::tree
{
using namespace test_write;

#ifdef CROWDB_HAVE_LIBURING
TEST(PageWriteback, RealUringWriteAndColdLoadCrossExtentBoundary)
{
    crowdb::tree_test::TempDir      dir("writeback_uring_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    auto store = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    ASSERT_TRUE(store->write_completion_is_reloadable());
    TestWriter test;
    ASSERT_TRUE(PageWriteOperation::create(
                    store, test.runtime, test.lane, write_input(),
                    {.max_frame_bytes = 4096, .compression = compress_algo::kNone}, test.credits.credits,
                    [owner = test.extent](uint64_t length, PageWriteOperation::ExtentComplete done) {
                        ReservedWriteExtent extent{.backend_id       = 1,
                                                   .address          = 4096,
                                                   .length           = length,
                                                   .content_id       = 3,
                                                   .generation       = 1,
                                                   .owner            = owner,
                                                   .physical_metrics = {},
                                                   .chunk            = {}};
                        done(Status::Ok(), std::move(extent));
                    },
                    &test.operation)
                    .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    auto backing = test.operation->result().backing;
    ASSERT_TRUE(backing);
    EXPECT_EQ(backing->length, 8192U);
    AsyncStorePageLoader            loader(store, test.runtime, test.lane, {}, {}, test.credits.credits);
    std::atomic<bool>               done{false};
    std::shared_ptr<const LeafBase> loaded;
    loader.load_leaf({.identity = 7, .backing = backing}, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        loaded = std::move(leaf);
        done.store(true, std::memory_order_release);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(done.load(std::memory_order_acquire));
    ASSERT_TRUE(loaded);
    EXPECT_EQ(CellView(loaded->cell(0)).value().to_string(), "value");
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(PageWriteback, CrossExtentInlineFailureCompletesOnceAfterAllSubmissions)
{
    crowdb::tree_test::TempDir      dir("writeback_inline_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    // An empty topology deterministically rejects every chunk inline.
    crowdb::common::DiskIOUring uring({});
    BlockAsyncPageStore         store(file.get(), &uring);
    std::vector<uint8_t>        buffer(3 * 4096, 0);
    size_t                      terminals = 0;
    store.submit_write(0, buffer.data(), buffer.size(), detail::own_async_completion([&](Status status) {
                           ++terminals;
                           EXPECT_EQ(status.code(), Code::kIoError);
                       }));
    EXPECT_EQ(terminals, 1U);
    terminals = 0;
    store.submit_read(0, buffer.data(), buffer.size(), detail::own_async_completion([&](Status status) {
                          ++terminals;
                          EXPECT_EQ(status.code(), Code::kIoError);
                      }));
    EXPECT_EQ(terminals, 1U);
}

TEST(PageWriteback, FsyncReleasesItsOwnedContextAfterInlineFailure)
{
    crowdb::tree_test::TempDir      dir("writeback_fsync_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    ASSERT_TRUE(file->ensure_extents(0, 3 * 4096).ok());
    crowdb::common::DiskIOUring uring({});
    BlockAsyncPageStore         store(file.get(), &uring);
    auto                        calls    = std::make_shared<size_t>(0);
    std::weak_ptr<size_t>       retained = calls;
    ASSERT_TRUE(store
                    .submit_fsync({.context = calls.get(),
                                   .complete_fn =
                                       [](void *context, Status status) {
                                           ++*static_cast<size_t *>(context);
                                           EXPECT_EQ(status.code(), Code::kIoError);
                                       },
                                   .owner = calls})
                    .ok());
    EXPECT_EQ(*calls, 1U);
    ASSERT_TRUE(store
                    .submit_fsync({.context = calls.get(),
                                   .complete_fn =
                                       [](void *context, Status status) {
                                           ++*static_cast<size_t *>(context);
                                           EXPECT_EQ(status.code(), Code::kIoError);
                                       },
                                   .owner = calls})
                    .ok());
    EXPECT_EQ(*calls, 2U);
    calls.reset();
    EXPECT_TRUE(retained.expired());
}

TEST(PageWriteback, MissingLaterReadExtentDrainsEarlierIoBeforeReportingFailure)
{
    crowdb::tree_test::TempDir      dir("writeback_partial_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 4096, 4096, &file).ok());
    std::vector<uint8_t> input(4096, 42);
    ASSERT_TRUE(file->write_at(0, input.data(), input.size()).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    BlockAsyncPageStore  store(file.get(), &uring);
    std::vector<uint8_t> output(8192, 0);
    // The second extent was never written. Absence must agree with the
    // synchronous store while accepted I/O for the first extent still drains.
    ASSERT_EQ(file->read_at(0, output.data(), output.size()).code(), Code::kNotFound);
    std::fill(output.begin(), output.end(), 0);
    std::atomic<size_t> calls{0};
    store.submit_read(0, output.data(), output.size(), detail::own_async_completion([&](Status status) {
                          EXPECT_EQ(status.code(), Code::kNotFound);
                          EXPECT_TRUE(std::equal(input.begin(), input.end(), output.begin()));
                          calls.fetch_add(1, std::memory_order_release);
                      }));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (calls.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(calls.load(std::memory_order_acquire), 1U);
}

TEST(BlockAsyncPageStore, UnwrittenAndShortExtentsMatchSynchronousNotFound)
{
    crowdb::tree_test::TempDir      dir("readback_short_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 2, 8192, 4096, &file).ok());
    std::vector<uint8_t> buffer(8192);
    ASSERT_TRUE(file->write_at(0, buffer.data(), 4096).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    BlockAsyncPageStore store(file.get(), &uring);
    for (const size_t offset : {0U, 4096U, 8192U, 16384U}) {
        const auto bytes = offset == 0 ? buffer.size() : size_t{4096};
        ASSERT_EQ(file->read_at(offset, buffer.data(), bytes).code(), Code::kNotFound);
        std::promise<Status> completed;
        auto                 result = completed.get_future();
        store.submit_read(offset, buffer.data(), bytes,
                          detail::own_async_completion([&](Status status) { completed.set_value(std::move(status)); }));
        EXPECT_EQ(result.get().code(), Code::kNotFound) << "read offset " << offset;
    }
}
#endif
} // namespace crowdb::tree
