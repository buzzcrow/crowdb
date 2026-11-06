// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "captured_write_test_support.h"
#include "test_tmp.h"

#include <future>

namespace crowdb::tree
{
using namespace test_write;

#ifdef CROWDB_HAVE_LIBURING
TEST(CapturedWrite, RealBlockReopenReloadsPrimaryAndOverflowThroughItsCapturedMapping)
{
    crowdb::tree_test::TempDir      dir("captured_write_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 3, 4096, 4096, &file).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    auto              store = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    TestCapturedWrite test;
    const std::string value(3000, 'b');
    uint64_t          next = 1;
    ASSERT_TRUE(
        CapturedWriteOperation::create(
            captured_leaf(page({
                {.key = "a", .value = value}
    })),
            {}, store, test.runtime, test.lane,
            {.frames = {.encode = {.frame_bytes = 512, .inline_value_bytes = 16}}, .write = {.max_frame_bytes = 512}},
            test.credits.credits,
            [owner = test.ledger, &next](uint64_t count, ReservedOverflowIds *out) {
                *out = {.first = next, .count = count, .owner = owner};
                next += count;
                return Status::Ok();
            },
            extent_write_reservation(test.ledger, test.credits.credits.reservation()), {}, &test.operation)
            .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.runtime->drain();
    auto pages = test.operation->result().pages;
    ASSERT_TRUE(pages);
    test.operation.reset();
    ASSERT_TRUE(file->sync().ok());
    store.reset();
    file.reset();
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 3, 4096, 4096, &file).ok());
    store       = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    auto loader = std::make_shared<AsyncStorePageLoader>(
        store, test.runtime, test.lane,
        PageLoadLimits{.max_inflight = 1, .max_blob_bytes = 4096, .max_frame_bytes = 512},
        [pages](uint64_t pid, std::shared_ptr<const BackingRef> *out) {
            auto frame = pages->frames.find(pid);
            if (frame == pages->frames.end()) {
                return Status::not_found();
            }
            *out = frame->second.backing;
            return Status::Ok();
        },
        test.credits.credits);
    std::promise<std::shared_ptr<const LeafBase>> leaf_result;
    loader->load_leaf({.identity = 1, .backing = pages->frames.at(0).backing}, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        leaf_result.set_value(std::move(leaf));
    });
    auto leaf = leaf_result.get_future().get();
    ASSERT_TRUE(leaf);
    const CellView                              cell(leaf->cell(0));
    std::promise<std::shared_ptr<const buffer>> value_result;
    loader->load_overflow(cell.overflow_head(), cell.overflow_len(), [&](Status status, auto restored) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        value_result.set_value(std::move(restored));
    });
    auto restored = value_result.get_future().get();
    ASSERT_TRUE(restored);
    EXPECT_EQ(Slice(restored->data(), restored->size()).to_string(), value);
    test.runtime->drain();
    leaf.reset();
    restored.reset();
    pages.reset();
    loader.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
#endif
} // namespace crowdb::tree
