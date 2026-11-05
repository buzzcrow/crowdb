// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_checkpoint_test_support.h"
#include "test_tmp.h"

namespace crowdb::tree
{
using namespace test_write;

#ifdef CROWDB_HAVE_LIBURING
TEST(LocalCheckpoint, RealBlockReopenUsesOnlyAnchorAndDurableMapping)
{
    crowdb::tree_test::TempDir      dir("local_checkpoint_");
    std::unique_ptr<BlockPageStore> file;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 3, 4096, 4096, &file).ok());
    crowdb::common::Topology topology;
    topology.pipelines.emplace_back().entries = 64;
    crowdb::common::DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    auto              store = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    TestCheckpoint    test;
    uint64_t          next = 1;
    const std::string value(12000, 'r');
    ASSERT_TRUE(
        CapturedWriteOperation::create(
            captured_leaf(page({
                {.key = "a", .value = value}
    })),
            {}, store, test.source.runtime, test.source.lane,
            {.frames = {.encode = {.frame_bytes = 4096, .inline_value_bytes = 16}}, .write = {.max_frame_bytes = 4096}},
            test.source.credits.credits,
            [owner = test.source.ledger, &next](uint64_t count, ReservedOverflowIds *out) {
                *out = {.first = next, .count = count, .owner = owner};
                next += count;
                return Status::Ok();
            },
            extent_write_reservation(test.source.ledger, test.source.credits.credits.bytes), {}, &test.source.operation)
            .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.source.operation->sender()));
    test.source.runtime->drain();
    ASSERT_TRUE(LocalCheckpointOperation::create(test.source.operation->result().pages, test.registry, store,
                                                 test.source.runtime, test.source.lane, {}, test.source.credits.credits,
                                                 &test.operation)
                    .ok());
    ASSERT_TRUE(stdexec::sync_wait(test.operation->sender()));
    test.source.runtime->drain();
    EXPECT_EQ(test.registry->durable_coverage(), 10U);
    test.operation.reset();
    test.source.operation.reset();
    test.registry.reset();
    test.source.ledger.reset();
    store.reset();
    file.reset();
    test.source.credits.seal();
    EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);

    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 3, 4096, 4096, &file).ok());
    Config config;
    config.page_store  = file.get();
    config.frame_bytes = 4096;
    std::unique_ptr<Crowdbtree> tree;
    const auto                  opened = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    std::string restored;
    uint64_t    slot = 0;
    ASSERT_TRUE(tree->get(Slice("a"), &slot, &restored));
    EXPECT_EQ(restored, value);
    EXPECT_EQ(slot, 1U);
}
#endif

TEST(LocalCheckpoint, ClosedRuntimeWaitsForAcceptedIoBeforeReleasingTheCommitGate)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    test.store->defer_at = 1;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.source.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    test.source.runtime->close();
    test.operation->request_stop();
    EXPECT_EQ(done.errors.load() + done.stopped.load(), 0U);
    test.store->finish();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.registry->needs_recovery());
    EXPECT_EQ(test.registry->snapshot_sequence(), 0U);
    EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}
} // namespace crowdb::tree
