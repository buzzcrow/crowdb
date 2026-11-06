// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_recovery_test_support.h"
#include "test_tmp.h"

#include <future>

namespace crowdb::tree
{
using namespace test_write;

#ifdef CROWDB_HAVE_LIBURING
namespace
{
Status await_status(detail::CallbackSender sender)
{
    try {
        return stdexec::sync_wait(sender) ? Status::Ok() : Status::unavailable();
    }
    catch (const Status &status) {
        return status;
    }
}
} // namespace

TEST(LocalRecovery, ReopenedBlockFilesRebuildProtectedPrimaryAndOverflowOwners)
{
    crowdb::tree_test::TempDir      dir("local_recovery_");
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
            extent_write_reservation(test.source.ledger, test.source.credits.credits.reservation()), {},
            &test.source.operation)
            .ok());
    auto status = await_status(test.source.operation->sender());
    ASSERT_TRUE(status.ok()) << "captured write: " << status.to_string();
    test.source.runtime->drain();
    ASSERT_TRUE(LocalCheckpointOperation::create(test.source.operation->result().pages, test.registry, store,
                                                 test.source.runtime, test.source.lane, {}, test.source.credits.credits,
                                                 &test.operation)
                    .ok());
    status = await_status(test.operation->sender());
    ASSERT_TRUE(status.ok()) << "checkpoint commit: " << status.to_string();
    test.source.runtime->drain();
    test.operation.reset();
    test.source.operation.reset();
    test.registry.reset();
    test.source.ledger.reset();
    store.reset();
    file.reset();
    test.source.credits.seal();
    EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);

    ASSERT_TRUE(BlockPageStore::open_blocks(dir.path, 1, 3, 4096, 4096, &file).ok());
    store = std::make_shared<BlockAsyncPageStore>(file.get(), &uring);
    LoadCredits                             credits;
    std::shared_ptr<LocalRecoveryOperation> recovery;
    ASSERT_TRUE(LocalRecoveryOperation::create(store, test.source.runtime, test.source.lane,
                                               {.backend_id = 9, .generation = 2, .file_size = file->size()}, {},
                                               credits.credits, &recovery)
                    .ok());
    status = await_status(recovery->sender());
    ASSERT_TRUE(status.ok()) << "local recovery: " << status.to_string();
    test.source.runtime->drain();
    auto pages  = recovery->result().latest;
    auto ledger = recovery->result().ledger;
    ASSERT_TRUE(pages);
    EXPECT_EQ(recovery->result().registry->durable_coverage(), 10U);
    EXPECT_EQ(ledger->stats().protected_ranges, pages->frames.size() + 2);
    recovery.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, pages->frames.size());
    auto loader = std::make_shared<AsyncStorePageLoader>(
        store, test.source.runtime, test.source.lane,
        PageLoadLimits{.max_inflight = 1, .max_blob_bytes = 4096, .max_frame_bytes = 4096},
        [pages](uint64_t pid, std::shared_ptr<const BackingRef> *out) {
            const auto frame = pages->frames.find(pid);
            if (frame == pages->frames.end()) {
                return Status::not_found();
            }
            *out = frame->second.backing;
            return Status::Ok();
        },
        credits.credits);
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
    test.source.runtime->drain();
    leaf.reset();
    restored.reset();
    pages.reset();
    loader.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 0U);
    ledger.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
#endif
} // namespace crowdb::tree
