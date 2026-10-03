// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "memtable_access.h"

#include <gtest/gtest.h>

#include <future>
#include <thread>

#ifdef CROWDB_TREE_TEST_UTIL
using namespace crowdb::tree;

namespace
{
using Access = MemTableAccess_for_tests;
using Point  = ConcurrentSkipList::PausePoint;

Batch values(const std::string &value)
{
    return Batch{
        {{.key = "a", .kind = OpKind::kPut, .value = value}, {.key = "b", .kind = OpKind::kPut, .value = value}}
    };
}

void fail_second_key(void * /*context*/, Point point, Slice key)
{
    if (point == Point::kBeforeVersionCas && key == Slice("b")) {
        throw std::bad_alloc();
    }
}

Status apply_variant(Crowdbtree &tree, int variant, uint64_t slot)
{
    if (variant == 0) {
        return tree.apply(slot, values("new"));
    }
    if (variant == 1) {
        std::vector<Crowdbtree::encoded_op> ops;
        for (const auto *key : {"a", "b"}) {
            ops.push_back({.key = key, .cell = encode_cell_buf(slot, OpKind::kPut, "new")});
        }
        return tree.apply_encoded(slot, std::move(ops));
    }
    std::vector<Crowdbtree::external_op> ops;
    for (const auto *key : {"a", "b"}) {
        ops.push_back({.key = key, .flags = 0, .value = buffer::copy_of("new")});
    }
    return tree.apply_external(slot, std::move(ops));
}
} // namespace

TEST(MemTableFailure, PartialApplyReleasesAdmissionWithoutCreditingItsSlot)
{
    for (int variant = 0; variant < 3; ++variant) {
        Crowdbtree tree;
        ASSERT_TRUE(tree.apply(1, values("old")).ok());
        auto table = Access::active(tree);
        table->set_hook_for_tests(nullptr, fail_second_key);
        EXPECT_EQ(apply_variant(tree, variant, 2).code(), Code::kResourceExhausted);
        EXPECT_EQ(table->writers(), 0U);
        EXPECT_EQ(tree.contiguous_slot(), 1U);
        EXPECT_EQ(tree.get_view("a").slot(), 2U);
        EXPECT_EQ(tree.get_view("b").slot(), 1U);
        EXPECT_EQ(Access::counters(tree).failed_batches.load(), 1U);
        table->set_hook_for_tests(nullptr, nullptr);
        ASSERT_TRUE(tree.flush().ok());
        uint64_t    slot = 0;
        std::string value;
        ASSERT_TRUE(tree.snapshot_view()->get("a", &slot, &value));
        EXPECT_EQ(slot, 1U);
        EXPECT_EQ(value, "old");
        ASSERT_TRUE(apply_variant(tree, variant, 2).ok());
        ASSERT_TRUE(tree.flush().ok());
        EXPECT_EQ(tree.last_applied_slot(), 2U);
        EXPECT_EQ(tree.get_view("a").value().to_string(), "new");
        EXPECT_EQ(tree.get_view("b").value().to_string(), "new");
    }
}

TEST(MemTableFailure, OptionalPruningFailureKeepsSuccessfulCompletion)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.apply(1, Batch{{{"a", OpKind::kPut, "old"}}}).ok());
    auto table = Access::active(tree);
    int  calls = 0;
    table->set_hook_for_tests(&calls, [](void *context, Point point, Slice) {
        if (point == Point::kBeforeVersionCas && ++*static_cast<int *>(context) == 2) {
            throw std::bad_alloc();
        }
    });
    ASSERT_TRUE(tree.apply(2, Batch{{{"a", OpKind::kPut, "new"}}}).ok());
    EXPECT_EQ(tree.contiguous_slot(), 2U);
    EXPECT_EQ(table->writers(), 0U);
    EXPECT_EQ(table->memory().history_count, 1U);
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree.apply(3, Batch{{{"a", OpKind::kPut, "next"}}}).ok());
    EXPECT_EQ(table->memory().history_count, 0U);
}

TEST(MemTableFailure, IncompleteL1PublicationBlocksExportsUntilRetry)
{
    MemPageStore store;
    Config       options;
    options.page_store = &store;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, values("old")).ok());
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.apply(2, values("new")).ok());
    Access::fail_publication(tree, [] { throw std::bad_alloc(); });
    EXPECT_FALSE(tree.flush().ok());
    EXPECT_EQ(tree.last_applied_slot(), 1U);
    EXPECT_EQ(tree.get_view("b").value().to_string(), "new");
    EXPECT_FALSE(tree.snapshot().ok());
    EXPECT_THROW((void)tree.snapshot_view(), std::runtime_error);
    std::unique_ptr<SnapshotExport> portable;
    EXPECT_FALSE(snapshot_export_begin(tree, snapshot_format::kPortable, 4096, &portable).ok());
    EXPECT_EQ(portable, nullptr);
    std::vector<NativeFrame> frames;
    EXPECT_FALSE(tree.collect_native_frames(&frames, nullptr, nullptr).ok());
    std::unique_ptr<NativeFrameIterator> iterator;
    EXPECT_FALSE(tree.open_native_frame_iterator(nullptr, &iterator).ok());
    Access::fail_publication(tree, nullptr);
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.snapshot().ok());
    EXPECT_EQ(tree.last_applied_slot(), 2U);
}

TEST(MemTableFailure, ReplacementDrainsOldBatchesBeforeAdmittingNewGeneration)
{
    Crowdbtree         tree;
    std::promise<void> entered;
    std::promise<void> release;
    auto               resume = release.get_future();
    auto               old    = std::async(std::launch::async, [&] {
        auto batch = Access::admit(tree);
        entered.set_value();
        resume.wait();
        batch.table->upsert("old", 1, encode_cell_buf(1, OpKind::kPut, "old"), batch.bound);
        Access::finish(tree, 1);
    });
    entered.get_future().wait();
    auto       reset    = std::async(std::launch::async, [&] { return tree.clear(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!Access::replacing(tree) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(Access::replacing(tree));
    auto next = std::async(std::launch::async, [&] { return tree.apply(1, values("new")); });
    EXPECT_EQ(next.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    release.set_value();
    old.get();
    EXPECT_TRUE(reset.get().ok());
    EXPECT_TRUE(next.get().ok());
    EXPECT_FALSE(tree.get_view("old").found());
    EXPECT_EQ(tree.get_view("a").value().to_string(), "new");
    EXPECT_EQ(tree.contiguous_slot(), 1U);
}

TEST(MemTableFailure, MetricsSeparateLogicalMergeFromDelayedPhysicalReclamation)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.apply(1, values("one")).ok());
    auto borrowed = tree.get_view("a");
    ASSERT_TRUE(tree.apply(2, values("two")).ok());
    ASSERT_TRUE(tree.apply(2, values("replay")).ok());
    auto &metrics = Access::counters(tree);
    EXPECT_EQ(metrics.overwrite.load(), 2U);
    EXPECT_EQ(metrics.keep.load(), 2U);
    EXPECT_EQ(metrics.merged.load(), 2U);
    EXPECT_EQ(metrics.keep_pending.load(), 2U);
    EXPECT_EQ(Access::active(tree)->memory().history_count, 0U);
    auto      &epoch    = Access::epoch(tree);
    const auto resident = epoch.memtable_allocation()->load();
    epoch.try_reclaim();
    EXPECT_EQ(epoch.memtable_allocation()->load(), resident);
    EXPECT_EQ(borrowed.value().to_string(), "one");
    borrowed = {};
    epoch.try_reclaim();
    EXPECT_LT(epoch.memtable_allocation()->load(), resident);
}

TEST(MemTableFailure, DescriptorCopyLatencyIncludesAbortedPreparation)
{
    Crowdbtree tree;
    tree.init_metrics("copy-test", "memory");
    auto *latency = Access::copy_latency(tree);
    ASSERT_NE(latency, nullptr);
    ASSERT_TRUE(tree.apply(1, values("old")).ok());
    auto first = latency->flush();
    // Each key's completion prune reconstructs its existing descriptor.
    EXPECT_EQ(first.count, 2U);
    auto table = Access::active(tree);
    table->set_hook_for_tests(nullptr, fail_second_key);
    EXPECT_EQ(tree.apply(2, values("new")).code(), Code::kResourceExhausted);
    table->set_hook_for_tests(nullptr, nullptr);
    auto failed = latency->flush();
    // Both candidates copied current + anchor; only the first CAS published.
    EXPECT_EQ(failed.count, 2U);
    EXPECT_GE(failed.sum, failed.max);
    EXPECT_EQ(failed.total_count, first.count + failed.count);
}
#endif
