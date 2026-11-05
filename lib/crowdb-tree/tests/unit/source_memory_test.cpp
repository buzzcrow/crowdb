// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

#include <gtest/gtest.h>

#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<CellVersion> value(uint64_t slot, Slice bytes)
{
    return std::make_shared<CellVersion>(encode_cell_buf(slot, OpKind::kPut, bytes), slot, 0);
}

void expect_same(const VersionMemory &actual, const VersionMemory &expected)
{
    EXPECT_EQ(actual.history_count, expected.history_count);
    EXPECT_EQ(actual.history_bytes, expected.history_bytes);
    EXPECT_EQ(actual.descriptor_bytes, expected.descriptor_bytes);
    EXPECT_EQ(actual.node_bytes, expected.node_bytes);
    EXPECT_EQ(actual.payload_bytes, expected.payload_bytes);
}

TEST(SourceMemory, LegacyInsertOverwriteAndPruneMatchIndependentLinkedVersionScan)
{
    ConcurrentSkipList source;
    expect_same(source.memory(), {});
    ASSERT_TRUE(source.upsert("a", value(1, "one"), 0));
    ASSERT_TRUE(source.upsert("a", value(2, "two-two"), 0));
    ASSERT_TRUE(source.upsert("b", value(4, "four"), 0));
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_count, 1U);
    ASSERT_TRUE(source.upsert("a", value(3, "three"), 2));
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_bytes, encode_cell_buf(2, OpKind::kPut, "two-two").size());
    source.prune("a", 3);
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_count, 0U);
    const auto before = source.memory();
    EXPECT_FALSE(source.upsert("a", value(3, "ignored"), 3));
    expect_same(source.memory(), before);
}

TEST(SourceMemory, PreparedInsertionLoserAndReplacementUsePublishedCapacities)
{
    ConcurrentSkipList source;
    auto               older = source.prepare_write("a", value(0, "one"), 0, 4, {});
    auto               newer = source.prepare_write("a", value(0, "longer-two"), 0, 4, {});
    EXPECT_TRUE(source.commit_write(std::move(newer), 2));
    EXPECT_TRUE(source.commit_write(std::move(older), 1));
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_count, 1U);
    {
        auto abandoned = source.prepare_write("b", value(0, "unused"), 0, 16, {});
        expect_same(source.memory(), source.scan_memory_for_tests());
    }
    expect_same(source.memory(), source.scan_memory_for_tests());
}

TEST(SourceMemory, LaterReplaceAccountsBeforePreparedInsertionWithoutCountingItTwice)
{
    ConcurrentSkipList source;

    struct Pause
    {
        std::latch published{1};
        std::latch proceed{1};
    } pause;

    source.set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
        if (point == ConcurrentSkipList::PausePoint::kBeforeInsertAccounting) {
            auto &pause = *static_cast<Pause *>(context);
            pause.published.count_down();
            pause.proceed.wait();
        }
    });
    std::thread insertion([&] {
        auto prepared = source.prepare_write("a", value(0, "longer-initial-value"), 0, 4, {});
        EXPECT_TRUE(source.commit_write(std::move(prepared), 1));
    });
    pause.published.wait();
    EXPECT_TRUE(source.upsert("a", value(2, "two"), 2));
    EXPECT_LE(source.memory().payload_bytes, sizeof(CellVersion) + encode_cell_buf(2, OpKind::kPut, "two").size());
    pause.proceed.count_down();
    insertion.join();
    source.set_hook_for_tests(nullptr, nullptr);
    EXPECT_EQ(source.approx_bytes(), 1 + encode_cell_buf(2, OpKind::kPut, "two").size());
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_count, 0U);
}

TEST(SourceMemory, LosingVersionCasCannotDoubleCountAbandonedDescriptor)
{
    ConcurrentSkipList source;
    ASSERT_TRUE(source.upsert("a", value(1, "one"), 0));

    struct Pause
    {
        std::atomic<bool> once{true};
        std::latch        entered{1};
        std::latch        proceed{1};
    } pause;

    source.set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
        auto &pause = *static_cast<Pause *>(context);
        if (point == ConcurrentSkipList::PausePoint::kBeforeVersionCas && pause.once.exchange(false)) {
            pause.entered.count_down();
            pause.proceed.wait();
        }
    });
    MutationStats retries;
    std::thread   older([&] { EXPECT_TRUE(source.upsert("a", value(2, "two"), 0, &retries)); });
    pause.entered.wait();
    EXPECT_TRUE(source.upsert("a", value(3, "three-three"), 0));
    pause.proceed.count_down();
    older.join();
    source.set_hook_for_tests(nullptr, nullptr);
    EXPECT_GT(retries.cas_retries, 0U);
    expect_same(source.memory(), source.scan_memory_for_tests());
    EXPECT_EQ(source.memory().history_count, 2U);
}
} // namespace
} // namespace crowdb::tree
