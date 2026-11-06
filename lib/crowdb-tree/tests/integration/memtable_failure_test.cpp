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
    if (point == Point::kBeforePrepare && key == Slice("b")) {
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

TEST(MemTableFailure, PreparationFailurePublishesNoKeysAndDoesNotCreditItsSlot)
{
    for (int variant = 0; variant < 3; ++variant) {
        Crowdbtree tree;
        ASSERT_TRUE(tree.apply(1, values("old")).ok());
        auto table = Access::active(tree);
        table->set_hook_for_tests(nullptr, fail_second_key);
        EXPECT_EQ(apply_variant(tree, variant, 2).code(), Code::kResourceExhausted);
        EXPECT_EQ(table->writers(), 0U);
        EXPECT_EQ(tree.contiguous_slot(), 1U);
        EXPECT_EQ(tree.get_view("a").slot(), 1U);
        EXPECT_EQ(tree.get_view("b").slot(), 1U);
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

TEST(MemTableFailure, FailureBeforeSecondPreparedKeyReturnsItsPermitForRetry)
{
    Crowdbtree tree;
    auto       table = Access::active(tree);
    table->set_hook_for_tests(nullptr, fail_second_key);
    EXPECT_EQ(tree.apply(1, values("unpublished")).code(), Code::kResourceExhausted);
    EXPECT_EQ(table->writers(), 0U);
    EXPECT_EQ(table->count(), 0U);
    EXPECT_EQ(tree.contiguous_slot(), 0U);
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree.apply(1, values("retry")).ok());
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.get_view("a").value().to_string(), "retry");
    EXPECT_EQ(tree.get_view("b").value().to_string(), "retry");
    EXPECT_EQ(tree.last_applied_slot(), 1U);
}

TEST(MemTableFailure, FailedPublicationPreservesAtomicOldSnapshotAndRepairableSources)
{
    MemPageStore store;
    Config       options;
    options.page_store = &store;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, values("old")).ok());
    ASSERT_TRUE(tree.flush().ok());
    auto snapshot = tree.snapshot_view();
    ASSERT_TRUE(tree.apply(2, values("new")).ok());
    auto &publisher = RuntimeTreeAccess_for_tests::core(tree).publisher();
    publisher.set_before_root_for_tests([] { throw std::bad_alloc(); });
    EXPECT_FALSE(tree.flush().ok());
    EXPECT_EQ(tree.last_applied_slot(), 1U);
    EXPECT_EQ(tree.get_view("b").value().to_string(), "new");
    uint64_t    slot = 0;
    std::string value;
    ASSERT_TRUE(snapshot->get("b", &slot, &value));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(value, "old");
    publisher.set_before_root_for_tests({});
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(tree.snapshot().ok());
    EXPECT_EQ(tree.last_applied_slot(), 2U);
    EXPECT_EQ(tree.get_view("b").value().to_string(), "new");
    ASSERT_TRUE(snapshot->get("b", &slot, &value));
    EXPECT_EQ(value, "old");
}

TEST(MemTableFailure, ReplacementDrainsOldBatchesBeforeInstallingNewGeneration)
{
    Crowdbtree         tree;
    auto               table = Access::active(tree);
    std::promise<void> entered;
    std::promise<void> release;
    auto               resume = release.get_future();

    struct Pause
    {
        std::promise<void> &entered;
        std::future<void>  &resume;
    } pause{.entered = entered, .resume = resume};

    table->set_hook_for_tests(&pause, [](void *argument, Point point, Slice) {
        if (point == Point::kBeforePrepare) {
            auto &pause = *static_cast<Pause *>(argument);
            pause.entered.set_value();
            pause.resume.wait();
        }
    });
    auto old = std::async(std::launch::async, [&] { return tree.put("old", "old"); });
    entered.get_future().wait();
    auto reset = std::async(std::launch::async, [&] { return tree.clear(); });
    EXPECT_EQ(reset.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(old.get().ok());
    EXPECT_TRUE(reset.get().ok());
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree.apply(1, values("new")).ok());
    EXPECT_FALSE(tree.get_view("old").found());
    EXPECT_EQ(tree.get_view("a").value().to_string(), "new");
    EXPECT_EQ(tree.contiguous_slot(), 1U);
}

TEST(MemTableFailure, LogicalReplacementDoesNotFreeBorrowedPayload)
{
    GetView borrowed;
    {
        Crowdbtree tree;
        ASSERT_TRUE(tree.apply(1, values("one")).ok());
        borrowed = tree.get_view("a");
        ASSERT_TRUE(tree.apply(2, values("two")).ok());
        ASSERT_TRUE(tree.apply(2, values("two")).ok());
        ASSERT_TRUE(tree.flush().ok());
        RuntimeTreeAccess_for_tests::reclaim(tree);
        EXPECT_EQ(borrowed.value().to_string(), "one");
        EXPECT_EQ(tree.get_view("a").value().to_string(), "two");
    }
    EXPECT_EQ(borrowed.value().to_string(), "one");
}

TEST(MemTableFailure, AbortedPreparationDoesNotConsumeAutomaticSlot)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.batch_put(values("old")).ok());
    auto table = Access::active(tree);
    table->set_hook_for_tests(nullptr, fail_second_key);
    EXPECT_EQ(tree.batch_put(values("failed")).code(), Code::kResourceExhausted);
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree.batch_put(values("next")).ok());
    EXPECT_EQ(tree.get_view("a").slot(), 2U);
    EXPECT_EQ(tree.get_view("b").slot(), 2U);
    EXPECT_EQ(tree.contiguous_slot(), 2U);
}
#endif
