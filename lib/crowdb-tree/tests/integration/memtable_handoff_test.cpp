// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "memtable_access.h"

#include <gtest/gtest.h>

#include <future>
#include <thread>

using namespace crowdb::tree;

#ifdef CROWDB_TREE_TEST_UTIL

namespace
{
Batch put(std::string key, std::string value)
{
    return Batch{{{.key = std::move(key), .kind = OpKind::kPut, .value = std::move(value)}}};
}

void expect_value(Crowdbtree &tree, uint64_t expected, const std::string &value)
{
    auto found = tree.get_view("x");
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.slot(), expected);
    EXPECT_EQ(found.value().to_string(), value);
}
} // namespace

TEST(MemTableHandoff, AcknowledgedResidualNeverRelocatesOrLeaksIntoL1)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.apply(843, put("x", "cursor23039")).ok());
    tree.force_advance_slot(843);
    ASSERT_TRUE(tree.flush().ok());
    tree.force_advance_slot(852);
    auto boundary = [&] {
        auto batch = MemTableAccess_for_tests::admit(tree);
        batch.table->upsert("x", 853, encode_cell_buf(853, OpKind::kPut, "cursor24040"), batch.bound);
        auto captured = MemTableAccess_for_tests::capture(tree);
        MemTableAccess_for_tests::finish(tree, 853);
        return captured;
    }();
    auto old_sources = MemTableAccess_for_tests::sources(tree);
    auto borrowed    = tree.get_view("x");
    ASSERT_EQ(boundary.frontier, 852U);
    expect_value(tree, 853, "cursor24040");
    MemTableAccess_for_tests::publish(tree, boundary);
    expect_value(tree, 853, "cursor24040");
    EXPECT_EQ(tree.last_applied_slot(), 852U);
    ASSERT_NE(old_sources.front()->find("x"), nullptr);
    EXPECT_EQ(old_sources.front()->find("x")->slot, 853U);
    auto        snapshot       = tree.snapshot_view();
    uint64_t    published_slot = 0;
    std::string published_value;
    ASSERT_TRUE(snapshot->get("x", &published_slot, &published_value));
    EXPECT_EQ(published_slot, 843U);
    EXPECT_EQ(published_value, "cursor23039");
    EXPECT_EQ(borrowed.value().to_string(), "cursor24040");
    tree.force_advance_slot(853);
    ASSERT_TRUE(tree.flush().ok());
    expect_value(tree, 853, "cursor24040");
    EXPECT_EQ(tree.memtable_count(), 0U);
    EXPECT_EQ(borrowed.value().to_string(), "cursor24040");
}

TEST(MemTableHandoff, CapturedPrefixSurvivesOverwriteInEitherOrder)
{
    for (bool reverse : {false, true}) {
        Crowdbtree tree;
        tree.force_advance_slot(1);
        ASSERT_TRUE(tree.flush().ok());
        for (auto slot : (reverse ? std::vector<uint64_t>{102, 2} : std::vector<uint64_t>{2, 102})) {
            ASSERT_TRUE(tree.apply(slot, put("x", std::to_string(slot))).ok());
        }
        tree.force_advance_slot(100);
        auto boundary = MemTableAccess_for_tests::capture(tree);
        ASSERT_EQ(boundary.frontier, 100U);
        auto prefix = boundary.tables.back()->prefix_cursor(100);
        ASSERT_TRUE(prefix.valid());
        EXPECT_EQ(prefix.cell_version()->slot, 2U);
        MemTableAccess_for_tests::publish(tree, boundary);
        EXPECT_EQ(tree.last_applied_slot(), 100U);
        expect_value(tree, 102, "102");
        ASSERT_TRUE(tree.apply(101, {}).ok());
        ASSERT_TRUE(tree.flush().ok());
        EXPECT_EQ(tree.last_applied_slot(), 102U);
        expect_value(tree, 102, "102");
    }
}

TEST(MemTableHandoff, EmptyAdmittedWriterSurvivesCloseAndSuccessorProgresses)
{
    Crowdbtree         tree;
    std::promise<void> admitted;
    std::promise<void> resume;
    auto               continuation = resume.get_future();
    std::thread        writer([&] {
        auto batch = MemTableAccess_for_tests::admit(tree);
        admitted.set_value();
        continuation.wait();
        batch.table->upsert("x", 1, encode_cell_buf(1, OpKind::kPut, "old-writer"), batch.bound);
        MemTableAccess_for_tests::finish(tree, 1);
    });
    admitted.get_future().wait();
    auto boundary = MemTableAccess_for_tests::capture(tree);
    EXPECT_EQ(boundary.tables.back()->writers(), 1U);
    EXPECT_TRUE(boundary.tables.back()->closed());
    EXPECT_FALSE(boundary.tables.back()->try_enter());
    EXPECT_FALSE(boundary.tables.back()->validate_open());
    ASSERT_TRUE(tree.apply(2, put("b", "successor")).ok());
    resume.set_value();
    writer.join();
    MemTableAccess_for_tests::publish(tree, boundary);
    EXPECT_EQ(tree.last_applied_slot(), 0U);
    expect_value(tree, 1, "old-writer");
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.last_applied_slot(), 2U);
    expect_value(tree, 1, "old-writer");
}

TEST(MemTableHandoff, SuccessorCompletionCannotWidenCapturedFrontier)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.apply(100, put("x", "100")).ok());
    tree.force_advance_slot(100);
    auto boundary = MemTableAccess_for_tests::capture(tree);
    ASSERT_TRUE(tree.apply(101, put("x", "101")).ok());
    MemTableAccess_for_tests::publish(tree, boundary);
    EXPECT_EQ(tree.last_applied_slot(), 100U);
    expect_value(tree, 101, "101");
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.last_applied_slot(), 101U);
}

TEST(MemTableHandoff, BorrowedResultOutlivesItsTreeAndGeneration)
{
    GetView borrowed;
    {
        Crowdbtree tree;
        ASSERT_TRUE(tree.apply(1, put("x", "borrowed")).ok());
        borrowed = tree.get_view("x");
        ASSERT_TRUE(tree.clear().ok());
        EXPECT_FALSE(tree.get_view("x").found());
        EXPECT_EQ(borrowed.value().to_string(), "borrowed");
    }
    EXPECT_EQ(borrowed.value().to_string(), "borrowed");
}

TEST(MemTableHandoff, AsyncFlushCanWaitWithoutBlockingTheCaller)
{
    Crowdbtree         tree;
    std::promise<void> entered;
    std::promise<void> release;
    auto               proceed = release.get_future();
    std::thread        writer([&] {
        auto batch = MemTableAccess_for_tests::admit(tree);
        entered.set_value();
        proceed.wait();
        batch.table->upsert("x", 1, encode_cell_buf(1, OpKind::kPut, "late"), batch.bound);
        MemTableAccess_for_tests::finish(tree, 1);
    });
    entered.get_future().wait();
    std::promise<Status> completion;
    auto                 done = completion.get_future();
    tree.flush_async([&](Status status) { completion.set_value(std::move(status)); });
    // Reaching this line must not depend on releasing the old writer.
    release.set_value();
    writer.join();
    EXPECT_TRUE(done.get().ok());
    expect_value(tree, 1, "late");
}

TEST(MemTableHandoff, EncodedMismatchCannotCreditUnpublishedFutureSlot)
{
    Crowdbtree                          tree;
    std::vector<Crowdbtree::encoded_op> operations;
    operations.push_back({.key = "x", .cell = encode_cell_buf(102, OpKind::kPut, "future")});
    EXPECT_FALSE(tree.apply_encoded(1, std::move(operations)).ok());
    EXPECT_EQ(tree.contiguous_slot(), 0U);
    EXPECT_FALSE(tree.get_view("x").found());
}

TEST(MemTableHandoff, RejectedImportPreservesVisibleGeneration)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.apply(1, put("x", "old")).ok());
    std::vector<leaf_entry> imported;
    imported.push_back({.key = "x", .cell = encode_cell_buf(102, OpKind::kPut, "future")});
    EXPECT_FALSE(tree.install_snapshot(std::move(imported), 100).ok());
    expect_value(tree, 1, "old");
    EXPECT_EQ(tree.contiguous_slot(), 1U);
}

TEST(MemTableHandoff, OverlayJournalCutoverDoesNotCreditL1Coverage)
{
    Crowdbtree source;
    Crowdbtree destination;
    ASSERT_TRUE(source.apply(1, put("x", "inherited")).ok());
    ASSERT_TRUE(destination.install_split_memtable_overlay(source, 1).ok());
    EXPECT_EQ(destination.last_applied_slot(), 0U);
    EXPECT_FALSE(destination.flush().ok());
    EXPECT_FALSE(destination.clear_split_memtable_overlay(source).ok());
    expect_value(destination, 1, "inherited");
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(source.begin_split_memtable_view(&generation, &frontier).ok());
    ASSERT_TRUE(source.flush().ok());
    // Source L1 publication is not destination L1 coverage.
    expect_value(destination, 1, "inherited");
    EXPECT_FALSE(source.publish_split_memtable_view(generation, frontier + 1, destination, KeyRange::unbounded()).ok());
    ASSERT_TRUE(source.publish_split_memtable_view(generation, frontier, destination, KeyRange::unbounded()).ok());
    ASSERT_TRUE(destination.clear_split_memtable_overlay(source).ok());
    EXPECT_EQ(destination.last_applied_slot(), 1U);
    expect_value(destination, 1, "inherited");
}

#endif
