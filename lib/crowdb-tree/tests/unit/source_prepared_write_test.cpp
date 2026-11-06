// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"

#include <gtest/gtest.h>

#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestBudget
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] SourceWriteOptions options(size_t entries = 4) const
    {
        return {.account           = account,
                .max_entries       = entries,
                .max_key_bytes     = 1024,
                .max_gap_entries   = 0,
                .max_gap_key_bytes = 0};
    }
};

TEST(SourcePreparedWrite, AbandonedBatchReturnsAllSourceCapacityWithoutChangingKeys)
{
    TestBudget      budget;
    SourceAdmission sources(0, 1, budget.options(2));
    {
        SourceAdmission::Writer writer;
        ASSERT_TRUE(sources.try_admit(0, {.entries = 2, .key_bytes = 2, .payload_bytes = 0}, &writer).ok());
        SourceAdmission::Writer::Prepared first;
        SourceAdmission::Writer::Prepared rejected;
        ASSERT_TRUE(writer.prepare_write("a", 0, encode_cell_buf(0, OpKind::kPut, "one"), &first).ok());
        EXPECT_EQ(writer.prepare_write("too-long", 0, encode_cell_buf(0, OpKind::kPut, "two"), &rejected).code(),
                  Code::kInvalidArgument);
        EXPECT_TRUE(writer.table()->empty());
        EXPECT_EQ(writer.upsert("b", 1, encode_cell_buf(1, OpKind::kPut, "legacy")).code(), Code::kInvalidArgument);
    }
    SourceAdmission::Writer retry;
    ASSERT_TRUE(sources.try_admit(0, {.entries = 2, .key_bytes = 2, .payload_bytes = 0}, &retry).ok());
    SourceAdmission::Writer::Prepared first;
    SourceAdmission::Writer::Prepared second;
    ASSERT_TRUE(retry.prepare_write("a", 0, encode_cell_buf(0, OpKind::kPut, "one"), &first).ok());
    ASSERT_TRUE(retry.prepare_write("b", 0, encode_cell_buf(0, OpKind::kPut, "two"), &second).ok());
    retry.allocation_budget()->seal();
    EXPECT_TRUE(retry.commit_write(std::move(first), 1));
    EXPECT_TRUE(retry.commit_write(std::move(second), 1));
    EXPECT_EQ(retry.table()->count(), 2U);
    EXPECT_EQ(retry.table()->find("a")->slot, 1U);
    EXPECT_EQ(CellView(retry.table()->find("b")->cell.slice()).slot(), 1U);
}

TEST(SourcePreparedWrite, NewKeyPreparationCanLoseInsertionWithoutAnotherAllocation)
{
    TestBudget              budget;
    SourceAdmission         sources(0, 1, budget.options(2));
    SourceAdmission::Writer older;
    SourceAdmission::Writer newer;
    ASSERT_TRUE(sources.try_admit(0, {.entries = 1, .key_bytes = 1, .payload_bytes = 0}, &older).ok());
    ASSERT_TRUE(sources.try_admit(0, {.entries = 1, .key_bytes = 1, .payload_bytes = 0}, &newer).ok());
    SourceAdmission::Writer::Prepared first;
    SourceAdmission::Writer::Prepared second;
    ASSERT_TRUE(older.prepare_write("k", 0, encode_cell_buf(0, OpKind::kPut, "old"), &first).ok());
    ASSERT_TRUE(newer.prepare_write("k", 0, encode_cell_buf(0, OpKind::kPut, "new"), &second).ok());
    older.allocation_budget()->seal();
    newer.allocation_budget()->seal();
    EXPECT_TRUE(newer.commit_write(std::move(second), 2));
    EXPECT_TRUE(older.commit_write(std::move(first), 1));
    auto table = older.table();
    EXPECT_EQ(table->count(), 1U);
    EXPECT_EQ(table->find("k")->slot, 2U);
    EXPECT_EQ(table->prefix_cursor(1).cell_version()->slot, 1U);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(SourcePreparedWrite, SealedCommitRetriesVersionCasWithTheOriginalCapacity)
{
    TestBudget              budget;
    SourceAdmission         sources(0, 1, budget.options(3));
    SourceAdmission::Writer initial;
    ASSERT_TRUE(sources.try_admit(0, {.entries = 1, .key_bytes = 1, .payload_bytes = 0}, &initial).ok());
    ASSERT_TRUE(initial.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "one")).ok());
    auto table = initial.table();
    initial.release();

    struct Pause
    {
        std::atomic<bool>     once{true};
        std::atomic<bool>     prepared{false};
        std::binary_semaphore entered{0};
        std::binary_semaphore resume{0};
    } pause;

    table->set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
        auto &state = *static_cast<Pause *>(context);
        if (point == ConcurrentSkipList::PausePoint::kBeforeVersionCas && state.once.exchange(false)) {
            state.entered.release();
            state.resume.acquire();
        }
    });
    std::jthread old([&] {
        SourceAdmission::Writer           writer;
        SourceAdmission::Writer::Prepared prepared;
        auto status = sources.try_admit(0, {.entries = 1, .key_bytes = 1, .payload_bytes = 0}, &writer);
        if (status.ok()) {
            status = writer.prepare_write("k", 0, encode_cell_buf(0, OpKind::kPut, "two"), &prepared);
        }
        EXPECT_TRUE(status.ok()) << status.to_string();
        if (!status.ok()) {
            pause.entered.release();
            return;
        }
        pause.prepared.store(true);
        writer.allocation_budget()->seal();
        EXPECT_TRUE(writer.commit_write(std::move(prepared), 2));
    });
    pause.entered.acquire();
    if (!pause.prepared.load()) {
        return;
    }
    const auto resume = [](std::binary_semaphore *value) { value->release(); };
    std::unique_ptr<std::binary_semaphore, decltype(resume)> release(&pause.resume, resume);
    SourceAdmission::Writer                                  newer;
    ASSERT_TRUE(sources.try_admit(0, {.entries = 1, .key_bytes = 1, .payload_bytes = 0}, &newer).ok());
    SourceAdmission::Writer::Prepared prepared;
    ASSERT_TRUE(newer.prepare_write("k", 0, encode_cell_buf(0, OpKind::kPut, "three"), &prepared).ok());
    newer.allocation_budget()->seal();
    EXPECT_TRUE(newer.commit_write(std::move(prepared), 3));
    release.reset();
    old.join();
    table->set_hook_for_tests(nullptr, nullptr);
    EXPECT_EQ(table->find("k")->slot, 3U);
    EXPECT_EQ(table->prefix_cursor(2).cell_version()->slot, 2U);
    EXPECT_EQ(table->prefix_cursor(1).cell_version()->slot, 1U);
    EXPECT_EQ(table->memory().history_count, 2U);
}
#endif
} // namespace
} // namespace crowdb::tree
