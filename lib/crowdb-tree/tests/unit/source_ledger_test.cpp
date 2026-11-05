// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_state.h"
#include "btree/source_admission.h"

#include <gtest/gtest.h>

#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestSourceCredit
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 128 * 1024) const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return std::make_shared<detail::AllocationReservation>(std::move(*credit));
    }
};

TEST(SourceLedger, ActiveAndClosedWritersFreezeExactlyOnceThenDetachWhileReaderRetainsTable)
{
    auto ledger = std::make_shared<SourceMemoryLedger>();
    auto table  = std::make_shared<MemTable>();
    table->attach_source_stats(ledger);
    EXPECT_EQ(ledger->snapshot(), table->source_stats());
    ASSERT_TRUE(table->try_enter());
    ASSERT_TRUE(table->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "one")));
    ASSERT_TRUE(table->upsert("a", 2, encode_cell_buf(2, OpKind::kPut, "two")));
    EXPECT_EQ(ledger->snapshot(), table->source_stats());
    table->close();
    table->close();
    EXPECT_EQ(ledger->snapshot().freezing_writers, 1U);
    EXPECT_EQ(ledger->snapshot().frozen_retained_bytes, 0U);
    ASSERT_TRUE(table->upsert("b", 3, encode_cell_buf(3, OpKind::kPut, "three")));
    table->leave();
    EXPECT_EQ(ledger->snapshot(), table->source_stats());
    EXPECT_GT(ledger->snapshot().frozen_retained_bytes, 0U);
    table->detach_source_stats();
    EXPECT_EQ(ledger->snapshot(), (SourceMemoryStats{.known = true}));
    auto guard = table->read_guard();
    ASSERT_NE(table->find("a"), nullptr);
    EXPECT_EQ(table->find("a")->slot, 2U);
    guard = {};
    table.reset();
    EXPECT_EQ(ledger->snapshot(), (SourceMemoryStats{.known = true}));
}

TEST(SourceLedger, SuccessorAndCoveredMembershipFollowAdmissionRatherThanOldCatalogOwners)
{
    TestSourceCredit        credits;
    SourceAdmission         sources;
    auto                    old    = sources.catalog();
    auto                    ledger = old->memory;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "one")));
    writer.release();
    std::atomic<uint64_t>          frontier{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(sources.try_capture(frontier, credits.reserve(), &capture).ok());
    EXPECT_EQ(ledger->snapshot().tables, 1U);
    EXPECT_EQ(ledger->snapshot().frozen_tables, 1U);
    ASSERT_TRUE(sources.activate_successor(capture, capture->batch_id()).ok());
    EXPECT_EQ(ledger->snapshot().tables, 2U);
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()));
    ASSERT_TRUE(sources.finish_covered(capture, capture->batch_id(), 1).ok());
    EXPECT_EQ(ledger->snapshot().tables, 1U);
    EXPECT_EQ(ledger->snapshot().records, 0U);
    EXPECT_EQ(ledger->snapshot().memory, VersionMemory{});
    EXPECT_EQ(old->active->count(), 1U);
    EXPECT_EQ(old->memory, ledger);
}

TEST(SourceLedger, FailedCaptureDoesNotPublishCandidateAndOldGenerationWriterCannotTouchNewLedger)
{
    TestSourceCredit        credits;
    auto                    old = std::make_unique<SourceAdmission>();
    SourceAdmission         next(0, 2);
    const auto              old_ledger  = old->catalog()->memory;
    const auto              next_ledger = next.catalog()->memory;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(old->try_admit(0, &writer).ok());
    std::atomic<uint64_t>          frontier{1};
    std::shared_ptr<SourceCapture> rejected;
    EXPECT_EQ(old->try_capture(frontier, credits.reserve(1), &rejected).code(), Code::kResourceExhausted);
    EXPECT_EQ(old_ledger->snapshot().tables, 1U);
    EXPECT_EQ(old_ledger->snapshot().frozen_tables, 0U);
    old.reset();
    ASSERT_TRUE(writer.table()->upsert("late", 1, encode_cell_buf(1, OpKind::kPut, "old")));
    EXPECT_EQ(old_ledger->snapshot().records, 1U);
    EXPECT_EQ(next_ledger->snapshot().records, 0U);
    writer.release();
    EXPECT_EQ(old_ledger->snapshot(), (SourceMemoryStats{.known = true}));
    EXPECT_EQ(next_ledger->snapshot().tables, 1U);
}

TEST(SourceLedger, DetachCanPrecedeTheLastWritersBookkeepingWithoutLeakingTheLedger)
{
    auto     ledger = std::make_shared<SourceMemoryLedger>();
    MemTable table;
    table.attach_source_stats(ledger);
    ASSERT_TRUE(table.try_enter());
    ASSERT_TRUE(table.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "one")));
    table.close();

    struct Pause
    {
        std::latch entered{1};
        std::latch proceed{1};
    } pause;

    table.set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
        if (point == ConcurrentSkipList::PausePoint::kBeforeLeaveAccounting) {
            auto &pause = *static_cast<Pause *>(context);
            pause.entered.count_down();
            pause.proceed.wait();
        }
    });
    std::thread writer([&] { table.leave(); });
    pause.entered.wait();
    std::atomic<bool>      notified{false};
    MemTable::FrozenWaiter waiter(
        [](void *context) noexcept { static_cast<std::atomic<bool> *>(context)->store(true); }, &notified);
    ASSERT_TRUE(table.notify_when_frozen(waiter));
    EXPECT_TRUE(notified.load());
    table.detach_source_stats();
    pause.proceed.count_down();
    writer.join();
    table.set_hook_for_tests(nullptr, nullptr);
    EXPECT_EQ(ledger->snapshot(), (SourceMemoryStats{.known = true}));
}

TEST(SourceLedger, LateCloseDeltaAndEarlyFreezeRemovalConvergeWithoutRebindingAWriter)
{
    auto     ledger = std::make_shared<SourceMemoryLedger>();
    MemTable table;
    table.attach_source_stats(ledger);
    ASSERT_TRUE(table.try_enter());

    struct Pause
    {
        std::latch entered{1};
        std::latch proceed{1};
    } pause;

    table.set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
        if (point == ConcurrentSkipList::PausePoint::kBeforeCloseAccounting) {
            auto &pause = *static_cast<Pause *>(context);
            pause.entered.count_down();
            pause.proceed.wait();
        }
    });
    std::thread close([&] { table.close(); });
    pause.entered.wait();
    table.leave();
    table.detach_source_stats();
    pause.proceed.count_down();
    close.join();
    table.set_hook_for_tests(nullptr, nullptr);
    EXPECT_EQ(ledger->snapshot(), (SourceMemoryStats{.known = true}));
}

TEST(SourceLedger, PublishedInheritedSummaryIsFixedWhenParentsAdvanceTheirCatalog)
{
    auto parent = std::make_shared<MemTable>();
    ASSERT_TRUE(parent->upsert("parent", 1, encode_cell_buf(1, OpKind::kPut, "v")));
    parent->close();
    auto     local = std::make_shared<SourceMemoryLedger>();
    MemTable child;
    child.attach_source_stats(local);
    PublishedSources sources;
    sources.memory      = local;
    sources.inheritance = PublishedSources::Inheritance{
        .owner_identity = 5, .session = 1, .frontier = 1, .local_count = 1, .memory = parent->source_stats()};
    parent.reset();
    EXPECT_TRUE(sources.source_stats().known);
    EXPECT_EQ(sources.source_stats().tables, 2U);
    EXPECT_EQ(sources.source_stats().frozen_tables, 1U);
    EXPECT_EQ(sources.source_stats().records, 1U);
    ASSERT_TRUE(child.try_enter());
    ASSERT_TRUE(child.upsert("child", 2, encode_cell_buf(2, OpKind::kPut, "v")));
    child.leave();
    EXPECT_EQ(sources.source_stats().records, 2U);
    EXPECT_EQ(sources.inheritance->memory.records, 1U);
}
} // namespace
} // namespace crowdb::tree
