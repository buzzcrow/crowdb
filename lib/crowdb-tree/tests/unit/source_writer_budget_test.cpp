// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"

#include <gtest/gtest.h>

#include <barrier>
#include <thread>

namespace crowdb::tree
{
namespace
{
constexpr size_t kWriter = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);

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

    [[nodiscard]] SourceWriteOptions options(size_t entries = 8) const
    {
        return {.account           = account,
                .max_entries       = entries,
                .max_key_bytes     = 1024,
                .max_gap_entries   = 2,
                .max_gap_key_bytes = 128};
    }

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> lifecycle() const
    {
        const auto bytes = SourceAdmission::allocation_bound();
        if (!bytes) {
            throw std::bad_alloc();
        }
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kAdmittedWriter, *bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return detail::AllocationReservation::create(std::move(*credit));
    }

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> handoff() const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 128 * 1024);
        if (!credit) {
            throw std::bad_alloc();
        }
        return std::make_shared<detail::AllocationReservation>(std::move(*credit));
    }
};

TEST(SourceWriterBudget, PartialCapacityRejectionRequestsOrdinaryRolloverButGapPressureDoesNot)
{
    TestBudget budget;
    for (bool key_limited : {false, true}) {
        auto options          = budget.options(key_limited ? 8 : 5);
        options.max_key_bytes = key_limited ? 128 : 1024;
        SourceWriteBudget                              source(options);
        std::shared_ptr<detail::AllocationReservation> first;
        const WriteFootprint                           footprint{.entries = 3, .key_bytes = key_limited ? 96U : 3U};
        ASSERT_TRUE(source.admit(footprint, detail::BudgetClass::kAdmittedWriter, &first).ok());
        EXPECT_FALSE(source.full());
        std::shared_ptr<detail::AllocationReservation> second;
        EXPECT_EQ(source.admit(footprint, detail::BudgetClass::kAdmittedWriter, &second).code(), Code::kUnavailable);
        EXPECT_TRUE(source.full());
    }
    SourceWriteBudget                              source(budget.options());
    std::shared_ptr<detail::AllocationReservation> first;
    ASSERT_TRUE(source.admit({.entries = 2, .key_bytes = 2}, detail::BudgetClass::kGapRepair, &first).ok());
    std::shared_ptr<detail::AllocationReservation> second;
    EXPECT_EQ(source.admit({.entries = 1, .key_bytes = 1}, detail::BudgetClass::kGapRepair, &second).code(),
              Code::kUnavailable);
    EXPECT_FALSE(source.full());
}

TEST(SourceWriterBudget, AdmittedWriterCompletesWhenUnreservedHostBudgetIsExhausted)
{
    TestBudget budget;
    {
        SourceAdmission         sources(0, 1, budget.options());
        SourceAdmission::Writer writer;
        ASSERT_TRUE(sources.try_admit(0, {4, 12, 4096}, &writer).ok());
        auto snapshot  = budget.budget.snapshot();
        auto remaining = TestBudget::limits()[kWriter] - snapshot.allocated[kWriter] - snapshot.reserved[kWriter];
        auto blocker =
            detail::ResourceBudget::try_reserve(budget.account, detail::BudgetClass::kAdmittedWriter, remaining);
        ASSERT_TRUE(blocker);
        SourceAdmission::Writer rejected;
        EXPECT_EQ(sources.try_admit(0, {1, 3, 0}, &rejected).code(), Code::kUnavailable);
        for (uint64_t slot = 1; slot <= 4; ++slot) {
            bool changed = false;
            ASSERT_TRUE(writer.upsert("key", slot, encode_cell_buf(slot, OpKind::kPut, "v"), &changed).ok());
            EXPECT_TRUE(changed);
        }
        writer.release();
        auto view  = sources.catalog();
        auto guard = view->active->read_guard();
        ASSERT_NE(view->active->find("key"), nullptr);
        EXPECT_EQ(view->active->find("key")->slot, 4U);
    }
    EXPECT_EQ(budget.budget.snapshot().allocated[kWriter], 0U);
    EXPECT_EQ(budget.budget.snapshot().reserved[kWriter], 0U);
}

TEST(SourceWriterBudget, FootprintViolationCannotModifySourceAndUnusedPermitCanBeReadmitted)
{
    TestBudget              budget;
    SourceAdmission         sources(0, 1, budget.options(1));
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &writer).ok());
    EXPECT_EQ(writer.upsert("too long", 1, encode_cell_buf(1, OpKind::kPut, "v")).code(), Code::kInvalidArgument);
    EXPECT_EQ(writer.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, std::string(1024, 'v'))).code(),
              Code::kInvalidArgument);
    EXPECT_TRUE(writer.table()->empty());
    SourceAdmission::Writer moved = std::move(writer);
    writer                        = {};
    EXPECT_EQ(sources.try_admit(0, {1, 1, 0}, &writer).code(), Code::kUnavailable);
    moved.release();
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    EXPECT_EQ(writer.upsert("k", 2, encode_cell_buf(2, OpKind::kPut, "v")).code(), Code::kInvalidArgument);
}

TEST(SourceWriterBudget, GapRepairHasIndependentEntryAndMemoryAdmission)
{
    TestBudget              budget;
    SourceAdmission         sources(0, 1, budget.options(1));
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("k", 2, encode_cell_buf(2, OpKind::kPut, "new")).ok());
    writer.release();
    EXPECT_EQ(sources.try_admit(0, {1, 1, 0}, &writer).code(), Code::kUnavailable);
    auto snapshot = budget.budget.snapshot();
    auto rest     = TestBudget::limits()[kWriter] - snapshot.allocated[kWriter] - snapshot.reserved[kWriter];
    auto blocker  = detail::ResourceBudget::try_reserve(budget.account, detail::BudgetClass::kAdmittedWriter, rest);
    ASSERT_TRUE(blocker);
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &writer, detail::BudgetClass::kGapRepair).ok());
    ASSERT_TRUE(writer.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "old")).ok());
    auto table = writer.table();
    writer.release();
    auto guard  = table->read_guard();
    auto cursor = table->prefix_cursor(1);
    ASSERT_TRUE(cursor.valid());
    EXPECT_EQ(cursor.cell_owner()->slot, 1U);
}

TEST(SourceWriterBudget, SeparatePermitsDoNotReuseSealedCreditOfOverwrittenVersion)
{
    TestBudget budget;
    {
        auto            lifecycle = budget.lifecycle();
        SourceAdmission sources(0, 1, budget.options(16), lifecycle);
        auto            table       = sources.catalog()->active;
        auto            read_credit = budget.handoff();
        auto            guard       = table->read_guard(read_credit);
        read_credit->seal();
        const auto before_observer  = lifecycle->available();
        const auto before_allocated = budget.budget.snapshot().allocated[kWriter];
        {
            // Register this thread's prepaid observer participant once. Each
            // later write must reuse it without consuming lifecycle credit.
            auto observer = sources.tail_domain()->observe();
        }
        const auto lifecycle_reserved = lifecycle->available();
        const auto observer_bytes     = before_observer - lifecycle_reserved;
        EXPECT_GT(observer_bytes, 0U);
        EXPECT_LE(observer_bytes, EpochManager::participant_allocation_bytes());
        EXPECT_EQ(budget.budget.snapshot().allocated[kWriter] - before_allocated, observer_bytes);
        EXPECT_GT(lifecycle_reserved, 0U);
        for (uint64_t slot = 1; slot <= 16; ++slot) {
            SourceAdmission::Writer writer;
            ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &writer).ok());
            EXPECT_GT(budget.budget.snapshot().reserved[kWriter], lifecycle_reserved);
            ASSERT_TRUE(writer.upsert("k", slot, encode_cell_buf(slot, OpKind::kPut, "v")).ok());
            writer.release();
            EXPECT_EQ(lifecycle->available(), lifecycle_reserved);
            EXPECT_EQ(budget.budget.snapshot().reserved[kWriter], lifecycle_reserved);
        }
        EXPECT_EQ(table->find("k")->slot, 16U);
        EXPECT_EQ(table->memory().history_count, 15U);
    }
    EXPECT_EQ(budget.budget.snapshot().allocated[kWriter], 0U);
    EXPECT_EQ(budget.budget.snapshot().reserved[kWriter], 0U);
}

TEST(SourceWriterBudget, CapturedWriterRetainsPermitAndSuccessorRequiresItsOwn)
{
    TestBudget              budget;
    SourceAdmission         sources(0, 1, budget.options());
    SourceAdmission::Writer old;
    ASSERT_TRUE(sources.try_admit(0, {2, 2, 0}, &old).ok());
    ASSERT_TRUE(old.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    std::atomic<uint64_t>          frontier{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(sources.try_capture(frontier, budget.handoff(), &capture).ok());
    ASSERT_TRUE(old.table()->closed());
    ASSERT_TRUE(old.upsert("b", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
    ASSERT_TRUE(sources.activate_successor(capture, capture->batch_id()).ok());
    SourceAdmission::Writer next;
    EXPECT_EQ(sources.try_admit(1, &next).code(), Code::kInvalidArgument);
    ASSERT_TRUE(sources.try_admit(1, {1, 1, 0}, &next).ok());
    EXPECT_NE(old.table(), next.table());
    ASSERT_TRUE(next.upsert("c", 3, encode_cell_buf(3, OpKind::kPut, "v")).ok());
    EXPECT_EQ(capture->frontier(), 1U);
}

TEST(SourceWriterBudget, ConcurrentPermitsKeepAllVersionsWithinTheirOwnCredit)
{
    TestBudget budget;
    {
        auto                                  lifecycle = budget.lifecycle();
        SourceAdmission                       sources(0, 1, budget.options(16), lifecycle);
        auto                                  table = sources.catalog()->active;
        std::array<SourceWriteReservation, 8> reservations;
        for (auto &reservation : reservations) {
            ASSERT_TRUE(SourceWriteReservation::create(budget.options(16), {2, 2, 0},
                                                       detail::BudgetClass::kAdmittedWriter, &reservation)
                            .ok());
        }
        std::barrier              registered(9);
        std::barrier              start(9);
        std::vector<std::jthread> workers;
        workers.reserve(reservations.size());
        for (size_t index = 0; index < reservations.size(); ++index) {
            workers.emplace_back([&, index] {
                {
                    // Prepay lifecycle participants on their owning thread,
                    // separately from each transferred write reservation.
                    auto observer = sources.tail_domain()->observe();
                    (void)sources.catalog();
                }
                registered.arrive_and_wait();
                start.arrive_and_wait();
                SourceAdmission::Writer writer;
                ASSERT_TRUE(sources.try_admit_reserved(0, &reservations[index], &writer).ok());
                EXPECT_FALSE(reservations[index].valid());
                for (uint64_t slot = (2 * index) + 1; slot <= (2 * index) + 2; ++slot) {
                    auto status = writer.upsert("k", slot, encode_cell_buf(slot, OpKind::kPut, "v"));
                    EXPECT_TRUE(status.ok()) << status.to_string();
                }
                writer.release();
            });
        }
        registered.arrive_and_wait();
        const auto lifecycle_reserved = lifecycle->available();
        EXPECT_GT(lifecycle_reserved, 0U);
        EXPECT_GT(budget.budget.snapshot().reserved[kWriter], lifecycle_reserved);
        start.arrive_and_wait();
        workers.clear();
        auto read_credit = budget.handoff();
        auto guard       = table->read_guard(read_credit);
        read_credit->seal();
        EXPECT_EQ(table->find("k")->slot, 16U);
        EXPECT_EQ(table->memory().history_count, 15U);
        EXPECT_EQ(lifecycle->available(), lifecycle_reserved);
        EXPECT_EQ(budget.budget.snapshot().reserved[kWriter], lifecycle_reserved);
    }
    EXPECT_EQ(budget.budget.snapshot().allocated[kWriter], 0U);
    EXPECT_EQ(budget.budget.snapshot().reserved[kWriter], 0U);
}

TEST(SourceWriterBudget, LargestKeyBoundIsCheckedAtAdmissionAndBeforeMutation)
{
    TestBudget budget;
    auto       options   = budget.options();
    options.max_key_size = 4;
    SourceAdmission         sources(0, 1, options);
    SourceAdmission::Writer writer;
    EXPECT_EQ(sources.try_admit(0, {2, 8, 0}, &writer).code(), Code::kInvalidArgument);
    EXPECT_EQ(sources.try_admit(0, {2, 8, 0, 5}, &writer).code(), Code::kInvalidArgument);
    ASSERT_TRUE(sources.try_admit(0, {2, 8, 0, 4}, &writer).ok());
    EXPECT_EQ(writer.upsert("abcde", 1, encode_cell_buf(1, OpKind::kPut, "v")).code(), Code::kInvalidArgument);
    EXPECT_TRUE(writer.table()->empty());
    ASSERT_TRUE(writer.upsert("abcd", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    ASSERT_TRUE(writer.upsert("efgh", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
    EXPECT_EQ(writer.table()->count(), 2U);
}
} // namespace
} // namespace crowdb::tree
