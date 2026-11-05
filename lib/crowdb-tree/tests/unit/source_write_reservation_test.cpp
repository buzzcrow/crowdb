// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
constexpr auto   kWriter      = detail::BudgetClass::kAdmittedWriter;
constexpr size_t kWriterIndex = static_cast<size_t>(kWriter);

struct TestAdmissionBudget
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] SourceWriteOptions options() const
    {
        return {
            .account = account, .max_entries = 1, .max_key_bytes = 128, .max_gap_entries = 1, .max_gap_key_bytes = 128};
    }

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> handoff() const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 128 * 1024);
        if (!credit) {
            throw std::bad_alloc();
        }
        return detail::AllocationReservation::create(std::move(*credit));
    }

    [[nodiscard]] size_t writer_usage() const
    {
        auto snapshot = budget.snapshot();
        return snapshot.allocated[kWriterIndex] + snapshot.reserved[kWriterIndex];
    }
};

TEST(SourceWriteReservation, CancellationReturnsCreditWithoutEnteringASource)
{
    TestAdmissionBudget budget;
    const auto          baseline = budget.writer_usage();
    {
        SourceWriteReservation reservation;
        ASSERT_TRUE(SourceWriteReservation::create(budget.options(), {1, 1, 0}, kWriter, &reservation).ok());
        EXPECT_GT(budget.writer_usage(), baseline);
        SourceWriteReservation moved(std::move(reservation));
        EXPECT_FALSE(reservation.valid());
        EXPECT_TRUE(moved.valid());
    }
    EXPECT_EQ(budget.writer_usage(), baseline);
}

TEST(SourceWriteReservation, BoundPermitDoesNotReserveTheSameMemoryTwice)
{
    TestAdmissionBudget    budget;
    SourceAdmission        sources(0, 1, budget.options());
    SourceWriteReservation reservation;
    ASSERT_TRUE(SourceWriteReservation::create(budget.options(), {1, 1, 0}, kWriter, &reservation).ok());
    const auto admitted = budget.writer_usage();
    auto       blocker  = detail::ResourceBudget::try_reserve(budget.account, kWriter,
                                                              TestAdmissionBudget::limits()[kWriterIndex] - admitted);
    ASSERT_TRUE(blocker);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit_reserved(0, &reservation, &writer).ok());
    EXPECT_FALSE(reservation.valid());
    EXPECT_EQ(budget.writer_usage(), TestAdmissionBudget::limits()[kWriterIndex]);
    ASSERT_TRUE(writer.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    writer.release();
    EXPECT_EQ(sources.try_admit_reserved(0, &reservation, &writer).code(), Code::kInvalidArgument);
}

TEST(SourceWriteReservation, SourceRotationDoesNotWaitForConsensusReservation)
{
    TestAdmissionBudget     budget;
    SourceAdmission         sources(0, 1, budget.options());
    SourceAdmission::Writer first;
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &first).ok());
    ASSERT_TRUE(first.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "old")).ok());
    first.release();
    SourceWriteReservation reservation;
    ASSERT_TRUE(SourceWriteReservation::create(budget.options(), {1, 1, 0}, kWriter, &reservation).ok());
    SourceAdmission::Writer next;
    EXPECT_EQ(sources.try_admit_reserved(1, &reservation, &next).code(), Code::kUnavailable);
    EXPECT_TRUE(reservation.valid());
    std::atomic<uint64_t>          frontier{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(sources.try_capture(frontier, budget.handoff(), &capture).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    EXPECT_TRUE(capture->drained());
    ASSERT_TRUE(sources.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(sources.try_admit_reserved(1, &reservation, &next).ok());
    ASSERT_TRUE(next.upsert("b", 2, encode_cell_buf(2, OpKind::kPut, "new")).ok());
    EXPECT_EQ(next.table(), capture->successor());
    EXPECT_EQ(capture->frontier(), 1U);
}

TEST(SourceWriteReservation, ForeignAccountIsRejectedWithoutConsumingReservation)
{
    TestAdmissionBudget    first;
    TestAdmissionBudget    second;
    SourceAdmission        sources(0, 1, second.options());
    SourceWriteReservation reservation;
    ASSERT_TRUE(SourceWriteReservation::create(first.options(), {1, 1, 0}, kWriter, &reservation).ok());
    SourceAdmission::Writer writer;
    EXPECT_EQ(sources.try_admit_reserved(0, &reservation, &writer).code(), Code::kInvalidArgument);
    EXPECT_TRUE(reservation.valid());
    EXPECT_FALSE(writer.table());
}

TEST(SourceWriteReservation, GapReservationRemainsIndependentOfOrdinaryAdmission)
{
    TestAdmissionBudget     budget;
    SourceAdmission         sources(0, 1, budget.options());
    SourceAdmission::Writer first;
    ASSERT_TRUE(sources.try_admit(0, {1, 1, 0}, &first).ok());
    ASSERT_TRUE(first.upsert("k", 2, encode_cell_buf(2, OpKind::kPut, "new")).ok());
    first.release();
    auto blocker = detail::ResourceBudget::try_reserve(
        budget.account, kWriter, TestAdmissionBudget::limits()[kWriterIndex] - budget.writer_usage());
    ASSERT_TRUE(blocker);
    SourceWriteReservation gap;
    ASSERT_TRUE(
        SourceWriteReservation::create(budget.options(), {1, 1, 0}, detail::BudgetClass::kGapRepair, &gap).ok());
    SourceAdmission::Writer repair;
    ASSERT_TRUE(sources.try_admit_reserved(0, &gap, &repair).ok());
    ASSERT_TRUE(repair.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "old")).ok());
}
} // namespace
} // namespace crowdb::tree
