// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestPartitionBudget
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    SourceWriteOptions                               options{
        .account = account, .max_entries = 2, .max_key_bytes = 128, .max_gap_entries = 1, .max_gap_key_bytes = 32};
};

TEST(SourceWritePartition, ChosenGapUsesItsPrepaidMemoryWithoutOrdinarySourceCapacity)
{
    constexpr auto         ordinary = detail::BudgetClass::kAdmittedWriter;
    constexpr auto         gap      = detail::BudgetClass::kGapRepair;
    TestPartitionBudget    budget;
    SourceAdmission        sources(0, 1, budget.options);
    SourceWriteReservation client;
    ASSERT_TRUE(SourceWriteReservation::create(budget.options, {1, 1, 0}, ordinary, &client).ok());
    SourceAdmission::Writer ahead;
    ASSERT_TRUE(sources.try_admit(0, {2, 2, 0}, &ahead).ok());
    ASSERT_TRUE(ahead.upsert("a", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
    ASSERT_TRUE(ahead.upsert("b", 3, encode_cell_buf(3, OpKind::kPut, "v")).ok());
    ahead.release();
    const auto              before = budget.budget.snapshot();
    SourceAdmission::Writer repair;
    EXPECT_EQ(sources.try_admit_reserved(0, &client, &repair).code(), Code::kUnavailable);
    EXPECT_TRUE(client.valid());
    ASSERT_TRUE(sources.try_admit_reserved(0, &client, &repair, gap).ok());
    EXPECT_EQ(repair.allocation_budget()->category(), ordinary);
    const auto after = budget.budget.snapshot();
    for (const auto category : {ordinary, gap}) {
        const auto index = static_cast<size_t>(category);
        EXPECT_EQ(after.reserved[index] + after.allocated[index], before.reserved[index] + before.allocated[index]);
    }
    ASSERT_TRUE(repair.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "old")).ok());
    repair.release();
    EXPECT_FALSE(client.valid());
}

TEST(SourceWritePartition, AbandoningReclassifiedWriterReturnsOnlyGapCounters)
{
    constexpr auto         ordinary = detail::BudgetClass::kAdmittedWriter;
    constexpr auto         gap      = detail::BudgetClass::kGapRepair;
    TestPartitionBudget    budget;
    SourceAdmission        sources(0, 1, budget.options);
    SourceWriteReservation first;
    ASSERT_TRUE(SourceWriteReservation::create(budget.options, {1, 1, 0}, ordinary, &first).ok());
    SourceAdmission::Writer abandoned;
    ASSERT_TRUE(sources.try_admit_reserved(0, &first, &abandoned, gap).ok());
    abandoned.release();
    SourceWriteReservation second;
    ASSERT_TRUE(SourceWriteReservation::create(budget.options, {1, 1, 0}, ordinary, &second).ok());
    SourceAdmission::Writer replacement;
    ASSERT_TRUE(sources.try_admit_reserved(0, &second, &replacement, gap).ok());
    ASSERT_TRUE(replacement.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    replacement.release();
    SourceAdmission::Writer full_ordinary;
    EXPECT_TRUE(sources.try_admit(1, {2, 2, 0}, &full_ordinary).ok());
}

TEST(SourceWritePartition, ImpossibleGapDoesNotConsumeTheOriginalReservation)
{
    TestPartitionBudget    budget;
    SourceAdmission        sources(0, 1, budget.options);
    SourceWriteReservation client;
    ASSERT_TRUE(
        SourceWriteReservation::create(budget.options, {2, 2, 0}, detail::BudgetClass::kAdmittedWriter, &client).ok());
    SourceAdmission::Writer writer;
    EXPECT_EQ(sources.try_admit_reserved(0, &client, &writer, detail::BudgetClass::kGapRepair).code(),
              Code::kResourceExhausted);
    EXPECT_TRUE(client.valid());
    EXPECT_EQ(sources.try_admit_reserved(0, &client, &writer, detail::BudgetClass::kRouting).code(),
              Code::kInvalidArgument);
    EXPECT_TRUE(client.valid());
    EXPECT_TRUE(sources.try_admit_reserved(0, &client, &writer).ok());
}
} // namespace
} // namespace crowdb::tree
