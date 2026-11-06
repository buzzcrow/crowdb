// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_state.h"
#include "btree/source_admission.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestRotationCredit
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits out;
        out.fill(1024 * 1024);
        return out;
    }

    detail::ResourceBudget                           budget{limits(), 2};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes) const
    {
        auto credit = detail::ResourceBudget::try_reserve(
            account, detail::BudgetClass::kHandoff,
            bytes + detail::shared_allocation_bound<detail::AllocationReservation>());
        return credit ? detail::AllocationReservation::create(std::move(*credit)) : nullptr;
    }
};

TEST(SourceRotation, PreparesBeforeClosureAndKeepsAdmittedOldWriterReadable)
{
    TestRotationCredit credits;
    SourceAdmission    source;
    const auto         old   = source.catalog();
    auto               bound = SourceAdmission::rotation_allocation_bound(1);
    ASSERT_TRUE(bound);
    auto credit = credits.reserve(bound.value_or(0));
    ASSERT_TRUE(credit);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(source.try_admit(0, &writer).ok());
    std::shared_ptr<SourceAdmission::Rotation> rotation;
    ASSERT_TRUE(source.prepare_rotation(credit, old, &rotation).ok());
    EXPECT_EQ(source.catalog(), old);
    EXPECT_FALSE(old->active->closed());
    ASSERT_EQ(rotation->next()->tables.size(), 2U);
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    EXPECT_EQ(source.try_capture(contiguous, credit, &capture).code(), Code::kUnavailable);
    credit->seal();
    ASSERT_TRUE(source.activate_rotation(rotation).ok());
    EXPECT_TRUE(old->active->closed());
    ASSERT_TRUE(writer.upsert("old", 1, encode_cell_buf(1, OpKind::kPut, "value")).ok());
    auto        guard = old->active->read_guard();
    const auto *value = old->active->find("old");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->slot, 1U);
    SourceAdmission::Writer next;
    ASSERT_TRUE(source.try_admit(1, &next).ok());
    EXPECT_NE(next.table(), writer.table());
    source.close();
    EXPECT_TRUE(source.activate_rotation(rotation).ok());
}

TEST(SourceRotation, AbandonmentAndInsufficientCreditLeaveOriginalSourceWritable)
{
    TestRotationCredit                         credits;
    SourceAdmission                            source;
    const auto                                 old = source.catalog();
    std::shared_ptr<SourceAdmission::Rotation> rotation;
    auto                                       tiny = credits.reserve(1);
    EXPECT_EQ(source.prepare_rotation(tiny, old, &rotation).code(), Code::kResourceExhausted);
    EXPECT_FALSE(old->active->closed());
    auto bound = SourceAdmission::rotation_allocation_bound(1);
    ASSERT_TRUE(bound);
    auto credit = credits.reserve(bound.value_or(0));
    ASSERT_TRUE(source.prepare_rotation(credit, old, &rotation).ok());
    ASSERT_TRUE(source.abort_rotation(rotation).ok());
    EXPECT_EQ(source.catalog(), old);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(source.try_admit(0, &writer).ok());
    EXPECT_EQ(writer.table(), old->active);
}
} // namespace
} // namespace crowdb::tree
