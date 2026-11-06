// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"
#include "btree/source_pause.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestPausedSource
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(16 * 1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 8};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    SourceWriteOptions                               options{
        .account = account, .max_entries = 8, .max_key_bytes = 1024, .max_gap_entries = 2, .max_gap_key_bytes = 128};
    SourceAdmission source{0, 1, options};

    static void complete(void *context) noexcept
    {
        *static_cast<bool *>(context) = true;
    }
};

TEST(SourcePause, BoundIncludesUnappliedWritersAndGapRemainsIndependent)
{
    TestPausedSource        test;
    SourceAdmission::Writer admitted;
    ASSERT_TRUE(test.source.try_admit(0, {2, 12, 0}, &admitted).ok());
    const auto catalog = test.source.catalog();
    bool       paused  = false;
    ASSERT_TRUE(catalog->active_budget->pause(&TestPausedSource::complete, &paused));
    ASSERT_TRUE(paused);
    EXPECT_EQ(catalog->active->count(), 0U);
    const auto bound = catalog->active_budget->paused_bound();
    EXPECT_EQ(bound.entries, 4U);
    EXPECT_EQ(bound.key_bytes, 140U);
    SourceWriteReservation ordinary;
    ASSERT_TRUE(
        SourceWriteReservation::create(test.options, {1, 1, 0}, detail::BudgetClass::kAdmittedWriter, &ordinary).ok());
    SourceAdmission::Writer next;
    EXPECT_EQ(test.source.try_admit_reserved(0, &ordinary, &next).code(), Code::kUnavailable);
    EXPECT_TRUE(ordinary.valid());
    SourceAdmission::Writer gap;
    ASSERT_TRUE(test.source.try_admit(0, {1, 1, 0}, &gap, detail::BudgetClass::kGapRepair).ok());
    ASSERT_TRUE(gap.upsert("g", 1, encode_cell_buf(1, OpKind::kPut, "gap")).ok());
    ASSERT_TRUE(admitted.upsert("old", 2, encode_cell_buf(2, OpKind::kPut, "accepted")).ok());
    admitted.release();
    gap.release();
    catalog->active_budget->resume();
    ASSERT_TRUE(test.source.try_admit_reserved(2, &ordinary, &next).ok());
    EXPECT_FALSE(ordinary.valid());
}

TEST(SourcePause, FailedRotationRestoresOrdinaryAdmissionWithoutChangingMembership)
{
    TestPausedSource test;
    const auto       catalog = test.source.catalog();
    bool             paused  = false;
    ASSERT_TRUE(catalog->active_budget->pause(&TestPausedSource::complete, &paused));
    ASSERT_TRUE(paused);
    auto raw =
        detail::ResourceBudget::try_reserve(test.account, detail::BudgetClass::kHandoff,
                                            detail::shared_allocation_bound<detail::AllocationReservation>() + 1);
    if (!raw) {
        FAIL() << "missing pause credit";
    }
    auto                                       credit = detail::AllocationReservation::create(std::move(*raw));
    std::shared_ptr<SourceAdmission::Rotation> rotation;
    EXPECT_EQ(test.source.prepare_rotation(credit, catalog, &rotation).code(), Code::kResourceExhausted);
    EXPECT_FALSE(catalog->active->closed());
    EXPECT_EQ(test.source.catalog(), catalog);
    catalog->active_budget->resume();
    SourceAdmission::Writer writer;
    ASSERT_TRUE(test.source.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("k", 1, encode_cell_buf(1, OpKind::kPut, "value")).ok());
}

TEST(SourcePause, PlanOwnerKeepsPauseAndReleasesItOnAbandonment)
{
    TestPausedSource test;
    auto credit_raw = detail::ResourceBudget::try_reserve(test.account, detail::BudgetClass::kHandoff, 128 * 1024);
    if (!credit_raw) {
        FAIL() << "missing pause credit";
    }
    auto                         metadata = detail::AllocationReservation::create(std::move(*credit_raw));
    std::shared_ptr<SourcePause> pause;
    ASSERT_TRUE(SourcePause::create(*test.source.catalog(), metadata, {}, &pause).ok());
    ASSERT_TRUE(pause->ready());
    std::shared_ptr<SourcePause> competing;
    EXPECT_EQ(SourcePause::create(*test.source.catalog(), metadata, {}, &competing).code(), Code::kUnavailable);
    SourceWriteReservation permit;
    ASSERT_TRUE(
        SourceWriteReservation::create(test.options, {1, 1, 0}, detail::BudgetClass::kAdmittedWriter, &permit).ok());
    SourceAdmission::Writer writer;
    EXPECT_EQ(test.source.try_admit_reserved(0, &permit, &writer).code(), Code::kUnavailable);
    EXPECT_TRUE(permit.valid());
    auto retained_plan = pause;
    pause.reset();
    EXPECT_EQ(test.source.try_admit_reserved(0, &permit, &writer).code(), Code::kUnavailable);
    retained_plan.reset();
    ASSERT_TRUE(test.source.try_admit_reserved(0, &permit, &writer).ok());
}

TEST(SourcePause, CaptureClosureRejectsAnOldLateOrdinaryBindAfterPlanRelease)
{
    TestPausedSource test;
    const auto       old = test.source.catalog();
    ASSERT_TRUE(old->active->try_enter());
    auto raw = detail::ResourceBudget::try_reserve(test.account, detail::BudgetClass::kHandoff, 128 * 1024);
    if (!raw) {
        FAIL() << "missing pause credit";
    }
    auto                         metadata = detail::AllocationReservation::create(std::move(*raw));
    std::shared_ptr<SourcePause> pause;
    ASSERT_TRUE(SourcePause::create(*old, metadata, {}, &pause).ok());
    std::atomic<uint64_t>          frontier{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(test.source.try_capture(frontier, metadata, &capture, old).ok());
    ASSERT_TRUE(test.source.activate_successor(capture, capture->batch_id()).ok());
    pause.reset();
    SourceWriteReservation late;
    ASSERT_TRUE(
        SourceWriteReservation::create(test.options, {1, 1, 0}, detail::BudgetClass::kAdmittedWriter, &late).ok());
    std::shared_ptr<detail::AllocationReservation> bound;
    EXPECT_EQ(old->active_budget->bind(&late, &bound).code(), Code::kUnavailable);
    EXPECT_TRUE(late.valid());
    old->active->leave();
    SourceAdmission::Writer next;
    ASSERT_TRUE(test.source.try_admit_reserved(1, &late, &next).ok());
    EXPECT_NE(next.table(), old->active);
}

} // namespace
} // namespace crowdb::tree
