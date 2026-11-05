// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_reservation_internal.h"
#include "flush_preparation_test_support.h"

namespace crowdb::tree
{
namespace
{
TEST(MaintenanceWindow, UsesTheLargestCompletelyFundedOutputWindow)
{
    TestPreparedFlushTree tree;
    const auto            view                = tree.publisher.capture();
    auto                  limits              = tree.preparation.maintenance;
    limits.publication.pages.max_output_pages = 7;
    const auto bytes                          = page_maintenance_reserve_bound(view->mapping, 0, limits, false, true);
    if (!bytes) {
        FAIL() << "maintenance bound missing";
    }
    limits.publication.pages.max_output_pages = 64;
    limit_page_maintenance_window(view->mapping, 0, *bytes, SIZE_MAX, false, &limits);
    EXPECT_EQ(limits.publication.pages.max_output_pages, 7U);
    EXPECT_EQ(page_maintenance_reserve_bound(view->mapping, 0, limits, false, true), bytes);
    limits.publication.pages.max_output_pages = 8;
    EXPECT_GT(page_maintenance_reserve_bound(view->mapping, 0, limits, false, true), bytes);
}

TEST(MaintenanceWindow, PreservesMinimumProgressAndNeverExceedsTheWorkLimit)
{
    TestPreparedFlushTree tree;
    const auto            view                = tree.publisher.capture();
    auto                  limits              = tree.preparation.maintenance;
    limits.publication.pages.max_output_pages = 64;
    limit_page_maintenance_window(view->mapping, 0, 0, 0, false, &limits);
    EXPECT_EQ(limits.publication.pages.max_output_pages, 2U);
    limits.publication.pages.max_output_pages = 64;
    limits.publication.pages.max_work_entries = 5;
    limit_page_maintenance_window(view->mapping, 0, SIZE_MAX, SIZE_MAX, false, &limits);
    EXPECT_EQ(limits.publication.pages.max_output_pages, 6U);
}

TEST(MaintenanceWindow, FlushBindsTheChosenWindowAndFinishesUsingItsHeldCredit)
{
    TestPreparedFlushTree tree;
    tree.frontier.store(1);
    tree.preparation.maintenance.publication.pages.max_output_pages = 64;
    const auto view                                                 = tree.publisher.capture();
    auto       limits                                               = tree.preparation.maintenance;
    limits.publication.pages.max_output_pages                       = 7;
    // Ordinary admission is paused, but the missing-slot allowance stays open.
    const auto gap_entries = tree.sources.catalog()->active_budget->options().max_gap_entries;
    const auto bytes       = page_maintenance_reserve_bound(view->mapping, gap_entries, limits, false, true);
    if (!bytes) {
        FAIL() << "maintenance bound missing";
    }
    const auto available = detail::ResourceBudget::available(tree.account, detail::BudgetClass::kReclaim);
    ASSERT_GT(available, *bytes);
    auto blocker = detail::ResourceBudget::try_reserve(tree.account, detail::BudgetClass::kReclaim, available - *bytes);
    ASSERT_TRUE(blocker);
    FlushReservePlan                plan;
    std::shared_ptr<FlushOperation> operation;
    auto                            status = tree.controller->prepare_or_plan(tree.preparation, &operation, &plan);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(operation);
    ASSERT_EQ(plan.bounds.maintenance_output_pages, 7U);
    detail::FlushCredits credits;
    for (size_t index = 0; index < credits.size(); ++index) {
        auto credit = detail::ResourceBudget::try_reserve(tree.account, detail::FlushReserveBounds::categories[index],
                                                          plan.bounds.amounts[index]);
        if (!credit) {
            FAIL() << "flush grant missing: " << index;
        }
        credits[index] = std::move(*credit);
    }
    status = tree.controller->prepare_prepaid(tree.preparation, plan, std::move(credits), &operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
    EXPECT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.to_string();
    EXPECT_EQ(operation->result().published_coverage, 1U);
}

TEST(MaintenanceWindow, IdleReacquisitionExpandsAndShrinksWithCurrentPressure)
{
    TestPreparedFlushTree tree;
    const auto            view                 = tree.publisher.capture();
    auto                  options              = tree.preparation.maintenance;
    options.publication.pages.max_output_pages = 64;
    detail::ReclaimAdmission admission{.publisher = &tree.publisher, .account = tree.account, .options = options};
    auto                     seven           = options;
    seven.publication.pages.max_output_pages = 7;
    const auto bound                         = page_maintenance_reserve_bound(view->mapping, 0, seven, false, true);
    if (!bound) {
        FAIL() << "missing pressure fixture bound";
    }
    const auto available = detail::ResourceBudget::available(tree.account, detail::BudgetClass::kReclaim);
    ASSERT_GT(available, *bound);
    auto blocker = detail::ResourceBudget::try_reserve(tree.account, detail::BudgetClass::kReclaim, available - *bound);
    ASSERT_TRUE(blocker);
    MaintenancePublicationOptions                  selected;
    std::shared_ptr<detail::AllocationReservation> credit;
    auto                                           status = admission.acquire_window(view->mapping, &selected, &credit);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(selected.pages.max_output_pages, 7U);
    ASSERT_TRUE(credit);
    EXPECT_GT(credit->available(), 0U);
    EXPECT_EQ(detail::ResourceBudget::available(tree.account, detail::BudgetClass::kReclaim), 0U);
    credit.reset();
    selected.snapshot_metadata.reset();
    blocker.reset();
    auto expected = options;
    limit_page_maintenance_window(
        view->mapping, 0, detail::ResourceBudget::available(tree.account, detail::BudgetClass::kReclaim),
        detail::ResourceBudget::available(tree.account, detail::BudgetClass::kRouting), false, &expected);
    ASSERT_GT(expected.publication.pages.max_output_pages, 7U);
    status = admission.acquire_window(view->mapping, &selected, &credit);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(selected.pages.max_output_pages, expected.publication.pages.max_output_pages);
    credit.reset();
    selected.snapshot_metadata.reset();
    blocker = detail::ResourceBudget::try_reserve(tree.account, detail::BudgetClass::kReclaim, available - *bound);
    ASSERT_TRUE(blocker);
    status = admission.acquire_window(view->mapping, &selected, &credit);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(selected.pages.max_output_pages, 7U);
}

TEST(MaintenanceWindow, FailedIdleAdmissionDoesNotChangeTheWindowOrHoldPartialCredit)
{
    TestPreparedFlushTree    tree;
    const auto               view = tree.publisher.capture();
    detail::ReclaimAdmission admission{
        .publisher = &tree.publisher, .account = tree.account, .options = tree.preparation.maintenance};
    const auto available = detail::ResourceBudget::available(tree.account, detail::BudgetClass::kReclaim);
    auto       blocker   = detail::ResourceBudget::try_reserve(tree.account, detail::BudgetClass::kReclaim, available);
    ASSERT_TRUE(blocker);
    const auto routing = detail::ResourceBudget::available(tree.account, detail::BudgetClass::kRouting);
    MaintenancePublicationOptions selected;
    selected.pages.max_output_pages = 7;
    std::shared_ptr<detail::AllocationReservation> credit;
    EXPECT_EQ(admission.acquire_window(view->mapping, &selected, &credit).code(), Code::kUnavailable);
    EXPECT_FALSE(credit);
    EXPECT_FALSE(selected.snapshot_metadata);
    EXPECT_EQ(selected.pages.max_output_pages, 7U);
    EXPECT_EQ(detail::ResourceBudget::available(tree.account, detail::BudgetClass::kRouting), routing);
}
} // namespace
} // namespace crowdb::tree
