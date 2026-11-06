// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(SourceTail, RegistrationBarrierWaitsForThePreviouslyAdmittedWriter)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree              tree(config);
    auto                    account = TreeCoreAccess::get(tree)->binding()->account();
    SourceAdmission         source(0, 1, SourceWriteOptions{.account = account});
    SourceAdmission::Writer before;
    ASSERT_TRUE(source.try_admit(0, {.entries = 1, .key_bytes = 1}, &before).ok());
    auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 1024 * 1024);
    if (!credit.has_value()) {
        FAIL() << "tail test metadata credit unavailable";
        return;
    }
    auto metadata = detail::AllocationReservation::create(std::move(credit.value()));
    auto tail     = detail::reserved_shared<SourceTail>(metadata, metadata, 8, 128);
    std::shared_ptr<SourceTailDomain::Barrier> barrier;
    auto                                       status = source.tail_domain()->arm(tail, metadata, &barrier);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(barrier->ready.load());
    SourceAdmission::Writer::Prepared old;
    ASSERT_TRUE(before.prepare_write("k", 0, encode_cell_buf(1, OpKind::kPut, "old"), &old).ok());
    EXPECT_TRUE(before.commit_write(std::move(old), 1));
    EXPECT_FALSE(barrier->ready.load());
    before.release(); // real Core callers acknowledge frontier before this
    barrier->wait();
    EXPECT_TRUE(barrier->ready.load());
    EXPECT_TRUE(tail->table()->empty());
    SourceAdmission::Writer after;
    ASSERT_TRUE(source.try_admit(1, {.entries = 1, .key_bytes = 1}, &after).ok());
    SourceAdmission::Writer::Prepared fresh;
    ASSERT_TRUE(after.prepare_write("k", 0, encode_cell_buf(2, OpKind::kPut, "new"), &fresh).ok());
    EXPECT_TRUE(after.commit_write(std::move(fresh), 2));
    after.release();
    ASSERT_TRUE(tail->freeze());
    EXPECT_TRUE(source.tail_domain()->detach(barrier));
    auto guard = tail->table()->read_guard(metadata);
    ASSERT_NE(tail->table()->find("k"), nullptr);
    EXPECT_EQ(tail->table()->find("k")->slot, 2U);
}

TEST(SourceTail, MovingAndOverwritingAnEnteredWriteReleasesExactlyOneTicket)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree      tree(config);
    auto            account = TreeCoreAccess::get(tree)->binding()->account();
    SourceAdmission source(0, 1, SourceWriteOptions{.account = account});
    auto            credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 1024 * 1024);
    if (!credit.has_value()) {
        FAIL() << "tail test metadata credit unavailable";
        return;
    }
    auto     metadata = detail::AllocationReservation::create(std::move(credit.value()));
    auto     tail     = detail::reserved_shared<SourceTail>(metadata, metadata, 8, 128);
    MemTable parent(1, nullptr, metadata);
    auto     prepared = parent.prepare_write("k", encode_cell_buf(1, OpKind::kPut, "v"), 0, {}, 0, 8, metadata);
    auto     first    = SourceTail::prepare(tail, prepared);
    auto     second   = SourceTail::prepare(tail, prepared);
    EXPECT_FALSE(tail->freeze());
    SourceTail::Write moved(std::move(first));
    second = std::move(moved);
    EXPECT_FALSE(tail->freeze());
    EXPECT_TRUE(parent.commit_write(std::move(prepared), 1));
    second.complete(true, 1);
    ASSERT_TRUE(tail->freeze());
    auto guard = tail->table()->read_guard(metadata);
    EXPECT_EQ(tail->table()->find("k")->slot, 1U);
}
} // namespace
} // namespace crowdb::tree
