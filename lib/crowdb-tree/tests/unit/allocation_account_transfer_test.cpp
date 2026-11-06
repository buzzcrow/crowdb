// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"

#include <array>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
using namespace test_allocation_account;

TEST(AllocationAccountTransfer, ClosedDeltasFollowTheSuccessfulDestination)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(72));
    test.budget->record_allocation(AllocationKind::kFrame, 40, true);
    const auto status =
        AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t) noexcept {
            if (step == Step::kClosed) {
                EXPECT_FALSE(test.budget->claim(1));
                test.budget->record_allocation(AllocationKind::kFrame, 32, true);
                test.budget->record_allocation(AllocationKind::kFrame, 40, false);
                test.budget->release(40);
            }
            if (step == Step::kTransferred) {
                test.budget->record_allocation(AllocationKind::kFrame, 32, false);
                test.budget->release(32);
            }
        });
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.expect_only_control(test.next);
    test.finish();
}

TEST(AllocationAccountTransfer, AcquireAfterDrainCannotUnderflowOrLoseMatchingFree)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(29));
    bool       injected = false;
    const auto status =
        AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t kind) noexcept {
            if (!injected && step == Step::kAcquiredDrained && kind == kFrame) {
                injected = true;
                test.budget->record_allocation(AllocationKind::kFrame, 29, true);
                test.budget->record_allocation(AllocationKind::kFrame, 29, false);
                test.budget->release(29);
            }
        });
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(injected);
    test.expect_only_control(test.next);
    test.finish();
}

TEST(AllocationAccountTransfer, FreeAfterLastDrainForcesAnotherPassBeforeReopen)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(31));
    test.budget->record_allocation(AllocationKind::kFrame, 31, true);
    bool       injected = false;
    const auto status =
        AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t) noexcept {
            if (!injected && step == Step::kBeforeReopen) {
                injected = true;
                test.budget->record_allocation(AllocationKind::kFrame, 31, false);
                test.budget->release(31);
            }
        });
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(injected);
    test.expect_only_control(test.next);
    test.finish();
}

TEST(AllocationAccountTransfer, FailedTransferAppliesConcurrentFreesToOriginalAccount)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(43));
    test.budget->record_allocation(AllocationKind::kFrame, 43, true);
    auto       small = test.host.make_account(limits(test.base / 2));
    const auto status =
        AllocationAccountTestAccess::rebind_for_tests(*test.budget, small, [&](Step step, size_t) noexcept {
            if (step == Step::kClosed) {
                test.budget->record_allocation(AllocationKind::kFrame, 43, false);
                test.budget->release(43);
            }
        });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(small->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(small->snapshot().physical[kMetadata], 0U);
    test.expect_only_control(test.old);
    ASSERT_TRUE(test.budget->claim(43));
    test.budget->release(43);
    test.finish();
}

TEST(AllocationAccountTransfer, ConcurrentClosedWritersLeaveNoPendingCharges)
{
    TestAccounts     test;
    constexpr size_t count = 8;
    constexpr size_t bytes = 47;
    ASSERT_TRUE(test.budget->claim(count * bytes));
    const auto status =
        AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t) noexcept {
            if (step != Step::kClosed) {
                return;
            }
            std::array<std::thread, count> writers;
            for (auto &writer : writers) {
                writer = std::thread([&] {
                    test.budget->record_allocation(AllocationKind::kFrame, bytes, true);
                    test.budget->record_allocation(AllocationKind::kFrame, bytes, false);
                    test.budget->release(bytes);
                });
            }
            for (auto &writer : writers) {
                writer.join();
            }
        });
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.expect_only_control(test.next);
    test.finish();
}
} // namespace
} // namespace crowdb::tree::detail
