// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"

#include <atomic>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
using namespace test_allocation_account;

void publish(std::atomic<bool> &flag) noexcept
{
    flag.store(true, std::memory_order_release);
    flag.notify_one();
}

void await(std::atomic<bool> &flag) noexcept
{
    flag.wait(false, std::memory_order_acquire);
}

TEST(AllocationAccountInterleaving, ActiveAccountAccessRejectsTransferWithoutSealing)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(59));
    std::atomic<bool> entered{false};
    std::atomic<bool> resume{false};
    std::thread       writer([&] {
        AllocationAccountTestAccess::release_for_tests(*test.budget, 59, [&](Step, size_t) noexcept {
            publish(entered);
            await(resume);
        });
    });
    await(entered);
    const auto status = test.budget->rebind(test.next);
    EXPECT_EQ(status.code(), Code::kUnavailable);
    EXPECT_EQ(test.next->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(test.old->snapshot().allocated[kRouting], test.base + 59);
    const bool accepted = test.budget->claim(1);
    EXPECT_TRUE(accepted);
    if (accepted) {
        test.budget->release(1);
    }
    publish(resume);
    writer.join();
    ASSERT_TRUE(test.budget->rebind(test.next).ok());
    test.expect_only_control(test.next);
    test.finish();
}

TEST(AllocationAccountInterleaving, ReopenWaitsForEnteredWriterBeforeAcceptingNewClaims)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(61));
    std::atomic<bool> release_started{false};
    std::atomic<bool> writer_entered{false};
    std::atomic<bool> resume_writer{false};
    std::atomic<bool> reopen_waiting{false};
    std::atomic<bool> completed{false};
    Status            result;
    std::thread       writer([&] {
        await(release_started);
        AllocationAccountTestAccess::release_for_tests(*test.budget, 61, [&](Step, size_t) noexcept {
            publish(writer_entered);
            await(resume_writer);
        });
    });
    std::thread       transfer([&] {
        bool injected = false;
        result =
            AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t) noexcept {
                if (!injected && step == Step::kBeforeReopen) {
                    injected = true;
                    publish(release_started);
                    await(writer_entered);
                }
                if (step == Step::kWaitForWriter) {
                    publish(reopen_waiting);
                }
            });
        publish(completed);
    });
    await(reopen_waiting);
    EXPECT_FALSE(completed.load(std::memory_order_acquire));
    EXPECT_FALSE(test.budget->claim(1));
    publish(resume_writer);
    writer.join();
    transfer.join();
    ASSERT_TRUE(result.ok()) << result.to_string();
    EXPECT_TRUE(test.budget->claim(1));
    test.budget->release(1);
    test.expect_only_control(test.next);
    test.finish();
}
} // namespace
} // namespace crowdb::tree::detail
