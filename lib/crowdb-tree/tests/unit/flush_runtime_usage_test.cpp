// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_runtime.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
struct TestCleanupOwner
{
    TestCleanupOwner(std::atomic<bool> &entered, std::atomic<bool> &released) : entered(entered), released(released)
    {
    }

    ~TestCleanupOwner()
    {
        entered.store(true, std::memory_order_release);
        entered.notify_one();
        released.wait(false, std::memory_order_acquire);
    }

    std::atomic<bool> &entered;
    std::atomic<bool> &released;
};

TEST(FlushRuntimeUsage, WorkerRemainsOccupiedUntilItsContextOwnerCleanupCompletes)
{
    FlushRuntime                            runtime(1, 8);
    auto                                    lane = std::make_shared<FlushRuntime::Lane>(8);
    std::shared_ptr<FlushRuntime::Progress> progress;
    std::atomic<bool>                       published{false};
    ASSERT_TRUE(runtime
                    .reserve_progress(
                        lane,
                        [](void *context) noexcept {
                            static_cast<std::atomic<bool> *>(context)->wait(false, std::memory_order_acquire);
                        },
                        &published, {}, &progress)
                    .ok());
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    auto              owner = std::make_shared<TestCleanupOwner>(entered, released);
    progress->request(owner);
    owner.reset();
    published.store(true, std::memory_order_release);
    published.notify_one();
    entered.wait(false, std::memory_order_acquire);
    const auto during = runtime.snapshot();
    EXPECT_EQ(during.inflight, 1U);
    EXPECT_EQ(during.completed, 0U);
    released.store(true, std::memory_order_release);
    released.notify_one();
    progress->close_and_drain();
    runtime.drain();
    EXPECT_EQ(runtime.snapshot().inflight, 0U);
    EXPECT_EQ(runtime.snapshot().completed, 1U);
}
} // namespace
} // namespace crowdb::tree::detail
