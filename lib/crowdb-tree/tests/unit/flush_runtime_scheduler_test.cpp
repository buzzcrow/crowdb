// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_runtime.h"
#include "load_budget_test_support.h"

#include <array>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
void count_task(void *context) noexcept
{
    static_cast<std::atomic<size_t> *>(context)->fetch_add(1, std::memory_order_relaxed);
}

TEST(FlushRuntimeScheduler, ShortLivedSubmittersReuseAdmittedControlStorage)
{
    test_read::LoadCredits credits;
    auto                   runtime   = std::make_unique<FlushRuntime>(4, 16, credits.credits.reservation());
    auto                   lane      = std::make_shared<FlushRuntime::Lane>(16);
    const auto             allocated = credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes];
    std::atomic<size_t>    completed{0};
    for (size_t batch = 0; batch < 8; ++batch) {
        std::array<Status, 8>       outcomes;
        std::array<std::jthread, 8> submitters;
        for (size_t index = 0; index < submitters.size(); ++index) {
            submitters[index] =
                std::jthread([&, index] { outcomes[index] = runtime->try_submit(lane, &count_task, &completed); });
        }
        for (auto &thread : submitters) {
            thread.join();
        }
        lane->drain();
        for (const auto &status : outcomes) {
            EXPECT_TRUE(status.ok()) << status.to_string();
        }
        EXPECT_EQ(completed.load(), (batch + 1) * submitters.size());
        EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], allocated);
    }
    runtime.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
}

TEST(FlushRuntimeScheduler, ReservedProgressCanMoveAcrossFreshHostThreads)
{
    test_read::LoadCredits credits;
    auto                   runtime   = std::make_unique<FlushRuntime>(2, 1, credits.credits.reservation());
    auto                   lane      = std::make_shared<FlushRuntime::Lane>(1);
    auto                   completed = std::make_shared<std::atomic<size_t>>(0);
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(
        runtime->reserve_progress(lane, &count_task, completed.get(), credits.credits.reservation(), &progress).ok());
    const auto allocated = credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes];
    for (size_t iteration = 0; iteration < 32; ++iteration) {
        std::jthread submitter([&] { progress->request(completed); });
        submitter.join();
        lane->drain();
        EXPECT_EQ(completed->load(), iteration + 1);
        EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], allocated);
    }
    progress->close_and_drain();
    progress.reset();
    runtime.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree::detail
