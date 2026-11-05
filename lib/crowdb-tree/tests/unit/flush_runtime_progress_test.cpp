// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_runtime.h"
#include "load_budget_test_support.h"

#include <chrono>
#include <future>
#include <latch>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
struct TestProgressGate
{
    std::latch entered{1};
    std::latch release{1};

    static void wait(void *context) noexcept
    {
        auto &self = *static_cast<TestProgressGate *>(context);
        self.entered.count_down();
        self.release.wait();
    }
};

struct TestProgressOwner : std::enable_shared_from_this<TestProgressOwner>
{
    explicit TestProgressOwner(std::atomic<size_t> &calls) : calls(calls)
    {
    }

    std::atomic<size_t>                    &calls;
    std::shared_ptr<FlushRuntime::Progress> progress;
    size_t                                  remaining = 1;

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<TestProgressOwner *>(context);
        self.calls.fetch_add(1);
        if (--self.remaining != 0) {
            for (size_t i = 0; i < 100; ++i) {
                self.progress->request(self.weak_from_this().lock());
            }
        }
    }
};

TEST(FlushRuntime, ReservedProgressConsumesLaneAndGlobalCapacityAndSurvivesFullQueueClose)
{
    FlushRuntime        runtime(1, 2);
    auto                progress_lane = std::make_shared<FlushRuntime::Lane>(1);
    auto                ordinary_lane = std::make_shared<FlushRuntime::Lane>(2);
    std::atomic<size_t> calls{0};
    std::atomic<size_t> ordinary{0};
    auto                owner = std::make_shared<TestProgressOwner>(calls);
    ASSERT_TRUE(
        runtime.reserve_progress(progress_lane, &TestProgressOwner::run, owner.get(), {}, &owner->progress).ok());
    auto increment = [](void *context) noexcept { static_cast<std::atomic<size_t> *>(context)->fetch_add(1); };
    EXPECT_EQ(runtime.try_submit(progress_lane, increment, &ordinary).code(), Code::kResourceExhausted);
    TestProgressGate gate;
    ASSERT_TRUE(runtime.try_submit(ordinary_lane, &TestProgressGate::wait, &gate).ok());
    gate.entered.wait();
    const auto queued = runtime.try_submit(ordinary_lane, increment, &ordinary);
    EXPECT_TRUE(queued.ok());
    EXPECT_EQ(runtime.try_submit(ordinary_lane, increment, &ordinary).code(), Code::kResourceExhausted);
    runtime.close();
    for (size_t i = 0; i < 100; ++i) {
        owner->progress->request(owner);
    }
    std::weak_ptr<TestProgressOwner> retained = owner;
    owner.reset();
    EXPECT_FALSE(retained.expired());
    EXPECT_EQ(runtime.snapshot().queued, 2U);
    gate.release.count_down();
    runtime.drain();
    EXPECT_TRUE(retained.expired());
    EXPECT_EQ(calls.load(), 1U);
    EXPECT_EQ(ordinary.load(), 1U);
    EXPECT_EQ(runtime.snapshot().completed, 3U);
}

TEST(FlushRuntime, ReservedProgressRerunsUseSealedStorageAndDrainReleasesLastContextOwner)
{
    test_read::LoadCredits credits;
    FlushRuntime           runtime(1, 2, credits.credits.bytes);
    auto                   lane     = std::make_shared<FlushRuntime::Lane>(2);
    const auto             baseline = credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes];
    std::atomic<size_t>    calls{0};
    auto                   owner = reserved_shared<TestProgressOwner>(credits.credits.bytes, calls);
    owner->remaining             = 1000;
    ASSERT_TRUE(
        runtime.reserve_progress(lane, &TestProgressOwner::run, owner.get(), credits.credits.bytes, &owner->progress)
            .ok());
    std::weak_ptr<TestProgressOwner> retained = owner;
    credits.seal();
    owner->progress->request(owner);
    owner.reset();
    runtime.drain();
    EXPECT_EQ(calls.load(), 1000U);
    EXPECT_TRUE(retained.expired());
    retained.reset();
    EXPECT_EQ(runtime.snapshot().submitted, 1000U);
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], baseline);
}

TEST(FlushRuntime, FailedProgressRegistrationReturnsSlotAndQueueCredits)
{
    test_read::LoadCredits                  credits;
    FlushRuntime                            runtime(1, 1);
    auto                                    lane = std::make_shared<FlushRuntime::Lane>(1);
    std::shared_ptr<FlushRuntime::Progress> progress;
    std::atomic<size_t>                     calls{0};
    auto increment = [](void *context) noexcept { static_cast<std::atomic<size_t> *>(context)->fetch_add(1); };
    credits.seal();
    EXPECT_EQ(runtime.reserve_progress(lane, increment, &calls, credits.credits.bytes, &progress).code(),
              Code::kResourceExhausted);
    EXPECT_FALSE(progress);
    ASSERT_TRUE(runtime.try_submit(lane, increment, &calls).ok());
    runtime.drain();
    ASSERT_TRUE(runtime.reserve_progress(lane, increment, &calls, {}, &progress).ok());
    progress.reset();
    ASSERT_TRUE(runtime.try_submit(lane, increment, &calls).ok());
    runtime.drain();
    EXPECT_EQ(calls.load(), 2U);
    runtime.close();
    EXPECT_EQ(runtime.reserve_progress(lane, increment, &calls, {}, &progress).code(), Code::kUnavailable);
}

TEST(FlushRuntime, ClosedProgressOutlivesRuntimeWithoutWaitingForAnotherTree)
{
    auto                runtime    = std::make_unique<FlushRuntime>(2, 4);
    auto                lane       = std::make_shared<FlushRuntime::Lane>(2);
    auto                other_lane = std::make_shared<FlushRuntime::Lane>(2);
    std::atomic<size_t> calls{0};
    auto                owner = std::make_shared<TestProgressOwner>(calls);
    ASSERT_TRUE(runtime->reserve_progress(lane, &TestProgressOwner::run, owner.get(), {}, &owner->progress).ok());
    TestProgressGate other;
    ASSERT_TRUE(runtime->try_submit(other_lane, &TestProgressGate::wait, &other).ok());
    other.entered.wait();
    owner->progress->request(owner);
    auto       closed = std::async(std::launch::async, [&] { owner->progress->close_and_drain(); });
    const auto result = closed.wait_for(std::chrono::seconds(5));
    other.release.count_down();
    EXPECT_EQ(result, std::future_status::ready);
    closed.get();
    EXPECT_EQ(calls.load(), 1U);
    runtime.reset();
    owner->progress->request(owner);
    owner->progress->close_and_drain();
    EXPECT_EQ(calls.load(), 1U);
}

struct TestCompletionBoundary
{
    std::latch          entered{1};
    std::latch          release{1};
    std::atomic<size_t> calls{0};

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<TestCompletionBoundary *>(context);
        if (self.calls.fetch_add(1) == 0) {
            self.entered.count_down();
            self.release.wait();
        }
    }
};

TEST(FlushRuntime, RequestsBeforeAndAfterCompletionKeepTheirWake)
{
    FlushRuntime                            runtime(1, 1);
    auto                                    lane  = std::make_shared<FlushRuntime::Lane>(1);
    auto                                    owner = std::make_shared<TestCompletionBoundary>();
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(runtime.reserve_progress(lane, &TestCompletionBoundary::run, owner.get(), {}, &progress).ok());
    progress->request(owner);
    owner->entered.wait();
    for (size_t i = 0; i < 100; ++i) {
        progress->request(owner);
    }
    owner->release.count_down();
    progress->drain();
    EXPECT_EQ(owner->calls.load(), 2U);
    progress->request(owner);
    progress->drain();
    EXPECT_EQ(owner->calls.load(), 3U);
    progress->close_and_drain();
    progress->request(owner);
    lane->drain();
    EXPECT_EQ(owner->calls.load(), 3U);
}

TEST(FlushRuntime, CloseDrainsTheCoalescedRequestAcceptedBeforeCompletion)
{
    FlushRuntime                            runtime(1, 1);
    auto                                    lane  = std::make_shared<FlushRuntime::Lane>(1);
    auto                                    owner = std::make_shared<TestCompletionBoundary>();
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(runtime.reserve_progress(lane, &TestCompletionBoundary::run, owner.get(), {}, &progress).ok());
    progress->request(owner);
    owner->entered.wait();
    for (size_t i = 0; i < 100; ++i) {
        progress->request(owner);
    }
    auto closed = std::async(std::launch::async, [&] { progress->close_and_drain(); });
    EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(10)), std::future_status::timeout);
    owner->release.count_down();
    closed.get();
    progress->request(owner);
    lane->drain();
    EXPECT_EQ(owner->calls.load(), 2U);
    EXPECT_EQ(runtime.snapshot().completed, 2U);
}

TEST(FlushRuntime, ConcurrentHostRequestsDuringCompletionNeverBecomeIdleAndPending)
{
    struct Observed
    {
        std::atomic<size_t> requested{0};
        std::atomic<size_t> completed{0};

        static void run(void *context) noexcept
        {
            auto &self = *static_cast<Observed *>(context);
            self.completed.store(self.requested.load(std::memory_order_acquire), std::memory_order_release);
        }
    };

    FlushRuntime                            runtime(1, 1);
    auto                                    lane  = std::make_shared<FlushRuntime::Lane>(1);
    auto                                    owner = std::make_shared<Observed>();
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(runtime.reserve_progress(lane, &Observed::run, owner.get(), {}, &progress).ok());
    constexpr size_t requests     = 10000;
    const auto       deadline     = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    size_t           acknowledged = 0;
    for (size_t i = 1; i <= requests; ++i) {
        owner->requested.store(i, std::memory_order_release);
        progress->request(owner);
        while (owner->completed.load(std::memory_order_acquire) < i && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        if (owner->completed.load(std::memory_order_acquire) != i) {
            break;
        }
        acknowledged = i;
    }
    progress->close_and_drain();
    lane->drain();
    EXPECT_EQ(acknowledged, requests);
    EXPECT_EQ(owner->completed.load(), requests);
}

TEST(FlushRuntime, ClosingProgressWaitsForItsAcceptedTask)
{
    FlushRuntime                            runtime(1, 2);
    auto                                    lane = std::make_shared<FlushRuntime::Lane>(2);
    auto                                    gate = std::make_shared<TestProgressGate>();
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(runtime.reserve_progress(lane, &TestProgressGate::wait, gate.get(), {}, &progress).ok());
    progress->request(gate);
    gate->entered.wait();
    auto closed = std::async(std::launch::async, [&] { progress->close_and_drain(); });
    EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(10)), std::future_status::timeout);
    gate->release.count_down();
    closed.get();
    runtime.drain();
    EXPECT_EQ(runtime.snapshot().completed, 1U);
}
} // namespace
} // namespace crowdb::tree::detail
