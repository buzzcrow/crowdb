// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_dispatcher.h"
#include "load_budget_test_support.h"

#include <array>
#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestDispatcherTask
{
    TestDispatcherTask(std::atomic<size_t> &completed, std::atomic<size_t> &destroyed)
        : completed(completed),
          destroyed(destroyed),
          node(&TestDispatcherTask::run, this)
    {
    }

    ~TestDispatcherTask()
    {
        destroyed.fetch_add(1);
    }

    static void run(void *context) noexcept
    {
        static_cast<TestDispatcherTask *>(context)->completed.fetch_add(1);
    }

    std::atomic<size_t>  &completed;
    std::atomic<size_t>  &destroyed;
    ApplyDispatcher::Node node;
};

TEST(ApplyDispatcher, ConcurrentProducersRetainAndExecuteEachQueuedOwnerOnce)
{
    test_read::LoadCredits           credits;
    auto                             runtime = std::make_shared<detail::FlushRuntime>(2, 4);
    auto                             lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    std::shared_ptr<ApplyDispatcher> dispatcher;
    ASSERT_TRUE(ApplyDispatcher::create(runtime, lane, 3, credits.credits.reservation(), &dispatcher).ok());
    credits.seal();
    std::atomic<size_t>         completed{0};
    std::atomic<size_t>         destroyed{0};
    std::array<std::jthread, 8> producers;
    for (auto &producer : producers) {
        producer = std::jthread([&] {
            for (size_t index = 0; index < 128; ++index) {
                auto owner = std::make_shared<TestDispatcherTask>(completed, destroyed);
                dispatcher->request(owner->node, owner);
            }
        });
    }
    for (auto &producer : producers) {
        producer.join();
    }
    dispatcher->close_and_drain();
    lane->drain();
    EXPECT_EQ(completed.load(), 1024U);
    EXPECT_EQ(destroyed.load(), 1024U);
    dispatcher.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
}

TEST(ApplyDispatcher, ReentrantContinuationKeepsFifoOrderAcrossBoundedTurns)
{
    test_read::LoadCredits           credits;
    auto                             runtime = std::make_shared<detail::FlushRuntime>(1, 2);
    auto                             lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    std::shared_ptr<ApplyDispatcher> dispatcher;
    ASSERT_TRUE(ApplyDispatcher::create(runtime, lane, 1, credits.credits.reservation(), &dispatcher).ok());

    struct Context : std::enable_shared_from_this<Context>
    {
        explicit Context(std::shared_ptr<ApplyDispatcher> target)
            : dispatcher(std::move(target)),
              first(&Context::run_first, this),
              second(&Context::run_second, this)
        {
        }

        static void run_first(void *argument) noexcept
        {
            auto &self               = *static_cast<Context *>(argument);
            self.order[self.count++] = 1;
            if (self.count == 1) {
                self.dispatcher->request(self.second, self.weak_from_this().lock());
                self.dispatcher->request(self.first, self.weak_from_this().lock());
            }
        }

        static void run_second(void *argument) noexcept
        {
            auto &self               = *static_cast<Context *>(argument);
            self.order[self.count++] = 2;
        }

        std::shared_ptr<ApplyDispatcher> dispatcher;
        ApplyDispatcher::Node            first;
        ApplyDispatcher::Node            second;
        std::array<size_t, 3>            order{};
        size_t                           count = 0;
    };

    auto owner = std::make_shared<Context>(dispatcher);
    dispatcher->request(owner->first, owner);
    dispatcher->close_and_drain();
    EXPECT_EQ(owner->count, 3U);
    EXPECT_EQ(owner->order, (std::array<size_t, 3>{1, 2, 1}));
    EXPECT_GE(runtime->snapshot().completed, 3U);
}

TEST(ApplyDispatcher, DuplicateWakeupsCoalesceAndPrepaidProgressSurvivesRuntimeClose)
{
    test_read::LoadCredits           credits;
    auto                             runtime = std::make_shared<detail::FlushRuntime>(1, 2);
    auto                             lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    std::shared_ptr<ApplyDispatcher> dispatcher;
    ASSERT_TRUE(ApplyDispatcher::create(runtime, lane, 1, credits.credits.reservation(), &dispatcher).ok());

    struct Gate
    {
        std::latch entered{1};
        std::latch release{1};
    } gate;

    ASSERT_TRUE(runtime
                    ->try_submit(
                        std::make_shared<detail::FlushRuntime::Lane>(1),
                        [](void *context) noexcept {
                            auto &state = *static_cast<Gate *>(context);
                            state.entered.count_down();
                            state.release.wait();
                        },
                        &gate)
                    .ok());
    gate.entered.wait();
    runtime->close();
    std::atomic<size_t> completed{0};
    std::atomic<size_t> destroyed{0};
    auto                owner = std::make_shared<TestDispatcherTask>(completed, destroyed);
    for (size_t wakeup = 0; wakeup < 32; ++wakeup) {
        dispatcher->request(owner->node, owner);
    }
    owner.reset();
    EXPECT_EQ(destroyed.load(), 0U);
    gate.release.count_down();
    dispatcher->close_and_drain();
    runtime->drain();
    EXPECT_EQ(completed.load(), 1U);
    EXPECT_EQ(destroyed.load(), 1U);
}
} // namespace
} // namespace crowdb::tree
