// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_progress.h"
#include "load_budget_test_support.h"

#include <barrier>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestProgressResult
{
    std::atomic<size_t> values{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> stopped{0};
};

struct TestProgressReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    TestProgressResult *result;

    void set_value() const noexcept
    {
        result->values.fetch_add(1);
    }

    void set_error(Status /*unused*/) const noexcept
    {
        result->errors.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        result->stopped.fetch_add(1);
    }
};

TEST(ApplyProgress, WakeupCoversTransitionsBeforeAndAfterRegistration)
{
    test_read::LoadCredits         credits;
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(1, credits.credits.reservation(), &progress).ok());
    const auto             initial = progress->sequence();
    ApplyProgress::Request before(*progress, initial);
    TestProgressResult     first;
    auto                   op_first = stdexec::connect(before.sender(), TestProgressReceiver{&first});
    progress->advance();
    stdexec::start(op_first);
    EXPECT_EQ(first.values.load(), 1U);
    ApplyProgress::Request after(*progress, progress->sequence());
    TestProgressResult     second;
    auto                   op_second = stdexec::connect(after.sender(), TestProgressReceiver{&second});
    stdexec::start(op_second);
    EXPECT_EQ(second.values.load(), 0U);
    progress->advance();
    progress->advance();
    EXPECT_EQ(second.values.load(), 1U);
    EXPECT_EQ(first.errors.load() + second.errors.load(), 0U);
}

TEST(ApplyProgress, BoundedWaitersReturnTheirSlotsOnCancellation)
{
    test_read::LoadCredits         credits;
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(1, credits.credits.reservation(), &progress).ok());
    ApplyProgress::Request first(*progress, progress->sequence());
    ApplyProgress::Request overflow(*progress, progress->sequence());
    TestProgressResult     a;
    TestProgressResult     b;
    auto                   op_a = stdexec::connect(first.sender(), TestProgressReceiver{&a});
    auto                   op_b = stdexec::connect(overflow.sender(), TestProgressReceiver{&b});
    stdexec::start(op_a);
    stdexec::start(op_b);
    EXPECT_EQ(b.errors.load(), 1U);
    first.request_stop();
    first.request_stop();
    EXPECT_EQ(a.stopped.load(), 1U);
    ApplyProgress::Request replacement(*progress, progress->sequence());
    TestProgressResult     c;
    auto                   op_c = stdexec::connect(replacement.sender(), TestProgressReceiver{&c});
    stdexec::start(op_c);
    progress->close();
    progress->advance();
    EXPECT_EQ(c.stopped.load(), 1U);
    EXPECT_EQ(c.errors.load() + c.values.load(), 0U);
}

TEST(ApplyProgress, LastIngressOwnerStopsPendingRequestsAndPreservesTheirChargedState)
{
    test_read::LoadCredits         credits;
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(2, credits.credits.reservation(), &progress).ok());
    auto               request = std::make_unique<ApplyProgress::Request>(*progress, progress->sequence());
    TestProgressResult result;
    auto               operation = stdexec::connect(request->sender(), TestProgressReceiver{&result});
    stdexec::start(operation);
    credits.seal();
    progress.reset();
    EXPECT_EQ(result.stopped.load(), 1U);
    EXPECT_GT(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
    request.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
}

struct TestDestroyingReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::unique_ptr<ApplyProgress::Request> *request;
    TestProgressResult                      *result;

    void set_value() const noexcept
    {
        result->values.fetch_add(1);
        request->reset();
    }

    void set_error(Status /*unused*/) const noexcept
    {
        result->errors.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        result->stopped.fetch_add(1);
    }
};

TEST(ApplyProgress, TerminalMayDestroyTheRequestInline)
{
    test_read::LoadCredits         credits;
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(1, credits.credits.reservation(), &progress).ok());
    auto               request = std::make_unique<ApplyProgress::Request>(*progress, progress->sequence());
    TestProgressResult result;
    auto               operation =
        stdexec::connect(request->sender(), TestDestroyingReceiver{.request = &request, .result = &result});
    stdexec::start(operation);
    progress->advance();
    EXPECT_FALSE(request);
    EXPECT_EQ(result.values.load(), 1U);
}

TEST(ApplyProgress, ConcurrentRegistrationAndTransitionNeverLoseTheWakeup)
{
    test_read::LoadCredits         credits;
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(1, credits.credits.reservation(), &progress).ok());
    for (size_t iteration = 0; iteration < 64; ++iteration) {
        ApplyProgress::Request request(*progress, progress->sequence());
        TestProgressResult     result;
        auto                   operation = stdexec::connect(request.sender(), TestProgressReceiver{&result});
        std::barrier           start(2);
        std::jthread           transition([&] {
            start.arrive_and_wait();
            progress->advance();
        });
        start.arrive_and_wait();
        stdexec::start(operation);
        transition.join();
        EXPECT_EQ(result.values.load(), 1U);
        EXPECT_EQ(result.errors.load() + result.stopped.load(), 0U);
    }
}
} // namespace
} // namespace crowdb::tree
