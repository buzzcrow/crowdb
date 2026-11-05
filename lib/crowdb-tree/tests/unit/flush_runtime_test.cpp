// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/allocation_reservation.h"
#include "btree/flush_runtime.h"

#include <gtest/gtest.h>

#include <atomic>
#include <latch>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{
struct JoinResult
{
    std::atomic<size_t> values{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> stopped{0};
};

struct JoinReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    JoinResult *result;

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

TEST(FlushRuntime, SingleWorkerCanSubmitNestedWorkWithOneQueueCredit)
{
    FlushRuntime runtime(1, 1);
    auto         lane = std::make_shared<FlushRuntime::Lane>(1);

    struct Context
    {
        FlushRuntime                       *runtime;
        std::shared_ptr<FlushRuntime::Lane> lane;
        std::atomic<size_t>                 completed{0};
        std::atomic<bool>                   child_accepted{false};
    } context{.runtime = &runtime, .lane = lane};

    auto parent = [](void *argument) noexcept {
        auto &state = *static_cast<Context *>(argument);
        auto  child = [](void *inner) noexcept { static_cast<Context *>(inner)->completed.fetch_add(1); };
        state.child_accepted.store(state.runtime->try_submit(state.lane, child, &state).ok());
    };
    ASSERT_TRUE(runtime.try_submit(lane, parent, &context).ok());
    runtime.drain();
    EXPECT_TRUE(context.child_accepted.load());
    EXPECT_EQ(context.completed.load(), 1U);
    auto stats = runtime.snapshot();
    EXPECT_EQ(stats.submitted, 2U);
    EXPECT_EQ(stats.completed, 2U);
    EXPECT_EQ(stats.queued, 0U);
    EXPECT_EQ(stats.inflight, 0U);
}

TEST(FlushRuntime, SealedJobStorageSupportsRepeatedNestedDispatchWithoutNewCredit)
{
    ResourceBudget::Limits limits{};
    const auto             category = static_cast<size_t>(BudgetClass::kHandoff);
    limits[category]                = 65536;
    ResourceBudget budget(limits, 4);
    auto credit = ResourceBudget::try_reserve(budget.make_account(limits), BudgetClass::kHandoff, limits[category]);
    if (!credit) {
        FAIL() << "runtime storage credit rejected";
    }
    auto reservation = std::make_shared<AllocationReservation>(std::move(credit.value()));
    {
        FlushRuntime runtime(1, 1, reservation);
        auto         lane      = std::make_shared<FlushRuntime::Lane>(1);
        const auto   allocated = budget.snapshot().allocated[category];
        EXPECT_GT(allocated, 0U);
        reservation->seal();

        struct Context
        {
            FlushRuntime                       &runtime;
            std::shared_ptr<FlushRuntime::Lane> lane;
            std::atomic<size_t>                 completed{0};
            std::atomic<size_t>                 failures{0};

            static void step(void *argument) noexcept
            {
                auto &self = *static_cast<Context *>(argument);
                if (self.completed.fetch_add(1) + 1 < 1000 &&
                    !self.runtime.try_submit(self.lane, &Context::step, argument).ok()) {
                    self.failures.fetch_add(1);
                }
            }
        } context{.runtime = runtime, .lane = lane};

        ASSERT_TRUE(runtime.try_submit(lane, &Context::step, &context).ok());
        runtime.drain();
        EXPECT_EQ(context.completed.load(), 1000U);
        EXPECT_EQ(context.failures.load(), 0U);
        EXPECT_EQ(budget.snapshot().allocated[category], allocated);
        EXPECT_EQ(budget.snapshot().reserved[category], 0U);
    }
    EXPECT_EQ(budget.snapshot().allocated[category], 0U);
}

TEST(FlushRuntime, FailedStorageAdmissionReturnsAllClaimedCapacity)
{
    ResourceBudget::Limits limits{};
    const auto             category = static_cast<size_t>(BudgetClass::kHandoff);
    limits[category]                = 4096;
    ResourceBudget budget(limits, 4);
    auto credit = ResourceBudget::try_reserve(budget.make_account(limits), BudgetClass::kHandoff, limits[category]);
    if (!credit) {
        FAIL() << "runtime storage credit rejected";
    }
    auto reservation = std::make_shared<AllocationReservation>(std::move(credit.value()));
    EXPECT_THROW((void)FlushRuntime(1, 1024, reservation), std::bad_alloc);
    EXPECT_EQ(reservation->available(), limits[category]);
    EXPECT_EQ(budget.snapshot().allocated[category], 0U);
}

TEST(FlushRuntime, QueueAndLaneAreBoundedAndCloseDrainsAcceptedTasks)
{
    FlushRuntime runtime(1, 2);
    auto         lane_a = std::make_shared<FlushRuntime::Lane>(1);
    auto         lane_b = std::make_shared<FlushRuntime::Lane>(1);

    struct Gate
    {
        std::latch entered{1};
        std::latch release{1};
    } gate;

    auto blocker = [](void *context) noexcept {
        auto &g = *static_cast<Gate *>(context);
        g.entered.count_down();
        g.release.wait();
    };
    auto noop = [](void *) noexcept {};
    ASSERT_TRUE(runtime.try_submit(lane_a, blocker, &gate).ok());
    gate.entered.wait();
    EXPECT_TRUE(runtime.try_submit(lane_a, noop, nullptr).ok());
    EXPECT_EQ(runtime.try_submit(lane_a, noop, nullptr).code(), Code::kResourceExhausted);
    EXPECT_TRUE(runtime.try_submit(lane_b, noop, nullptr).ok());
    EXPECT_EQ(runtime.try_submit(std::make_shared<FlushRuntime::Lane>(1), noop, nullptr).code(),
              Code::kResourceExhausted);
    runtime.close();
    EXPECT_EQ(runtime.try_submit(lane_b, noop, nullptr).code(), Code::kUnavailable);
    gate.release.count_down();
    runtime.drain();
    EXPECT_EQ(runtime.snapshot().completed, 3U);
    EXPECT_EQ(runtime.snapshot().rejected, 3U);
}

TEST(FlushRuntime, LaneDrainIncludesProgressOwnerDestructionWithoutWaitingForAnotherTree)
{
    FlushRuntime runtime(2, 4);
    auto         lane  = std::make_shared<FlushRuntime::Lane>(2);
    auto         other = std::make_shared<FlushRuntime::Lane>(2);

    struct Gate
    {
        std::latch entered{1};
        std::latch release{1};
    } unrelated, destruction;

    struct Owner
    {
        Gate              *gate;
        std::atomic<bool> *destroyed;

        ~Owner()
        {
            gate->entered.count_down();
            gate->release.wait();
            destroyed->store(true, std::memory_order_release);
        }
    };

    ASSERT_TRUE(runtime
                    .try_submit(
                        other,
                        [](void *context) noexcept {
                            auto &gate = *static_cast<Gate *>(context);
                            gate.entered.count_down();
                            gate.release.wait();
                        },
                        &unrelated)
                    .ok());
    unrelated.entered.wait();
    std::atomic<bool>                       destroyed{false};
    auto                                    owner = std::make_shared<Owner>(&destruction, &destroyed);
    std::shared_ptr<FlushRuntime::Progress> progress;
    ASSERT_TRUE(runtime.reserve_progress(lane, [](void *) noexcept {}, nullptr, {}, &progress).ok());
    progress->request(std::move(owner));
    destruction.entered.wait();
    std::thread release([&] { destruction.release.count_down(); });
    lane->drain();
    EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
    EXPECT_EQ(runtime.snapshot().inflight, 1U);
    release.join();
    progress->close_and_drain();
    progress.reset();
    unrelated.release.count_down();
    other->drain();
}

TEST(BoundedRangeJoin, InlineChildrenReuseSlotsWithoutRecursiveStackGrowth)
{
    size_t           count = 0;
    BoundedRangeJoin join(10000, 1, [&](size_t index, BoundedRangeJoin::Completion complete) {
        EXPECT_EQ(index, count++);
        complete();
        complete(Status::io_error("duplicate"));
    });
    JoinResult       result;
    auto             operation = stdexec::connect(join.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(count, 10000U);
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_EQ(result.errors.load(), 0U);
}

TEST(BoundedRangeJoin, FirstErrorStopsDispatchAndWaitsForEveryStartedChild)
{
    std::vector<BoundedRangeJoin::Completion> children;
    BoundedRangeJoin join(8, 2, [&](size_t, BoundedRangeJoin::Completion complete) { children.push_back(complete); });
    JoinResult       result;
    auto             operation = stdexec::connect(join.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    ASSERT_EQ(children.size(), 2U);
    children[1](Status::io_error("injected"));
    EXPECT_EQ(result.errors.load(), 0U);
    children[0]();
    EXPECT_EQ(children.size(), 2U);
    EXPECT_EQ(result.errors.load(), 1U);
    children[1]();
    EXPECT_EQ(result.errors.load(), 1U);
}

TEST(BoundedRangeJoin, StaleSlotCompletionCannotCompleteItsReplacement)
{
    std::vector<BoundedRangeJoin::Completion> children;
    BoundedRangeJoin join(2, 1, [&](size_t, BoundedRangeJoin::Completion complete) { children.push_back(complete); });
    JoinResult       result;
    auto             operation = stdexec::connect(join.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    // Copy before completing: the starter appends and may relocate the vector.
    auto first = children[0];
    first();
    ASSERT_EQ(children.size(), 2U);
    first(Status::io_error("late duplicate"));
    EXPECT_EQ(result.values.load(), 0U);
    children[1]();
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_EQ(result.errors.load(), 0U);
}

TEST(BoundedRangeJoin, StopDrainsLateCallbacksAndPreStartStopStartsNoChildren)
{
    std::optional<BoundedRangeJoin::Completion> child;
    BoundedRangeJoin join(10, 1, [&](size_t, BoundedRangeJoin::Completion complete) { child = complete; });
    JoinResult       result;
    auto             operation = stdexec::connect(join.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    join.request_stop();
    EXPECT_EQ(result.stopped.load(), 0U);
    if (!child.has_value()) {
        FAIL() << "range was not dispatched";
    }
    child.value()();
    EXPECT_EQ(result.stopped.load(), 1U);

    BoundedRangeJoin stopped(10, 1, [](size_t, BoundedRangeJoin::Completion) { ADD_FAILURE(); });
    stopped.request_stop();
    JoinResult stopped_result;
    auto       stopped_operation = stdexec::connect(stopped.sender(), JoinReceiver{&stopped_result});
    stdexec::start(stopped_operation);
    EXPECT_EQ(stopped_result.stopped.load(), 1U);
}

TEST(BoundedRangeJoin, EmptyAndThrowingRangesHaveOneTerminalSignal)
{
    BoundedRangeJoin empty(0, 1, [](size_t, BoundedRangeJoin::Completion) { ADD_FAILURE(); });
    JoinResult       result;
    auto             operation = stdexec::connect(empty.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(result.values.load(), 1U);

    BoundedRangeJoin throwing(5, 1, [](size_t, BoundedRangeJoin::Completion) { throw std::runtime_error("injected"); });
    JoinResult       failure;
    auto             failed = stdexec::connect(throwing.sender(), JoinReceiver{&failure});
    stdexec::start(failed);
    EXPECT_EQ(failure.errors.load(), 1U);
}

TEST(BoundedRangeJoin, ReusableSlotsRunOnSharedSingleWorkerWithoutWaiting)
{
    FlushRuntime        runtime(1, 4);
    auto                lane = std::make_shared<FlushRuntime::Lane>(4);
    std::atomic<size_t> processed{0};

    struct Child
    {
        BoundedRangeJoin::Completion complete;
        std::atomic<size_t>         *processed;
    };

    BoundedRangeJoin join(100, 2, [&](size_t, BoundedRangeJoin::Completion complete) {
        auto *child = new Child{.complete = complete, .processed = &processed};
        auto  run   = [](void *context) noexcept {
            std::unique_ptr<Child> current(static_cast<Child *>(context));
            current->processed->fetch_add(1);
            current->complete();
        };
        auto status = runtime.try_submit(lane, run, child);
        if (!status.ok()) {
            delete child;
            complete(std::move(status));
        }
    });
    JoinResult       result;
    auto             operation = stdexec::connect(join.sender(), JoinReceiver{&result});
    stdexec::start(operation);
    runtime.drain();
    EXPECT_EQ(processed.load(), 100U);
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_EQ(result.errors.load(), 0U);
}

} // namespace
} // namespace crowdb::tree::detail
