// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_frontier.h"
#include "load_budget_test_support.h"

#include <thread>

namespace crowdb::tree
{
using test_read::LoadCredits;

TEST(ApplyFrontier, NoOpCannotSkipEarlierIncompleteSlots)
{
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(0, 4, {}, &tracker).ok());
    ASSERT_TRUE(tracker->complete(3).ok());
    EXPECT_EQ(tracker->contiguous().load(), 0U);
    EXPECT_EQ(tracker->max_seen(), 3U);
    ASSERT_TRUE(tracker->complete(1).ok());
    EXPECT_EQ(tracker->contiguous().load(), 1U);
    ASSERT_TRUE(tracker->complete(2).ok());
    EXPECT_EQ(tracker->contiguous().load(), 3U);
}

TEST(ApplyFrontier, FullGapWindowRejectsBeforeMutationButStillAdmitsGapRepair)
{
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(10, 4, {}, &tracker).ok());
    for (uint64_t slot = 12; slot <= 14; ++slot) {
        ASSERT_TRUE(tracker->complete(slot).ok());
    }
    EXPECT_EQ(tracker->check_slot(15).code(), Code::kResourceExhausted);
    EXPECT_EQ(tracker->complete(15).code(), Code::kResourceExhausted);
    EXPECT_EQ(tracker->max_seen(), 14U);
    ASSERT_TRUE(tracker->complete(11).ok());
    EXPECT_EQ(tracker->contiguous().load(), 14U);
    EXPECT_TRUE(tracker->check_slot(15).ok());
}

TEST(ApplyFrontier, LateReplaysCannotEraseNewCompletionsAfterRingReuse)
{
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(0, 4, {}, &tracker).ok());
    for (uint64_t first = 1; first <= 1000; first += 4) {
        for (uint64_t offset : {3U, 2U, 1U}) {
            ASSERT_TRUE(tracker->complete(first + offset).ok());
        }
        if (first > 4) {
            for (uint64_t old = first - 4; old < first; ++old) {
                ASSERT_TRUE(tracker->complete(old).ok());
            }
        }
        EXPECT_EQ(tracker->contiguous().load(), first - 1);
        ASSERT_TRUE(tracker->complete(first).ok());
        EXPECT_EQ(tracker->contiguous().load(), first + 3);
    }
}

TEST(ApplyFrontier, ConcurrentCompletionsPublishTheirWritesBeforeTheFrontier)
{
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(0, 256, {}, &tracker).ok());
    std::array<uint64_t, 256> payload{};
    std::vector<std::jthread> workers;
    workers.reserve(4);
    for (size_t worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            for (size_t i = worker; i < payload.size(); i += 4) {
                payload[i] = i + 1;
                EXPECT_TRUE(tracker->complete(i + 1).ok());
            }
        });
    }
    uint64_t checked = 0;
    while (checked != payload.size()) {
        const auto frontier = tracker->contiguous().load(std::memory_order_acquire);
        while (checked < frontier) {
            EXPECT_EQ(payload[checked], checked + 1);
            ++checked;
        }
        std::this_thread::yield();
    }
    workers.clear();
    EXPECT_EQ(tracker->max_seen(), payload.size());
}

TEST(ApplyFrontier, ExplicitRecoveryAdvanceFoldsCompletedSuffixWithoutOverflow)
{
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(UINT64_MAX - 5, 4, {}, &tracker).ok());
    EXPECT_EQ(tracker->check_slot(UINT64_MAX).code(), Code::kResourceExhausted);
    ASSERT_TRUE(tracker->complete(UINT64_MAX - 2).ok());
    tracker->force_advance(UINT64_MAX - 3);
    EXPECT_EQ(tracker->contiguous().load(), UINT64_MAX - 2);
    ASSERT_TRUE(tracker->complete(UINT64_MAX).ok());
    ASSERT_TRUE(tracker->complete(UINT64_MAX - 1).ok());
    EXPECT_EQ(tracker->contiguous().load(), UINT64_MAX);
    tracker->force_advance(0);
    EXPECT_TRUE(tracker->complete(1).ok());
    EXPECT_EQ(tracker->contiguous().load(), UINT64_MAX);
}

TEST(ApplyFrontier, FixedStorageIsChargedOnceAndCompletionsWorkAfterSealing)
{
    LoadCredits                    credits;
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, credits.credits.reservation(), &tracker).ok());
    const auto allocated = credits.budget.snapshot().allocated[LoadCredits::kBytes];
    EXPECT_GE(allocated, sizeof(ApplyFrontier) + (8 * sizeof(std::atomic<uint64_t>)));
    const auto bound = ApplyFrontier::allocation_bound(8);
    ASSERT_TRUE(bound.has_value());
    EXPECT_LE(allocated, bound.value_or(0));
    credits.seal();
    for (uint64_t slot = 1; slot <= 100; ++slot) {
        ASSERT_TRUE(tracker->complete(slot).ok());
    }
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], allocated);
    tracker.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(ApplyFrontier, InvalidWindowAndAllocationFailureLeaveOutputUnchanged)
{
    LoadCredits                    credits;
    std::shared_ptr<ApplyFrontier> tracker;
    ASSERT_TRUE(ApplyFrontier::create(0, 4, {}, &tracker).ok());
    const auto original = tracker;
    EXPECT_EQ(ApplyFrontier::create(0, 0, {}, &tracker).code(), Code::kInvalidArgument);
    EXPECT_EQ(ApplyFrontier::create(0, SIZE_MAX, {}, &tracker).code(), Code::kInvalidArgument);
    AllocationLease pressure(credits.credits.reservation(), credits.credits.reservation()->available());
    EXPECT_EQ(ApplyFrontier::create(0, 4, credits.credits.reservation(), &tracker).code(), Code::kResourceExhausted);
    EXPECT_EQ(tracker, original);
}
} // namespace crowdb::tree
