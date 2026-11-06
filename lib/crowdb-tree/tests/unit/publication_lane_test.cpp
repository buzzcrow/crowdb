// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/publication_lane.h"
#include "btree/published_state.h"
#include "load_budget_test_support.h"

#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<PublicationRequest> request(uint64_t sequence)
{
    auto value = std::make_shared<PublicationRequest>();
    value->transaction.record =
        std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = sequence});
    return value;
}

TEST(PublicationLane, PrepaidWindowProgressesWhenOrdinaryQueueIsFullAndReturnsBeforeCallback)
{
    test_read::LoadCredits           credits;
    PublicationCoordinator           publisher(2);
    std::shared_ptr<PublicationLane> lane;
    ASSERT_TRUE(publisher.prepare_lane(2, credits.credits.reservation(), &lane).ok());
    std::latch entered(1);
    std::latch release(1);
    auto       blocker = request(1);
    blocker->on_done   = [&](auto result) {
        EXPECT_TRUE(result.status.ok());
        entered.count_down();
        release.wait();
    };
    std::jthread worker([&] { EXPECT_TRUE(publisher.try_submit(blocker)); });
    entered.wait();
    EXPECT_TRUE(publisher.try_submit(request(2)));
    EXPECT_TRUE(publisher.try_submit(request(3)));
    EXPECT_FALSE(publisher.try_submit(request(4)));
    auto                first     = request(5);
    auto                second    = request(6);
    auto                reentrant = request(7);
    std::atomic<size_t> completed{0};
    reentrant->on_done = [&](auto result) {
        EXPECT_TRUE(result.status.ok());
        completed.fetch_add(1);
    };
    first->on_done = [&](auto result) {
        EXPECT_TRUE(result.status.ok());
        EXPECT_TRUE(publisher.try_submit(reentrant, lane));
        completed.fetch_add(1);
    };
    second->on_done = [&](auto result) {
        EXPECT_TRUE(result.status.ok());
        completed.fetch_add(1);
    };
    EXPECT_TRUE(publisher.try_submit(first, lane));
    EXPECT_TRUE(publisher.try_submit(second, lane));
    EXPECT_FALSE(publisher.try_submit(request(8), lane));
    release.count_down();
    worker.join();
    EXPECT_EQ(completed.load(), 3U);
    EXPECT_TRUE(publisher.idle());
}

TEST(PublicationLane, WindowIsFixedUniqueAndBoundToItsPublisher)
{
    test_read::LoadCredits           credits;
    PublicationCoordinator           publisher(2);
    PublicationCoordinator           other(2);
    std::shared_ptr<PublicationLane> lane;
    std::shared_ptr<PublicationLane> duplicate;
    EXPECT_EQ(publisher.prepare_lane(3, credits.credits.reservation(), &lane).code(), Code::kInvalidArgument);
    ASSERT_TRUE(publisher.prepare_lane(2, credits.credits.reservation(), &lane).ok());
    EXPECT_EQ(publisher.prepare_lane(1, credits.credits.reservation(), &duplicate).code(), Code::kUnavailable);
    EXPECT_FALSE(other.try_submit(request(1), lane));
    publisher.close();
    EXPECT_FALSE(publisher.try_submit(request(2), lane));
}
} // namespace
} // namespace crowdb::tree
