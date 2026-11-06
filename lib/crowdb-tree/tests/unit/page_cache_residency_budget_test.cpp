// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "inner_load_test_support.h"
#include "load_budget_test_support.h"
#include "page_cache_residency_test_support.h"

#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
using namespace test_cache_residency;

TEST(PageCacheResidency, ActualDecodedAllocationSurvivesEvictionUntilLastReaderPin)
{
    LoadCredits credits;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestInnerStore>();
    auto        source  = base({
        {.key = "a", .value = "value"}
    });
    encode_durable_page(source->frame(), source->page_bytes(), compress_algo::kNone, &store->blobs[0]);
    auto backing    = std::make_shared<BackingRef>();
    backing->length = store->blobs[0].size();
    backing->owner  = store;
    auto upstream   = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                             OverflowBackingResolver{}, credits.credits);
    auto metrics    = std::make_shared<TreeResourceMetrics>();
    {
        SharedPageCache cache({.max_pages = 1, .max_waiters = 2}, credits.credits.reservation(), metrics, runtime.get(),
                              lane);
        auto            loader         = cache.bind(upstream, credits.credits.reservation());
        const auto      infrastructure = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        std::shared_ptr<const LeafBase> pin;
        loader->load_leaf({.identity = 1, .backing = backing}, [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            pin = std::move(leaf);
        });
        runtime->drain();
        ASSERT_TRUE(pin);
        const auto bytes = pin->page_bytes();
        EXPECT_EQ(metrics->snapshot().resident_frames, 1U);
        EXPECT_EQ(metrics->snapshot().cache_bytes, bytes);
        credits.seal();
        cache.evict();
        runtime->drain();
        EXPECT_EQ(metrics->snapshot().cache_pinned_bytes, bytes);
        EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], infrastructure);
        pin.reset();
        EXPECT_EQ(metrics->snapshot().resident_frames, 0U);
        EXPECT_EQ(metrics->snapshot().cache_bytes, 0U);
        EXPECT_EQ(metrics->snapshot().cache_pinned_bytes, 0U);
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], infrastructure);
    }
    runtime->drain();
    upstream.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageCacheResidency, PinAdmissionFailureReturnsErrorWithoutDiscardingReadyFrame)
{
    TestCache fixture;
    fixture.frame(1);
    const auto input = cold(1);
    fixture.load(input).reset();
    LoadCredits credits;
    auto        loader = fixture.cache->bind(fixture.upstream, credits.credits.reservation());
    credits.seal();
    size_t done = 0;
    loader->load_leaf(input, [&](Status status, auto frame) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(frame);
        ++done;
    });
    EXPECT_EQ(done, 1U);
    EXPECT_EQ(fixture.upstream->calls, 1U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, 0U);
}

TEST(PageCacheResidency, MissAdmissionFailureOccursBeforeIoAndCanRetry)
{
    TestCache fixture;
    fixture.frame(1);
    LoadCredits credits;
    auto        loader = fixture.cache->bind(fixture.upstream, credits.credits.reservation());
    credits.seal();
    size_t     failed = 0;
    const auto input  = cold(1);
    loader->load_leaf(input, [&](Status status, auto frame) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(frame);
        ++failed;
    });
    EXPECT_EQ(failed, 1U);
    EXPECT_EQ(fixture.upstream->calls, 0U);
    EXPECT_EQ(fixture.cache->snapshot().waiters, 0U);
    ASSERT_TRUE(fixture.load(input));
    EXPECT_EQ(fixture.upstream->calls, 1U);
}

TEST(PageCacheResidency, PublisherShutdownRemovesOnlyLiveMembershipAndKeepsCachedFrames)
{
    detail::FlushRuntime runtime(1, 8);
    auto                 lane      = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 resources = std::make_shared<TreeResourceMetrics>();
    auto cache = std::make_shared<SharedPageCache>(PageCacheLimits{}, nullptr, resources, &runtime, lane);
    std::shared_ptr<const PublishedTreeState> retained;
    {
        PublicationCoordinator publisher(4, 1, {}, std::pmr::get_default_resource(), cache);
        auto                   request = std::make_shared<PublicationRequest>();
        request->transaction = {.record       = std::make_shared<InstallRecord>(InstallToken{}),
                                .pages        = {{.page_id = 0, .expected = {}, .replacement = published({cold(1)})}},
                                .root_page_id = 0};
        ASSERT_TRUE(publisher.try_submit(request));
        retained = publisher.capture();
        runtime.drain();
        EXPECT_EQ(resources->snapshot().proxy_pages, 1U);
        EXPECT_THROW((PublicationCoordinator(4, 1, {}, std::pmr::get_default_resource(), cache)),
                     std::invalid_argument);
    }
    runtime.drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    EXPECT_TRUE(retained->mapping.get(0));
}

TEST(PageCacheResidency, QueuedCommandsRetainTheirOwnersAfterCacheAndPublisherDestruction)
{
    LoadCredits          credits;
    detail::FlushRuntime runtime(1, 4);
    auto                 lane      = std::make_shared<detail::FlushRuntime::Lane>(4);
    auto                 resources = std::make_shared<TreeResourceMetrics>();

    struct Gate
    {
        std::latch entered{1}, release{1};
    } gate;

    auto blocker = [](void *context) noexcept {
        auto &self = *static_cast<Gate *>(context);
        self.entered.count_down();
        self.release.wait();
    };
    ASSERT_TRUE(runtime.try_submit(lane, blocker, &gate).ok());
    gate.entered.wait();
    {
        auto cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 1, .max_waiters = 1},
                                                       credits.credits.reservation(), resources, &runtime, lane);
        PublicationCoordinator publisher(4, 1, {}, std::pmr::get_default_resource(), cache);
        auto                   request = std::make_shared<PublicationRequest>();
        request->transaction = {.record       = std::make_shared<InstallRecord>(InstallToken{}),
                                .pages        = {{.page_id = 0, .expected = {}, .replacement = published({cold(1)})}},
                                .root_page_id = 0,
                                .metadata     = credits.credits.reservation()};
        EXPECT_TRUE(publisher.try_submit(request));
        auto upstream       = std::make_shared<TestLoader>();
        upstream->frames[1] = base({
            {.key = "a", .value = "value"}
        });
        auto loader         = cache->bind(upstream, credits.credits.reservation());
        loader->load_leaf(cold(1), [](Status status, auto frame) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(frame);
        });
        credits.seal();
        runtime.close();
    }
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    gate.release.count_down();
    runtime.drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(resources->snapshot().resident_frames, 0U);
    EXPECT_EQ(resources->snapshot().cache_pinned_bytes, 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageCacheResidency, ConcurrentCacheEventsAndMappingReplacementsLeaveExactLiveMembership)
{
    TestCache fixture;
    fixture.frame(1);
    auto input   = cold(1);
    auto current = published({input});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = current}
    },
                        0)
                    .status.ok());
    fixture.synchronize = false;
    std::atomic<bool> finished{false};
    std::jthread      reader([&] {
        while (!finished.load(std::memory_order_acquire)) {
            fixture.loader->load_leaf(input, [](Status status, auto frame) {
                EXPECT_TRUE(status.ok());
                EXPECT_TRUE(frame);
            });
            fixture.cache->evict();
        }
    });
    for (size_t i = 0; i < 100; ++i) {
        auto       next   = published({input, input});
        const auto status = fixture
                                .publish({
                                    {.page_id = 0, .expected = current, .replacement = next}
        })
                                .status;
        EXPECT_TRUE(status.ok());
        if (!status.ok()) {
            break;
        }
        current = std::move(next);
    }
    finished.store(true, std::memory_order_release);
    reader.join();
    fixture.cache->evict();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 1U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 0U);
    EXPECT_EQ(fixture.resources->snapshot().cache_bytes, 0U);
}
} // namespace
} // namespace crowdb::tree
