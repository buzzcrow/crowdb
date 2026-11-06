// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "load_budget_test_support.h"
#include "page_cache_residency_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_cache_residency;

TEST(PageCacheResidency, CloseDetachesRuntimeWhileOldPinsAndPreparedUpdatesRemainAlive)
{
    auto fixture = std::make_unique<TestCache>();
    fixture->frame(1);
    auto input = cold(1);
    auto page  = published({input});
    ASSERT_TRUE(fixture
                    ->publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = page}
    },
                        0)
                    .status.ok());
    auto pin = fixture->load(input);
    ASSERT_TRUE(pin);
    auto                                 resources = fixture->resources;
    auto                                 residency = fixture->cache->residency();
    const std::array<PageReplacement, 1> edits{{{.page_id = 0, .expected = page, .replacement = {}}}};
    auto                                 late_update = residency->prepare(edits, {});
    auto late_ready = residency->prepare_ready(page_cache_key(PageCacheKind::Leaf, input), {});
    fixture->cache->close_and_drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    fixture.reset();
    PageCacheResidency::publish(std::move(late_update));
    PageCacheResidency::ready(late_ready);
    PageCacheResidency::evict(late_ready);
    residency->clear();
    late_ready.reset();
    residency.reset();
    EXPECT_EQ(resources->snapshot().resident_frames, 1U);
    pin.reset();
    EXPECT_EQ(resources->snapshot().resident_frames, 0U);
    EXPECT_EQ(resources->snapshot().cache_pinned_bytes, 0U);
}

TEST(PageCacheResidency, SplitViewsShareOneFrameAndOnlyLivePagesContributeToProxyCount)
{
    TestCache fixture;
    fixture.frame(1);
    auto input            = cold(1);
    auto left_ref         = input;
    left_ref.range.upper  = "m";
    auto right_ref        = input;
    right_ref.range.lower = "m";
    auto left             = published({left_ref});
    auto right            = published({right_ref});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = left },
                            {.page_id = 1, .expected = {}, .replacement = right}
    },
                        0)
                    .status.ok());
    const auto old_view = fixture.publisher.capture();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 2U);
    auto pin = fixture.load(input);
    ASSERT_TRUE(pin);
    const auto bytes = pin->page_bytes();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    EXPECT_EQ(fixture.resources->snapshot().cache_bytes, bytes);
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, bytes);
    auto second_pin = fixture.load(right_ref);
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, bytes);
    fixture.cache->evict();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 2U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    EXPECT_EQ(fixture.resources->snapshot().cache_bytes, bytes);
    auto overlay = published({});
    ASSERT_TRUE(fixture
                    .publish({
                        {.page_id = 0, .expected = left,  .replacement = overlay},
                        {.page_id = 1, .expected = right, .replacement = {}     }
    })
                    .status.ok());
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(old_view->mapping.get(1), right);
    pin.reset();
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, bytes);
    second_pin.reset();
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, 0U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 0U);
    EXPECT_EQ(fixture.resources->snapshot().cache_bytes, 0U);
}

TEST(PageCacheResidency, MultiBaseAndRepeatedBaseReferencesCountEachLogicalPageOnce)
{
    TestCache fixture;
    fixture.frame(1);
    fixture.frame(2);
    auto first  = cold(1);
    auto second = cold(2);
    auto page   = published({first, second, first});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = page}
    },
                        0)
                    .status.ok());
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    fixture.load(first).reset();
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    fixture.load(second).reset();
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 2U);
    EXPECT_EQ(fixture.resources->snapshot().cache_pinned_bytes, 0U);
    fixture.cache->evict();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 0U);
}

TEST(PageCacheResidency, ReadyBaseInstalledIntoNewLiveViewsDoesNotBecomeAProxy)
{
    TestCache fixture;
    fixture.frame(1);
    const auto input = cold(1);
    fixture.load(input).reset();
    auto page = published({input});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = page}
    },
                        0)
                    .status.ok());
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
    const auto loaded    = fixture.resources->snapshot();
    const auto before    = fixture.publisher.capture();
    auto       duplicate = published({input});
    ASSERT_TRUE(fixture
                    .publish({
                        {.page_id = 0, .expected = page, .replacement = duplicate}
    })
                    .status.ok());
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, loaded.resident_frames);
    EXPECT_EQ(fixture.resources->snapshot().cache_bytes, loaded.cache_bytes);
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
    EXPECT_EQ(before->mapping.get(0), page);
}

TEST(PageCacheResidency, LateLoadOfOldGenerationDoesNotMarkReplacementResident)
{
    TestCache fixture;
    fixture.frame(1);
    fixture.upstream->deferred = true;
    const auto input           = cold(1);
    auto       old_page        = published({input});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = old_page}
    },
                        0)
                    .status.ok());
    size_t done = 0;
    fixture.loader->load_leaf(input, [&](Status status, auto frame) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(frame);
        ++done;
    });
    auto newer = input;
    ++newer.generation;
    auto replacement = published({newer});
    ASSERT_TRUE(fixture
                    .publish({
                        {.page_id = 0, .expected = old_page, .replacement = replacement}
    })
                    .status.ok());
    fixture.upstream->finish();
    fixture.runtime.drain();
    EXPECT_EQ(done, 1U);
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    fixture.loader->load_leaf(newer, [&](Status status, auto frame) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(frame);
        ++done;
    });
    fixture.upstream->finish();
    fixture.runtime.drain();
    EXPECT_EQ(done, 2U);
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
}

TEST(PageCacheResidency, FailedAndStalePublicationsLeaveLiveMembershipUnchanged)
{
    TestCache fixture;
    auto      page = published({cold(1)});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = page}
    },
                        0)
                    .status.ok());
    auto replacement = published({});
    EXPECT_EQ(fixture
                  .publish({
                      {.page_id = 0, .expected = {}, .replacement = replacement}
    })
                  .status.code(),
              Code::kUnavailable);
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    fixture.publisher.set_before_root_for_tests([] { throw std::bad_alloc(); });
    EXPECT_EQ(fixture
                  .publish({
                      {.page_id = 0, .expected = page, .replacement = replacement}
    })
                  .status.code(),
              Code::kResourceExhausted);
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 1U);
    EXPECT_EQ(fixture.publisher.capture()->mapping.get(0), page);
    fixture.publisher.set_before_root_for_tests({});
    ASSERT_TRUE(fixture
                    .publish({
                        {.page_id = 0, .expected = page, .replacement = replacement}
    })
                    .status.ok());
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
}

TEST(PageCacheResidency, ExistingResidentAndOverlayOnlyPagesNeverContributeColdRefs)
{
    TestCache fixture;
    fixture.frame(1);
    auto resident     = cold(1);
    resident.resident = fixture.upstream->frames[1];
    auto page         = published({resident});
    auto overlay      = published({});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = page   },
                            {.page_id = 1, .expected = {}, .replacement = overlay}
    },
                        0)
                    .status.ok());
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
    fixture.cache->evict();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.cache->snapshot().proxy_pages, 0U);
    EXPECT_EQ(fixture.upstream->calls, 0U);
    const auto sample = fixture.publisher.stats(*fixture.resources);
    EXPECT_EQ(sample.logical.page_count(), 2U);
    EXPECT_EQ(sample.resources.proxy_pages, 0U);
}

TEST(PageCacheResidency, PreparedNodesRollbackAndReleaseAfterBudgetSealing)
{
    LoadCredits                          credits;
    auto                                 resources = std::make_shared<TreeResourceMetrics>();
    detail::FlushRuntime                 runtime(1, 4);
    auto                                 lane = std::make_shared<detail::FlushRuntime::Lane>(4);
    PageCacheResidency                   residency(resources, {}, &runtime, lane);
    auto                                 page = published({cold(1), cold(2)});
    const std::array<PageReplacement, 1> edits{{{.page_id = 0, .expected = {}, .replacement = page}}};
    auto                                 update    = residency.prepare(edits, credits.credits.reservation());
    const auto                           allocated = credits.budget.snapshot().allocated[LoadCredits::kBytes];
    EXPECT_GT(allocated, 0U);
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    update.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    update = residency.prepare(edits, credits.credits.reservation());
    credits.seal();
    PageCacheResidency::publish(std::move(update));
    runtime.drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 1U);
    residency.clear();
    runtime.drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
