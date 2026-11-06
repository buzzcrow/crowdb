// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_operation.h"
#include "load_budget_test_support.h"
#include "page_cache_residency_test_support.h"

#include <algorithm>
#include <thread>

namespace crowdb::tree
{
namespace
{
using namespace test_cache_residency;

TEST(PublicationBootstrap, LifecycleChargesSourceControllerEpochAndInitialPublicationUntilLastViewRelease)
{
    LoadCredits                               credits;
    detail::FlushRuntime                      runtime(1, 8);
    auto                                      lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<const PublishedTreeState> retained;
    {
        auto       sources = detail::reserved_unique<SourceAdmission>(credits.credits.reservation(), 0, 1,
                                                                      SourceWriteOptions{.account = credits.account},
                                                                      credits.credits.reservation());
        const auto source_bytes = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        EXPECT_GT(source_bytes, sizeof(SourceAdmission));
        EXPECT_LE(source_bytes, SourceAdmission::allocation_bound().value_or(0));
        auto publisher = detail::reserved_unique<PublicationCoordinator>(
            credits.credits.reservation(), 8, 1, std::shared_ptr<TreeWorkMetrics>{}, std::pmr::get_default_resource(),
            std::shared_ptr<SharedPageCache>{},
            PublicationBootstrap{
                .metadata = credits.credits.reservation(), .initial = {}, .source_catalog = sources->catalog()});
        retained = publisher->capture();
        EXPECT_TRUE(std::ranges::equal(retained->sources->tables, sources->catalog()->tables));
        const auto publication_bytes = credits.budget.snapshot().allocated[LoadCredits::kBytes] - source_bytes;
        EXPECT_GT(publication_bytes, sizeof(PublicationCoordinator));
        EXPECT_LE(publication_bytes, PublicationCoordinator::allocation_bound(1).value_or(0));
        std::atomic<uint64_t> frontier{0};
        {
            auto controller = detail::reserved_unique<FlushController>(
                credits.credits.reservation(), *publisher, runtime, *sources, frontier, lane, FlushBatchOptions{},
                FlushBatch::Maintenance{}, credits.credits.reservation());
            const auto controller_bytes =
                credits.budget.snapshot().allocated[LoadCredits::kBytes] - source_bytes - publication_bytes;
            EXPECT_GT(controller_bytes, sizeof(FlushController));
            EXPECT_LE(controller_bytes, FlushController::allocation_bound());
        }
        credits.seal();
    }
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    retained.reset();
    runtime.drain();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PublicationBootstrap, SparseRecoveredMappingInstallsExactSourcesAndAllColdResidencyBatches)
{
    LoadCredits          credits;
    detail::FlushRuntime runtime(1, 8);
    auto                 lane      = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 resources = std::make_shared<TreeResourceMetrics>();
    auto                 cache =
        std::make_shared<SharedPageCache>(PageCacheLimits{}, credits.credits.reservation(), resources, &runtime, lane);
    SourceAdmission sources(42, 9, {}, credits.credits.reservation());
    auto            initial            = std::make_shared<PublishedTreeState>(nullptr, 42);
    initial->generation                = 9;
    initial->coverage                  = 42;
    initial->root_page_id              = 0;
    const auto                   input = cold(1);
    std::vector<PageReplacement> edits;
    edits.reserve(71);
    for (uint64_t i = 0; i < 70; ++i) {
        edits.push_back({.page_id = i * 64, .expected = {}, .replacement = published({input})});
    }
    const uint64_t last = (uint64_t{1} << 29) + 3;
    edits.push_back({.page_id = last, .expected = {}, .replacement = published({input, input})});
    MappingView::CopyStats copied;
    initial->mapping      = initial->mapping.replace(edits, copied);
    initial->next_page_id = last + 1;
    for (const auto &edit : edits) {
        ASSERT_TRUE(apply_page_metrics(initial->stats, nullptr, &edit.replacement->metrics));
    }
    const auto baseline = credits.budget.snapshot().allocated[LoadCredits::kBytes];
    {
        PublicationCoordinator publisher(
            8, 9, {}, std::pmr::get_default_resource(), cache,
            {.metadata = credits.credits.reservation(), .initial = initial, .source_catalog = sources.catalog()});
        runtime.drain();
        auto view = publisher.capture();
        EXPECT_EQ(view->coverage, 42U);
        EXPECT_EQ(view->generation, 9U);
        EXPECT_EQ(view->commit_seq, 0U);
        EXPECT_EQ(view->mapping.get(last), edits.back().replacement);
        EXPECT_TRUE(std::ranges::equal(view->sources->tables, sources.catalog()->tables));
        EXPECT_EQ(view->stats.page_count(), edits.size());
        EXPECT_EQ(resources->snapshot().proxy_pages, edits.size());
        EXPECT_LE(credits.budget.snapshot().allocated[LoadCredits::kBytes] - baseline,
                  PublicationCoordinator::allocation_bound(1, 64, &initial->mapping, true).value_or(0));
        auto upstream       = std::make_shared<TestLoader>();
        upstream->frames[1] = base({
            {.key = "a", .value = "value"}
        });
        auto loader         = cache->bind(upstream, credits.credits.reservation());
        loader->load_leaf(input, [](Status status, auto frame) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(frame);
        });
        runtime.drain();
        EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
        cache->evict();
        runtime.drain();
        EXPECT_EQ(resources->snapshot().proxy_pages, edits.size());
    }
    runtime.drain();
    EXPECT_EQ(resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(resources->snapshot().resident_frames, 0U);
}

TEST(PublicationBootstrap, FailedInitialPublicationDoesNotClaimCacheOrLeaveAllocations)
{
    LoadCredits          credits;
    detail::FlushRuntime runtime(1, 8);
    auto                 lane      = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 resources = std::make_shared<TreeResourceMetrics>();
    auto            cache = std::make_shared<SharedPageCache>(PageCacheLimits{}, nullptr, resources, &runtime, lane);
    SourceAdmission wrong(7, 1);
    const auto      initial = std::make_shared<PublishedTreeState>();
    EXPECT_THROW(
        (PublicationCoordinator(
            8, 1, {}, std::pmr::get_default_resource(), cache,
            {.metadata = credits.credits.reservation(), .initial = initial, .source_catalog = wrong.catalog()})),
        std::invalid_argument);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    SourceAdmission matching;
    {
        PublicationCoordinator publisher(
            8, 1, {}, std::pmr::get_default_resource(), cache,
            {.metadata = credits.credits.reservation(), .initial = initial, .source_catalog = matching.catalog()});
        EXPECT_TRUE(std::ranges::equal(publisher.capture()->sources->tables, matching.catalog()->tables));
    }
    runtime.drain();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PublicationBootstrap, ExhaustedLifecycleRejectsNewThreadBeforeWriterAdmission)
{
    LoadCredits     credits;
    SourceAdmission sources(0, 1, {}, credits.credits.reservation());
    const auto      catalog = sources.catalog();
    credits.seal();
    Status       status;
    std::jthread caller([&] {
        SourceAdmission::Writer writer;
        status = sources.try_admit(1, &writer);
        EXPECT_FALSE(writer.table());
    });
    caller.join();
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    EXPECT_EQ(sources.catalog(), catalog);
}

TEST(PublicationBootstrap, RejectedControlAllocationsReturnAllPartialCharges)
{
    LoadCredits credits;
    auto        token = credits.credits.reservation()->take_reserved(16);
    if (!token) {
        FAIL() << "tiny control reservation unavailable";
    }
    auto tiny = std::make_shared<detail::AllocationReservation>(std::move(*token));
    EXPECT_THROW((SourceAdmission(0, 1, {}, tiny)), std::bad_alloc);
    EXPECT_THROW((PublicationCoordinator(8, 1, {}, std::pmr::get_default_resource(), {},
                                         {.metadata = tiny, .initial = {}, .source_catalog = {}})),
                 std::bad_alloc);
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    detail::FlushRuntime   runtime(1, 4);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(4);
    std::atomic<uint64_t>  frontier{0};
    EXPECT_THROW((FlushController(publisher, runtime, sources, frontier, lane, {}, {}, tiny)), std::bad_alloc);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
