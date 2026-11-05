// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"
#include "page_cache_residency_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_cache_residency;

std::shared_ptr<PublicationCoordinator::Replacement> replacement(TestCache &fixture, size_t pages, const BaseRef &base)
{
    auto candidate          = std::make_shared<PublishedTreeState>();
    candidate->generation   = fixture.publisher.capture()->generation + 1;
    candidate->root_page_id = 0;
    candidate->next_page_id = pages;
    PublicationTransaction::Pages edits;
    for (size_t i = 0; i < pages; ++i) {
        edits.push_back({.page_id = i, .expected = {}, .replacement = published({base})});
    }
    MappingView::CopyStats copied;
    candidate->mapping = candidate->mapping.replace(edits, copied);
    SourceAdmission                                      sources(0, candidate->generation);
    std::shared_ptr<PublicationCoordinator::Replacement> result;
    EXPECT_TRUE(
        fixture.publisher
            .prepare_replacement({.metadata = {}, .initial = candidate, .source_catalog = sources.catalog()}, &result)
            .ok());
    return result;
}

TEST(PublicationGeneration, NewNamespaceClearsOldMembershipWithoutDroppingIndependentReadyFrames)
{
    TestCache fixture;
    fixture.frame(1);
    const auto base     = cold(1);
    auto       old_page = published({base});
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = old_page}
    },
                        0)
                    .status.ok());
    auto old_view = fixture.publisher.capture();
    auto pin      = fixture.load(base);
    ASSERT_TRUE(pin);
    auto prepared = replacement(fixture, 97, cold(2));
    ASSERT_TRUE(prepared);
    fixture.publisher.request_gc_floor(100);
    fixture.publisher.install_replacement(std::move(prepared));
    fixture.runtime.drain();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 97U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    EXPECT_EQ(old_view->mapping.get(0), old_page);
    EXPECT_EQ(fixture.publisher.requested_gc_floor(), 0U);
    auto current = fixture.publisher.capture();
    EXPECT_EQ(current->generation, 2U);
    EXPECT_GT(current->commit_seq, old_view->commit_seq);
    EXPECT_EQ(current->stats.checkpoint_pending_pages, 97U);
    EXPECT_TRUE(current->stats.checkpoint_mapping_pending);
    fixture.publisher.install_replacement(replacement(fixture, 2, base));
    fixture.runtime.drain();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 0U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    EXPECT_EQ(fixture.publisher.capture()->stats.checkpoint_pending_pages, 2U);
    fixture.cache->evict();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 2U);
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 1U);
    pin.reset();
    EXPECT_EQ(fixture.resources->snapshot().resident_frames, 0U);
}

TEST(PublicationGeneration, DiscardedPreparationLeavesTheOldCheckpointLedgerAndRootUnchanged)
{
    TestCache fixture;
    ASSERT_TRUE(fixture
                    .publish(
                        {
                            {.page_id = 0, .expected = {}, .replacement = published({cold(1)})}
    },
                        0)
                    .status.ok());
    auto original = fixture.publisher.capture();
    auto prepared = replacement(fixture, 64, cold(2));
    ASSERT_TRUE(prepared);
    prepared.reset();
    fixture.runtime.drain();
    EXPECT_EQ(fixture.publisher.capture(), original);
    EXPECT_EQ(fixture.resources->snapshot().proxy_pages, 1U);
    EXPECT_EQ(fixture.publisher.capture()->stats.checkpoint_pending_pages, 1U);
    fixture.publisher.reclaim();
}
} // namespace
} // namespace crowdb::tree
