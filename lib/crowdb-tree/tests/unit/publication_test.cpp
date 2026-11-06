// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <barrier>
#include <future>
#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<const PublishedPage> leaf(uint64_t revision)
{
    auto result           = std::make_shared<PublishedPage>();
    result->revision      = revision;
    result->leaf          = std::make_shared<PageState>();
    result->metrics.dirty = true;
    return result;
}

std::shared_ptr<InstallRecord> record(uint64_t sequence, uint64_t generation = 1)
{
    return std::make_shared<InstallRecord>(
        InstallToken{.generation = generation, .batch = 7, .range = sequence, .sequence = 0});
}

PublicationResult publish(PublicationCoordinator &coordinator, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> done;
    auto                            result = done.get_future();
    request->on_done                       = [&done](const auto &value) { done.set_value(value); };
    if (!coordinator.try_submit(request)) {
        ADD_FAILURE() << "publication was not accepted";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

TEST(Publication, PersistentMappingCopiesEachTouchedSegmentOnce)
{
    MappingView                  initial;
    auto                         a = leaf(1);
    auto                         b = leaf(2);
    auto                         c = leaf(3);
    std::vector<PageReplacement> changes{
        {.page_id = 1,  .expected = {}, .replacement = a},
        {.page_id = 2,  .expected = {}, .replacement = b},
        {.page_id = 65, .expected = {}, .replacement = c}
    };
    MappingView::CopyStats copied;
    auto                   next = initial.replace(changes, copied);
    EXPECT_FALSE(initial.get(1));
    EXPECT_EQ(next.get(1), a);
    EXPECT_EQ(next.get(65), c);
    EXPECT_EQ(copied.directories, 4U);
    EXPECT_EQ(copied.segments, 2U);
    copied  = {};
    changes = {
        {.page_id = 2, .expected = b, .replacement = c}
    };
    auto later = next.replace(changes, copied);
    EXPECT_EQ(next.get(2), b);
    EXPECT_EQ(later.get(1), a);
    EXPECT_EQ(later.get(2), c);
    EXPECT_EQ(copied.segments, 1U);
    EXPECT_FALSE(later.get(MappingView::kPageLimit));
}

TEST(Publication, DescriptorBoundsFollowOnlyChangedRadixPathsAndShrinkAfterCoalescing)
{
    auto small       = leaf(1);
    auto large       = std::make_shared<PublishedPage>();
    auto large_state = std::make_shared<PageState>();
    large_state->bases.resize(16);
    large_state->overlays.resize(128);
    large->leaf = large_state;
    std::vector<PageReplacement> changes{
        {.page_id = 1,                 .expected = {}, .replacement = small},
        {.page_id = 65,                .expected = {}, .replacement = large},
        {.page_id = uint64_t{1} << 24, .expected = {}, .replacement = small}
    };
    MappingView::CopyStats copied;
    auto                   before = MappingView{}.replace(changes, copied);
    const auto             bounds = before.copy_bounds();
    EXPECT_EQ(bounds.leaf_pages, 3U);
    EXPECT_EQ(bounds.max_leaf_bases, 16U);
    EXPECT_EQ(bounds.max_leaf_append_bytes, bounds.max_leaf_bytes);
    EXPECT_GT(bounds.max_leaf_bytes, sizeof(PageState) + (16 * sizeof(BaseRef)));
    changes = {
        {.page_id = 65, .expected = large, .replacement = small}
    };
    auto after = before.replace(changes, copied);
    EXPECT_EQ(after.copy_bounds().leaf_pages, 3U);
    EXPECT_LT(after.copy_bounds().max_leaf_bytes, bounds.max_leaf_bytes);
    EXPECT_EQ(after.copy_bounds().max_leaf_bases, 0U);
    EXPECT_EQ(before.copy_bounds().max_leaf_bases, 16U);
    EXPECT_EQ(after.copy_bounds().leaf_bytes, 3 * after.copy_bounds().max_leaf_bytes);
    EXPECT_EQ(before.copy_bounds().max_leaf_bytes, bounds.max_leaf_bytes);
    changes = {
        {.page_id = 1,                 .expected = small, .replacement = {}},
        {.page_id = 65,                .expected = small, .replacement = {}},
        {.page_id = uint64_t{1} << 24, .expected = small, .replacement = {}}
    };
    auto empty = after.replace(changes, copied);
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.copy_bounds().leaf_pages, 0U);
    EXPECT_EQ(empty.copy_bounds().leaf_bytes, 0U);
    EXPECT_EQ(empty.copy_bounds().max_leaf_bytes, 0U);
    EXPECT_EQ(empty.copy_bounds().max_leaf_append_bytes, 0U);
    EXPECT_EQ(empty.copy_bounds().max_leaf_bases, 0U);
}

TEST(Publication, AtomicCoverageAndCatalogKeepOldViewAlive)
{
    PublicationCoordinator coordinator;
    auto                   before    = coordinator.capture();
    auto                   installed = record(1);
    auto                   page      = leaf(1);
    auto                   first     = publish(
        coordinator,
        {.record = installed, .pages = {{.page_id = 0, .expected = {}, .replacement = page}}, .root_page_id = 0});
    ASSERT_TRUE(first.status.ok());
    auto handoff   = coordinator.capture();
    auto sources   = std::make_shared<PublishedSources>();
    sources->floor = 42;
    auto result    = publish(coordinator, {.record            = record(2),
                                           .expected_coverage = 0,
                                           .coverage          = 42,
                                           .expected_sources  = handoff->sources,
                                           .sources           = sources,
                                           .required_installs = {installed}});
    ASSERT_TRUE(result.status.ok());
    const auto after = coordinator.capture();
    EXPECT_EQ(before->root_page_id, UINT64_MAX);
    EXPECT_EQ(handoff->coverage, 0U);
    EXPECT_EQ(handoff->sources->floor, 0U);
    EXPECT_EQ(after->coverage, 42U);
    EXPECT_EQ(after->sources->floor, 42U);
    EXPECT_EQ(after->mapping.get(0), page);
    EXPECT_EQ(after->stats.leaf_pages, 1U);
    EXPECT_EQ(after->stats.height, 1U);
    coordinator.reclaim();
    EXPECT_EQ(handoff->mapping.get(0), page);
}

TEST(Publication, RetryIsResourceIdempotentAndConflictsDoNotPublish)
{
    PublicationCoordinator coordinator;
    auto                   token = record(1);
    auto                   page  = leaf(1);
    PublicationTransaction tx{
        .record = token, .pages = {{.page_id = 0, .expected = {}, .replacement = page}}, .root_page_id = 0};
    auto first = publish(coordinator, tx);
    ASSERT_TRUE(first.status.ok());
    auto repeated = publish(coordinator, tx);
    EXPECT_TRUE(repeated.status.ok());
    EXPECT_TRUE(repeated.already_committed);
    EXPECT_EQ(first.commit_seq, repeated.commit_seq);
    auto rejected =
        publish(coordinator, {.record = record(2), .pages = {{.page_id = 0, .expected = {}, .replacement = leaf(2)}}});
    EXPECT_EQ(rejected.status.code(), Code::kUnavailable);
    auto state = coordinator.capture();
    EXPECT_EQ(state->commit_seq, 1U);
    EXPECT_EQ(state->stats.leaf_pages, 1U);
    EXPECT_EQ(state->stats.dirty_pages, 1U);
}

TEST(Publication, ConcurrentPagesInSameSegmentMergeIntoLatestRoot)
{
    constexpr size_t             count = 12;
    PublicationCoordinator       coordinator(32);
    std::vector<PageReplacement> initial;
    initial.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        initial.push_back({.page_id = i, .expected = {}, .replacement = leaf(1)});
    }
    ASSERT_TRUE(publish(coordinator,
                        {
                            .record = record(0), .pages = {initial.begin(), initial.end()},
                                 .root_page_id = 0
    })
                    .status.ok());
    auto                     old = coordinator.capture();
    std::barrier             start(static_cast<ptrdiff_t>(count));
    std::vector<std::thread> writers;
    writers.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        writers.emplace_back([&, i] {
            auto changed = leaf(2);
            start.arrive_and_wait();
            EXPECT_TRUE(publish(coordinator,
                                {.record = record(i + 1),
                                 .pages  = {{.page_id = i, .expected = old->mapping.get(i), .replacement = changed}}})
                            .status.ok());
        });
    }
    for (auto &writer : writers) {
        writer.join();
    }
    const auto after = coordinator.capture();
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(after->mapping.get(i)->revision, 2U);
        EXPECT_EQ(old->mapping.get(i)->revision, 1U);
    }
    EXPECT_EQ(after->stats.leaf_pages, count);
    EXPECT_EQ(after->commit_seq, count + 1);
}

TEST(Publication, IncompleteCoverageAndWrongGenerationAreRejected)
{
    PublicationCoordinator coordinator;
    const auto             old     = coordinator.capture();
    auto                   sources = std::make_shared<PublishedSources>();
    sources->floor                 = 10;
    auto result                    = publish(coordinator, {.record            = record(1),
                                                           .coverage          = 10,
                                                           .expected_sources  = old->sources,
                                                           .sources           = sources,
                                                           .required_installs = {record(2)}});
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_EQ(coordinator.capture(), old);
    result = publish(coordinator, {.record = record(3, 2)});
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_EQ(coordinator.capture(), old);
}

TEST(Publication, BoundedQueueDrainsReentrantSubmissionsAndCloseRejects)
{
    PublicationCoordinator coordinator(1);
    std::latch             callback_entered(1);
    std::latch             release_callback(1);
    auto                   first = std::make_shared<PublicationRequest>();
    first->transaction.record    = record(1);
    first->on_done               = [&](const auto &result) {
        EXPECT_TRUE(result.status.ok());
        callback_entered.count_down();
        release_callback.wait();
    };
    std::thread publisher([&] { EXPECT_TRUE(coordinator.try_submit(first)); });
    callback_entered.wait();
    auto second                = std::make_shared<PublicationRequest>();
    second->transaction.record = record(2);
    EXPECT_TRUE(coordinator.try_submit(second));
    auto third                = std::make_shared<PublicationRequest>();
    third->transaction.record = record(3);
    EXPECT_FALSE(coordinator.try_submit(third));
    coordinator.close();
    release_callback.count_down();
    publisher.join();
    EXPECT_TRUE(coordinator.idle());
    EXPECT_EQ(coordinator.capture()->commit_seq, 2U);
    EXPECT_FALSE(coordinator.try_submit(third));
}
} // namespace
} // namespace crowdb::tree
