// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> safety_record(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = sequence, .sequence = 0});
}

std::shared_ptr<PublishedPage> safety_page(uint64_t revision)
{
    auto page      = std::make_shared<PublishedPage>();
    page->revision = revision;
    page->leaf     = std::make_shared<PageState>();
    return page;
}

PublicationResult safety_publish(PublicationCoordinator &coordinator, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> done;
    auto                            result = done.get_future();
    request->on_done                       = [&done](PublicationResult value) { done.set_value(std::move(value)); };
    if (!coordinator.try_submit(request)) {
        ADD_FAILURE() << "publication was not accepted";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

TEST(PublicationSafety, SharedInstallRecordBindsToExactlyOneConcurrentPublisher)
{
    PublicationCoordinator first;
    PublicationCoordinator second;
    auto                   record = safety_record(1);
    std::barrier           start(2);
    PublicationResult      a;
    PublicationResult      b;
    std::thread            one([&] {
        start.arrive_and_wait();
        a = safety_publish(first, {.record = record});
    });
    std::thread            two([&] {
        start.arrive_and_wait();
        b = safety_publish(second, {.record = record});
    });
    one.join();
    two.join();
    EXPECT_NE(a.status.ok(), b.status.ok());
    EXPECT_EQ(a.status.ok() ? b.status.code() : a.status.code(), Code::kUnavailable);
    EXPECT_EQ(first.capture()->commit_seq + second.capture()->commit_seq, 1U);
    EXPECT_FALSE(a.already_committed);
    EXPECT_FALSE(b.already_committed);
}

TEST(PublicationSafety, ForeignInstallCannotProveSourceCoverage)
{
    PublicationCoordinator first;
    PublicationCoordinator second;
    auto                   installed = safety_record(1);
    ASSERT_TRUE(safety_publish(first, {.record = installed}).status.ok());
    const auto before  = second.capture();
    auto       sources = std::make_shared<PublishedSources>();
    sources->floor     = 42;
    const auto result  = safety_publish(second, {.record            = safety_record(2),
                                                 .coverage          = 42,
                                                 .expected_sources  = before->sources,
                                                 .sources           = sources,
                                                 .required_installs = {installed}});
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_EQ(second.capture(), before);
    EXPECT_EQ(second.capture()->sources->floor, 0U);
}

TEST(PublicationSafety, FailedMultiPageChangeLeavesMappingStatsAndRecordUncommitted)
{
    PublicationCoordinator coordinator;
    auto                   a = safety_page(1);
    auto                   b = safety_page(1);
    ASSERT_TRUE(safety_publish(coordinator,
                               {
                                   .record       = safety_record(1),
                                   .pages        = {{.page_id = 0, .expected = {}, .replacement = a},
                                                    {.page_id = 1, .expected = {}, .replacement = b}},
                                   .root_page_id = 0
    })
                    .status.ok());
    const auto before            = coordinator.capture();
    auto       good              = safety_page(2);
    auto       invalid           = safety_page(2);
    invalid->metrics.split_ready = true;
    auto record                  = safety_record(2);
    auto result = safety_publish(coordinator, {
                                                  .record = record,
                                                  .pages  = {{.page_id = 0, .expected = a, .replacement = good},
                                                             {.page_id = 1, .expected = b, .replacement = invalid}}
    });
    EXPECT_EQ(result.status.code(), Code::kInvalidArgument);
    EXPECT_EQ(coordinator.capture(), before);
    EXPECT_EQ(coordinator.capture()->mapping.get(0), a);
    EXPECT_EQ(coordinator.capture()->stats.leaf_pages, 2U);
    EXPECT_FALSE(record->committed.load());

    auto corrected = safety_page(2);
    result = safety_publish(coordinator, {
                                             .record = record,
                                             .pages  = {{.page_id = 0, .expected = a, .replacement = good},
                                                        {.page_id = 1, .expected = b, .replacement = corrected}}
    });
    EXPECT_TRUE(result.status.ok());
    EXPECT_EQ(coordinator.capture()->mapping.get(0), good);
    EXPECT_TRUE(record->committed.load());
}

TEST(PublicationSafety, RemovingAnAbsentPageCannotAdvancePidAllocation)
{
    PublicationCoordinator coordinator;
    const auto             before = coordinator.capture();
    const auto             result = safety_publish(
        coordinator, {.record = safety_record(1), .pages = {{.page_id = 999, .expected = {}, .replacement = {}}}});
    EXPECT_EQ(result.status.code(), Code::kInvalidArgument);
    EXPECT_EQ(coordinator.capture(), before);
    EXPECT_EQ(coordinator.capture()->next_page_id, 0U);
}

TEST(PublicationSafety, RemovingLastEntriesPrunesRadixPathsAndRetainsOldViews)
{
    MappingView                  initial;
    auto                         a       = safety_page(1);
    auto                         b       = safety_page(2);
    auto                         c       = safety_page(3);
    const uint64_t               distant = MappingView::kPageLimit - 1;
    MappingView::CopyStats       copied;
    std::vector<PageReplacement> add{
        {.page_id = 1,       .expected = {}, .replacement = a},
        {.page_id = 65,      .expected = {}, .replacement = b},
        {.page_id = distant, .expected = {}, .replacement = c}
    };
    auto live = initial.replace(add, copied);
    ASSERT_FALSE(live.empty());
    std::vector<PageReplacement> remove{
        {.page_id = 1,       .expected = a, .replacement = {}},
        {.page_id = distant, .expected = c, .replacement = {}}
    };
    auto partial = live.replace(remove, copied);
    EXPECT_FALSE(partial.empty());
    EXPECT_EQ(partial.get(65), b);
    EXPECT_EQ(partial.get(1), nullptr);
    EXPECT_EQ(partial.get(distant), nullptr);
    EXPECT_EQ(live.get(distant), c);
    remove = {
        {.page_id = 65, .expected = b, .replacement = {}}
    };
    auto empty = partial.replace(remove, copied);
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(partial.get(65), b);
    EXPECT_EQ(live.get(1), a);
}

TEST(PublicationSafety, CapturedRootOutlivesPublisherAndRetiredRootReclamation)
{
    std::shared_ptr<const PublishedTreeState> captured;
    auto                                      old = safety_page(1);
    {
        PublicationCoordinator coordinator;
        ASSERT_TRUE(safety_publish(coordinator, {.record       = safety_record(1),
                                                 .pages        = {{.page_id = 0, .expected = {}, .replacement = old}},
                                                 .root_page_id = 0})
                        .status.ok());
        captured  = coordinator.capture();
        auto next = safety_page(2);
        ASSERT_TRUE(safety_publish(coordinator, {.record = safety_record(2),
                                                 .pages  = {{.page_id = 0, .expected = old, .replacement = next}}})
                        .status.ok());
        coordinator.reclaim();
        EXPECT_EQ(captured->mapping.get(0), old);
    }
    EXPECT_EQ(captured->commit_seq, 1U);
    EXPECT_EQ(captured->mapping.get(0), old);
    EXPECT_EQ(captured->stats.leaf_pages, 1U);
}

TEST(PublicationSafety, ConcurrentIngressCannotStrandAcceptedRequests)
{
    constexpr size_t                 workers             = 4;
    constexpr size_t                 requests_per_worker = 128;
    PublicationCoordinator           coordinator(workers * requests_per_worker);
    std::atomic<size_t>              completed{0};
    std::barrier                     start(static_cast<ptrdiff_t>(workers));
    std::array<std::thread, workers> producers;
    for (size_t worker = 0; worker < workers; ++worker) {
        producers[worker] = std::thread([&, worker] {
            start.arrive_and_wait();
            for (size_t i = 0; i < requests_per_worker; ++i) {
                auto request                = std::make_shared<PublicationRequest>();
                request->transaction.record = safety_record((worker * requests_per_worker) + i);
                request->on_done            = [&](PublicationResult result) {
                    EXPECT_TRUE(result.status.ok());
                    completed.fetch_add(1, std::memory_order_relaxed);
                };
                EXPECT_TRUE(coordinator.try_submit(request));
            }
        });
    }
    for (auto &producer : producers) {
        producer.join();
    }
    EXPECT_TRUE(coordinator.idle());
    EXPECT_EQ(completed.load(), workers * requests_per_worker);
    EXPECT_EQ(coordinator.capture()->commit_seq, workers * requests_per_worker);
}

} // namespace
} // namespace crowdb::tree
