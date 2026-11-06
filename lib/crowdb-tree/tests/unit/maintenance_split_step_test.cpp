// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> record(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = sequence});
}

Status publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    Status status        = Status::unavailable();
    auto   request       = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    request->on_done     = [&](PublicationResult result) { status = std::move(result.status); };
    EXPECT_TRUE(publisher.try_submit(request));
    return status;
}

std::shared_ptr<PublishedPage> seed(PublicationCoordinator &publisher, size_t count)
{
    OverlayBuilder builder({});
    for (size_t i = 0; i < count; ++i) {
        std::string key  = std::to_string(1000 + i);
        auto        cell = std::make_shared<CellVersion>(encode_cell_buf(1, OpKind::kPut, std::string(16, 'v')), 1, 0);
        EXPECT_TRUE(builder.append(key, cell).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(1, &run).ok());
    auto logical = std::make_shared<PageState>();
    logical->overlays.push_back(run);
    auto page                     = std::make_shared<PublishedPage>();
    page->leaf                    = logical;
    page->metrics.dirty           = true;
    page->metrics.overlay_entries = count;
    page->metrics.overlay_runs    = 1;
    EXPECT_TRUE(publish(publisher, {.record       = record(0),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                    .root_page_id = 0})
                    .ok());
    return page;
}

TEST(MaintenanceSplitStep, OversizedLeafMakesBoundedProgressWithSharedPayloadsAndOldView)
{
    PublicationCoordinator        publisher;
    auto                          original = seed(publisher, 80);
    auto                          before   = publisher.capture();
    MaintenancePublicationOptions options;
    options.pages.max_work_entries   = 3;
    options.pages.max_output_pages   = 2;
    options.pages.primary_page_bytes = 512;
    options.pages.target_page_bytes  = 256;
    options.max_inner_children       = 3;
    auto full                        = build_split_publication(before, 0, record(1), options);
    EXPECT_EQ(full.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(publisher.capture(), before);
    uint64_t remaining = 0;
    size_t   steps     = 0;
    for (; steps < 100; ++steps) {
        auto plan = build_split_step_publication(publisher.capture(), remaining, record(steps + 2), options);
        ASSERT_TRUE(plan.status.ok()) << plan.status.message();
        if (!plan.changed()) {
            break;
        }
        const auto next = plan.output_leaf_pids.back();
        ASSERT_TRUE(publish(publisher, std::move(plan.transaction)).ok());
        auto current = publisher.capture();
        EXPECT_EQ(current->stats.overlay_entries, 80U);
        if (plan.output_leaf_pids.size() == 2) {
            EXPECT_FALSE(current->mapping.get(next)->metrics.page_size_known);
            EXPECT_TRUE(current->mapping.get(next)->metrics.split_candidate);
        }
        remaining = next;
    }
    ASSERT_LT(steps, 100U);
    const auto after = publisher.capture();
    EXPECT_GT(after->stats.leaf_pages, 2U);
    EXPECT_EQ(after->stats.page_size_unknown_pages, 0U);
    EXPECT_EQ(after->stats.kv_unknown_pages, 0U);
    EXPECT_EQ(after->stats.kv_known_count, 80U);
    size_t   seen = 0;
    uint64_t pid  = 0;
    while (pid != kInvalidPageId) {
        const auto page = after->mapping.get(pid);
        ASSERT_TRUE(page && page->leaf);
        EXPECT_LE(page->metrics.page_size_bytes, options.pages.primary_page_bytes);
        LogicalPageCursor cursor(page->leaf, page->leaf->range, UINT64_MAX, 64);
        while (cursor.valid()) {
            const auto key = std::to_string(1000 + seen++);
            EXPECT_EQ(cursor.key().to_string(), key);
            EXPECT_EQ(cursor.cell().owner, original->leaf->lookup(key).cell.owner);
            cursor.next();
        }
        pid = page->leaf->right_sibling;
    }
    EXPECT_EQ(seen, 80U);
    EXPECT_EQ(before->mapping.get(0), original);
    EXPECT_EQ(before->stats.leaf_pages, 1U);
}

TEST(MaintenanceSplitStep, ColdInputRequestsLoadAndTinyTransactionBudgetLeavesRootUntouched)
{
    PublicationCoordinator publisher;
    seed(publisher, 16);
    auto                          before = publisher.capture();
    MaintenancePublicationOptions options;
    options.pages.max_work_entries = 1;
    options.max_changed_pages      = 1;
    auto denied                    = build_split_step_publication(before, 0, record(1), options);
    EXPECT_EQ(denied.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(denied.changed());
    EXPECT_EQ(publisher.capture(), before);

    auto old        = before->mapping.get(0);
    auto logical    = std::make_shared<PageState>(*old->leaf);
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 4096;
    backing->owner  = std::make_shared<int>(1);
    logical->bases.push_back({.identity = 1, .backing = backing});
    auto proxy  = std::make_shared<PublishedPage>(*old);
    proxy->leaf = logical;
    ASSERT_TRUE(
        publish(publisher, {.record = record(2), .pages = {{.page_id = 0, .expected = old, .replacement = proxy}}})
            .ok());
    auto current = publisher.capture();
    auto needs   = build_split_step_publication(current, 0, record(3));
    ASSERT_TRUE(needs.status.ok());
    EXPECT_EQ(needs.readiness, MaintenanceReadiness::kNeedsBase);
    EXPECT_FALSE(needs.changed());
    ASSERT_EQ(needs.loads.size(), 1U);
    EXPECT_EQ(needs.loads.front().page, logical);
    EXPECT_EQ(publisher.capture(), current);
}

TEST(MaintenanceSplitStep, OneWindowPublishesSeveralExactRangesAndAnUnvisitedSuffix)
{
    PublicationCoordinator        publisher;
    const auto                    original = seed(publisher, 80);
    const auto                    before   = publisher.capture();
    MaintenancePublicationOptions options;
    options.pages.primary_page_bytes = 512;
    options.pages.target_page_bytes  = 256;
    options.pages.max_work_entries   = 80;
    options.pages.max_output_pages   = 8;
    auto plan                        = build_split_step_publication(before, 0, record(1), options);
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    ASSERT_EQ(plan.output_leaf_pids.size(), options.pages.max_output_pages);
    ASSERT_TRUE(publish(publisher, std::move(plan.transaction)).ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->commit_seq, before->commit_seq + 1);
    EXPECT_EQ(after->stats.leaf_pages, options.pages.max_output_pages);
    EXPECT_EQ(after->stats.page_size_unknown_pages, 1U);
    EXPECT_EQ(after->stats.overlay_entries, 80U);
    size_t seen = 0;
    for (size_t i = 0; i < plan.output_leaf_pids.size(); ++i) {
        const auto page = after->mapping.get(plan.output_leaf_pids[i]);
        ASSERT_TRUE(page && page->leaf);
        if (i + 1 < plan.output_leaf_pids.size()) {
            EXPECT_TRUE(page->metrics.page_size_known);
            EXPECT_LE(page->metrics.page_size_bytes, options.pages.target_page_bytes);
            EXPECT_FALSE(page->metrics.split_candidate);
        }
        else {
            EXPECT_FALSE(page->metrics.page_size_known);
            EXPECT_TRUE(page->metrics.split_candidate);
        }
        LogicalPageCursor cursor(page->leaf, page->leaf->range, UINT64_MAX, 64);
        while (cursor.valid()) {
            const auto key = std::to_string(1000 + seen++);
            EXPECT_EQ(cursor.key().to_string(), key);
            EXPECT_EQ(cursor.cell().owner, original->leaf->lookup(key).cell.owner);
            cursor.next();
        }
    }
    EXPECT_EQ(seen, 80U);
    EXPECT_EQ(before->mapping.get(0), original);
    EXPECT_EQ(before->stats.leaf_pages, 1U);
}

TEST(MaintenanceSplitStep, WorkLimitCountsAllOutputRangesAndTransactionFailurePublishesNothing)
{
    PublicationCoordinator publisher;
    seed(publisher, 80);
    const auto                    before = publisher.capture();
    MaintenancePublicationOptions options;
    options.pages.primary_page_bytes = 512;
    options.pages.target_page_bytes  = 256;
    options.pages.max_work_entries   = 5;
    auto plan                        = build_split_step_publication(before, 0, record(1), options);
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    const auto suffix_pid = plan.output_leaf_pids.back();
    const auto suffix     = std::find_if(plan.transaction.pages.begin(), plan.transaction.pages.end(),
                                         [&](const auto &page) { return page.page_id == suffix_pid; });
    ASSERT_NE(suffix, plan.transaction.pages.end());
    if (!suffix->replacement->leaf->range.lower) {
        FAIL() << "split suffix must retain its lower boundary";
    }
    EXPECT_EQ(Slice(*suffix->replacement->leaf->range.lower).to_string(), "1005");
    options.max_changed_pages = 2;
    const auto rejected       = build_split_step_publication(before, 0, record(2), options);
    EXPECT_EQ(rejected.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(rejected.changed());
    EXPECT_TRUE(rejected.output_leaf_pids.empty());
    EXPECT_EQ(publisher.capture(), before);
}
} // namespace
} // namespace crowdb::tree
