// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_stats_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeProgressMetrics, MaintenanceSlotsBoundAdmissionAndTrackOldestUntilItsActualTerminal)
{
    PublicationCoordinator                                        metrics;
    std::array<size_t, PublicationCoordinator::kMaintenanceSlots> slots{};
    for (size_t i = 0; i < slots.size(); ++i) {
        slots[i] = metrics.start_maintenance(100 + i);
        ASSERT_LT(slots[i], slots.size());
    }
    EXPECT_EQ(metrics.start_maintenance(1), slots.size());
    EXPECT_EQ(metrics.oldest_maintenance_since_ns(), 100U);
    metrics.finish_maintenance(slots[0]);
    EXPECT_EQ(metrics.oldest_maintenance_since_ns(), 101U);
    slots[0] = metrics.start_maintenance(500);
    EXPECT_LT(slots[0], slots.size());
    EXPECT_EQ(metrics.oldest_maintenance_since_ns(), 101U);
    for (const auto slot : slots) {
        metrics.finish_maintenance(slot);
    }
    EXPECT_EQ(metrics.oldest_maintenance_since_ns(), 0U);
}

TEST(TreeProgressMetrics, ChronologicalMembershipSurvivesEraseSwapAndRepeatedChanges)
{
    PendingPageTimes pages(std::pmr::get_default_resource());
    PendingPageTimes other(std::pmr::get_default_resource());
    EXPECT_TRUE(pages.insert(900, 100));
    EXPECT_TRUE(pages.insert(1, 200));
    EXPECT_TRUE(pages.insert(700, 300));
    EXPECT_FALSE(pages.insert(900, 400));
    EXPECT_EQ(pages.oldest_since_ns(), 100U);
    pages.erase(1);
    EXPECT_EQ(pages.oldest_since_ns(), 100U);
    other.swap(pages);
    EXPECT_EQ(pages.oldest_since_ns(), 0U);
    other.erase(900);
    EXPECT_EQ(other.oldest_since_ns(), 300U);
    other.erase_first();
    EXPECT_TRUE(other.empty());
    EXPECT_TRUE(other.insert(1, 500));
    EXPECT_EQ(other.oldest_since_ns(), 500U);
}

TEST(TreeProgressMetrics, CaptureCommitRetainsNewChangesAndFailureKeepsTheOriginalAge)
{
    CheckpointPendingMetrics pending;
    pending.change_page(10);
    const auto original = pending.oldest_since_ns();
    ASSERT_NE(original, 0U);
    ASSERT_TRUE(pending.capture(1));
    pending.change_page(10);
    pending.change_page(20);
    EXPECT_EQ(pending.oldest_since_ns(), original);
    ASSERT_TRUE(pending.finish(1, false));
    EXPECT_EQ(pending.oldest_since_ns(), original);
    ASSERT_TRUE(pending.capture(2));
    pending.change_page(20);
    ASSERT_TRUE(pending.finish(2, true));
    EXPECT_FALSE(pending.contains(10));
    EXPECT_TRUE(pending.contains(20));
    EXPECT_GE(pending.oldest_since_ns(), original);
    ASSERT_TRUE(pending.capture(3));
    ASSERT_TRUE(pending.finish(3, true));
    EXPECT_EQ(pending.oldest_since_ns(), 0U);
    pending.remove_page(20);
    EXPECT_NE(pending.oldest_since_ns(), 0U);
    ASSERT_TRUE(pending.capture(4));
    ASSERT_TRUE(pending.finish(4, true));
    EXPECT_EQ(pending.oldest_since_ns(), 0U);
}

TEST(TreeProgressMetrics, CandidateFirstEntrySurvivesReplacementAndOldViews)
{
    MappingView            mapping;
    MappingView::CopyStats copied;
    auto                   candidate         = std::make_shared<PublishedPage>();
    candidate->leaf                          = std::make_shared<PageState>();
    candidate->metrics.consolidate_candidate = true;
    const std::array<PageReplacement, 1> first{
        {{.page_id = MappingView::kPageLimit - 1, .expected = nullptr, .replacement = candidate}}};
    mapping           = mapping.replace(first, copied);
    const auto oldest = mapping.copy_bounds().candidate_since_ns;
    ASSERT_NE(oldest, 0U);
    const auto old_view                    = mapping;
    auto       changed                     = std::make_shared<PublishedPage>(*candidate);
    changed->revision                      = 20;
    changed->metrics.consolidate_candidate = false;
    changed->metrics.writeback_candidate   = true;
    const std::array<PageReplacement, 1> second{
        {{.page_id = MappingView::kPageLimit - 1, .expected = candidate, .replacement = changed}}};
    mapping = mapping.replace(second, copied);
    EXPECT_EQ(mapping.copy_bounds().candidate_since_ns, oldest);
    const std::array<PageReplacement, 1> remove{
        {{.page_id = MappingView::kPageLimit - 1, .expected = changed, .replacement = nullptr}}};
    mapping = mapping.replace(remove, copied);
    EXPECT_EQ(mapping.copy_bounds().candidate_since_ns, 0U);
    EXPECT_EQ(old_view.copy_bounds().candidate_since_ns, oldest);
}

TEST(TreeProgressMetrics, VersionedCAbiSamplesWithoutChangingTheExistingSnapshotContract)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ct_tree_progress_snapshot_v1 sample{};
    sample.struct_size = sizeof(sample);
    sample.version     = 1;
    ASSERT_EQ(ct_get_tree_progress_stats(tree, &sample), 0);
    EXPECT_NE(sample.instance_generation, 0U);
    EXPECT_NE(sample.sampled_at_ns, 0U);
    const auto sampled_at = sample.sampled_at_ns;
    sample.version        = 2;
    EXPECT_EQ(ct_get_tree_progress_stats(tree, &sample), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(sample.sampled_at_ns, sampled_at);
    ct_close(tree);
    ct_runtime_free(runtime);
}

TEST(TreeProgressMetrics, OldestWritebackUsesItsOwnMembershipAgeAcrossSparseImmutableViews)
{
    MappingView            mapping;
    MappingView::CopyStats copied;
    const auto             make = [] {
        auto page                         = std::make_shared<PublishedPage>();
        page->leaf                        = std::make_shared<PageState>();
        page->metrics.dirty               = true;
        page->metrics.writeback_candidate = true;
        return page;
    };
    const auto                           older     = make();
    const auto                           newer     = make();
    const uint64_t                       older_pid = MappingView::kPageLimit - 1;
    const std::array<PageReplacement, 1> first{{{.page_id = older_pid, .expected = nullptr, .replacement = older}}};
    mapping        = mapping.replace(first, copied);
    const auto age = mapping.copy_bounds().writeback_since_ns;
    ASSERT_NE(age, 0U);
    const auto                           old_view = mapping;
    const std::array<PageReplacement, 1> second{{{.page_id = 1, .expected = nullptr, .replacement = newer}}};
    mapping = mapping.replace(second, copied);
    EXPECT_EQ(mapping.oldest_writeback_leaf(), older_pid);
    auto changed = std::make_shared<PublishedPage>(*older);
    ++changed->revision;
    const std::array<PageReplacement, 1> replace{{{.page_id = older_pid, .expected = older, .replacement = changed}}};
    mapping = mapping.replace(replace, copied);
    EXPECT_EQ(mapping.copy_bounds().writeback_since_ns, age);
    changed                              = std::make_shared<PublishedPage>(*changed);
    changed->metrics.dirty               = false;
    changed->metrics.writeback_candidate = false;
    changed->metrics.evict_candidate     = true;
    const std::array<PageReplacement, 1> clean{{{.page_id = older_pid, .expected = nullptr, .replacement = changed}}};
    mapping = mapping.replace(clean, copied);
    EXPECT_EQ(mapping.oldest_writeback_leaf(), 1U);
    EXPECT_EQ(old_view.oldest_writeback_leaf(), older_pid);
    const std::array<PageReplacement, 1> remove{{{.page_id = 1, .expected = nullptr, .replacement = nullptr}}};
    mapping = mapping.replace(remove, copied);
    EXPECT_FALSE(mapping.oldest_writeback_leaf());
    EXPECT_EQ(mapping.copy_bounds().writeback_since_ns, 0U);
    EXPECT_NE(mapping.copy_bounds().candidate_since_ns, 0U);
}
} // namespace crowdb::tree
