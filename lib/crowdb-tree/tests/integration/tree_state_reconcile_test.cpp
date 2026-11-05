// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_state_reconcile.h"
#include "published_read_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_read;

std::shared_ptr<PublishedTreeState> sparse_state()
{
    auto state = std::make_shared<PublishedTreeState>();
    auto leaf  = std::make_shared<PageState>();
    leaf->bases.push_back(cold(7));
    auto entry           = published(leaf);
    entry->metrics.dirty = false;
    const std::array<uint64_t, 3> ids{0, uint64_t{1} << 29, MappingView::kPageLimit - 1};
    std::vector<PageReplacement>  changes;
    for (auto pid : ids) {
        changes.push_back({.page_id = pid, .expected = {}, .replacement = entry});
        EXPECT_TRUE(apply_page_metrics(state->stats, nullptr, &entry->metrics));
    }
    MappingView::CopyStats copies;
    state->mapping      = state->mapping.replace(changes, copies);
    state->root_page_id = 0;
    state->stats.height = 1;
    return state;
}

TEST(TreeStateReconciliation, SparseColdMappingYieldsAndSkipsUnallocatedSubtrees)
{
    auto                    state = sparse_state();
    TreeStateReconciliation audit(state);
    for (size_t steps = 0; steps < 1024 && !audit.complete(); ++steps) {
        const auto before = audit.visited_pages();
        const auto status = audit.step(1);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_LE(audit.visited_pages() - before, 1U);
    }
    ASSERT_TRUE(audit.complete());
    EXPECT_EQ(audit.visited_pages(), 3U);
    EXPECT_EQ(audit.recomputed().kv_unknown_pages, 3U);
    EXPECT_EQ(audit.recomputed().dirty_pages, 0U);
}

TEST(TreeStateReconciliation, AggregateCorruptionAndInvalidWorkBoundAreReported)
{
    auto state = sparse_state();
    ++state->stats.dirty_pages;
    TreeStateReconciliation audit(state);
    EXPECT_EQ(audit.step(0).code(), Code::kInvalidArgument);
    EXPECT_FALSE(audit.complete());
    Status status;
    for (size_t i = 0; i < 1024 && !audit.complete(); ++i) {
        status = audit.step(1);
    }
    ASSERT_TRUE(audit.complete());
    EXPECT_EQ(status.code(), Code::kCorruption);
    EXPECT_EQ(audit.step(1).code(), Code::kCorruption);
}

TEST(TreeStateReconciliation, OverlayDescriptorsAreCheckedAgainstTheActualRange)
{
    auto logical = page(
        {
            {.key = "a", .value = "one"},
            {.key = "z", .value = "two"}
    },
        {.lower = "m"});
    auto entry                                 = published(logical);
    entry->metrics.overlay_runs                = 1;
    entry->metrics.overlay_entries             = 2; // Only z belongs to this logical range.
    auto                                 state = std::make_shared<PublishedTreeState>();
    MappingView::CopyStats               copies;
    const std::array<PageReplacement, 1> changes{{{.page_id = 0, .expected = {}, .replacement = entry}}};
    state->mapping = state->mapping.replace(changes, copies);
    ASSERT_TRUE(apply_page_metrics(state->stats, nullptr, &entry->metrics));
    state->root_page_id = 0;
    state->stats.height = 1;
    TreeStateReconciliation audit(state);
    ASSERT_TRUE(audit.step(1).ok());
    ASSERT_TRUE(audit.step(1).ok()); // Bounded inspection of one run.
    EXPECT_FALSE(audit.complete());
    EXPECT_EQ(audit.step(1).code(), Code::kCorruption);
}

TEST(TreeStateReconciliation, CapturedRuntimeGenerationsStayConsistentAcrossLaterFlushes)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_TRUE(tree.put("a", "first").ok());
    ASSERT_TRUE(tree.flush().ok());
    TreeStateReconciliation old(core->capture());
    ASSERT_TRUE(tree.put("b", "second").ok());
    ASSERT_TRUE(tree.flush().ok());
    TreeStateReconciliation current(core->capture());
    for (auto *audit : {&old, &current}) {
        for (size_t i = 0; i < 1024 && !audit->complete(); ++i) {
            const auto status = audit->step(4);
            ASSERT_TRUE(status.ok()) << status.to_string();
        }
        EXPECT_TRUE(audit->complete());
    }
}
} // namespace
} // namespace crowdb::tree
