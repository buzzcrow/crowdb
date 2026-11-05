// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT10: consolidation tests (fold by highest slot, triggers, tombstone keep,
// old-chain retirement via the epoch manager).
#include "crowdb-tree/crowdb-tree.h"
#include "runtime_tree_access.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

uint64_t overlay_runs(Crowdbtree &tree)
{
    return RuntimeTreeAccess_for_tests::root(tree)->metrics.overlay_runs;
}
} // namespace

TEST(Consolidation, FiniteFlushCoalescesOverlayRunsWithoutLosingEarlierKeys)
{
    Crowdbtree tree;
    for (uint64_t slot = 1; slot <= 5; ++slot) {
        ASSERT_TRUE(tree.apply(slot, put_one("k" + std::to_string(slot), "v")).ok());
        ASSERT_TRUE(tree.flush().ok());
        EXPECT_LE(overlay_runs(tree), 1U);
        EXPECT_EQ(tree.last_applied_slot(), slot);
        for (uint64_t prior = 1; prior <= slot; ++prior) {
            auto found = tree.get_view("k" + std::to_string(prior));
            ASSERT_TRUE(found.found());
            EXPECT_EQ(found.slot(), prior);
        }
    }
}

TEST(Consolidation, FoldKeepsHighestSlotPerKey)
{
    Config opt;
    opt.max_delta_len = 3;
    Crowdbtree t(opt);
    for (uint64_t s = 1; s <= 10; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("k", "v" + std::to_string(s))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    std::string v;
    uint64_t    slot;
    ASSERT_TRUE(t.get(Slice("k"), &slot, &v));
    EXPECT_EQ(v, "v10");
    EXPECT_EQ(slot, 10U);
}

TEST(Consolidation, TombstonePreservedThroughFold)
{
    Config opt;
    opt.max_delta_len = 2;
    Crowdbtree t(opt);
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.flush().ok());
    Batch del{{{.key = "a", .kind = OpKind::kDelete, .value = ""}}};
    ASSERT_TRUE(t.apply(2, del).ok());
    ASSERT_TRUE(t.flush().ok());
    // Drive enough flushes to force consolidation.
    for (uint64_t s = 3; s <= 6; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("b" + std::to_string(s), "x")).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // Tombstone for "a" still honored (delete wins; not resurrected).
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice("a"), &slot, &v));
}

TEST(Consolidation, OldPublishedPageRemainsAliveUntilCapturedViewIsReleased)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.put("k", "old").ok());
    ASSERT_TRUE(tree.flush().ok());
    auto                               old      = RuntimeTreeAccess_for_tests::capture(tree);
    std::weak_ptr<const PublishedPage> retained = old->mapping.get(old->root_page_id);
    ASSERT_TRUE(tree.put("k", "new").ok());
    ASSERT_TRUE(tree.flush().ok());
    RuntimeTreeAccess_for_tests::reclaim(tree);
    EXPECT_FALSE(retained.expired());
    old.reset();
    RuntimeTreeAccess_for_tests::reclaim(tree);
    EXPECT_TRUE(retained.expired());
    EXPECT_EQ(tree.get_view("k").value().to_string(), "new");
}

TEST(Consolidation, BorrowedValueSurvivesRetiredPageReclamation)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.put("k", "old").ok());
    ASSERT_TRUE(tree.flush().ok());
    auto borrowed = tree.get_view("k");
    ASSERT_TRUE(tree.put("k", "new").ok());
    ASSERT_TRUE(tree.flush().ok());
    RuntimeTreeAccess_for_tests::reclaim(tree);
    EXPECT_EQ(borrowed.value().to_string(), "old");
    EXPECT_EQ(tree.get_view("k").value().to_string(), "new");
}
