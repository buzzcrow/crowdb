// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/merge_pair_metrics.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void insert(MergePairMetrics &ledger, uint64_t left, uint64_t right, uint64_t parent, uint64_t when, bool ready = true)
{
    MergePairMetrics::Prepared update(ledger, {}, {});
    update.set_pair({.left = left, .right = right, .parent = parent, .ready = ready}, when);
    update.finish();
    (void)update.commit();
}

TEST(MergePairMetrics, OverlappingAdjacentPairsCountEachPidOnce)
{
    MergePairMetrics ledger;
    insert(ledger, 1, 2, 10, 5);
    insert(ledger, 2, 3, 10, 8, false);
    EXPECT_EQ(ledger.snapshot(), (MergePairMetrics::Snapshot{3, 2, 1, 5}));
    EXPECT_TRUE(ledger.consistent());
    MergePairMetrics::Prepared update(ledger, {}, {});
    update.invalidate_page(1);
    update.finish();
    EXPECT_EQ(update.snapshot(), (MergePairMetrics::Snapshot{2, 1, 0, 8}));
    EXPECT_EQ(ledger.snapshot().pages, 3U);
    (void)update.commit();
    EXPECT_FALSE(ledger.contains_page(1));
    EXPECT_TRUE(ledger.contains_page(2));
    EXPECT_TRUE(ledger.consistent());
}

TEST(MergePairMetrics, ReadinessRetryPreservesTheFirstCandidateAge)
{
    MergePairMetrics ledger;
    insert(ledger, 1, 2, 10, 5, false);
    insert(ledger, 1, 2, 10, 20, true);
    EXPECT_EQ(ledger.snapshot(), (MergePairMetrics::Snapshot{2, 1, 1, 5}));
    EXPECT_TRUE(ledger.consistent());
}

TEST(MergePairMetrics, ChangedEndpointAndParentInvalidateOnlyTheirOwnPairs)
{
    MergePairMetrics ledger;
    insert(ledger, 1, 2, 10, 5);
    insert(ledger, 2, 3, 10, 6);
    insert(ledger, 4, 5, 11, 7);
    MergePairMetrics::Prepared update(ledger, {}, {});
    update.invalidate_page(2);
    update.invalidate_parent(10);
    update.invalidate_page(3);
    update.finish();
    (void)update.commit();
    EXPECT_EQ(ledger.snapshot(), (MergePairMetrics::Snapshot{2, 1, 1, 7}));
    EXPECT_TRUE(ledger.consistent());
}

TEST(MergePairMetrics, AbandonedPreparedChangeLeavesTheLiveIndexesIntact)
{
    MergePairMetrics ledger;
    insert(ledger, 1, 2, 10, 5);
    {
        MergePairMetrics::Prepared update(ledger, {}, {});
        update.invalidate_parent(10);
        update.set_pair({.left = 3, .right = 4, .parent = 11}, 8);
        update.finish();
    }
    EXPECT_EQ(ledger.snapshot(), (MergePairMetrics::Snapshot{2, 1, 1, 5}));
    EXPECT_TRUE(ledger.consistent());
}
} // namespace
} // namespace crowdb::tree
