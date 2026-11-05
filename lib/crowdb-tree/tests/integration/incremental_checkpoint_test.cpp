// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT6d: incremental snapshot writes only dirty pages, retains clean pages'
// durable addrs, and reopens to identical state.
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "runtime_tree_access.h"
#include "snapshot/primary_backing.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

// Bounded batches exercise real byte-aware splitting without relying on one
// oversized leaf or a pointer-delta chain.
void fill(Crowdbtree *tree, int count, std::map<std::string, std::string> *oracle)
{
    for (int first = 0; first < count; first += 100) {
        Batch     batch;
        const int last = std::min(first + 100, count);
        for (int index = first; index < last; ++index) {
            auto value = "val" + std::to_string(index);
            batch.ops.push_back({.key = key(index), .kind = OpKind::kPut, .value = value});
            (*oracle)[key(index)] = value;
        }
        ASSERT_TRUE(tree->apply(static_cast<uint64_t>(last), batch).ok());
        tree->force_advance_slot(static_cast<uint64_t>(last));
        ASSERT_TRUE(tree->flush().ok());
    }
}

using DurableLocations = std::map<uint64_t, std::pair<uint64_t, uint64_t>>;

DurableLocations durable_locations(Crowdbtree &tree)
{
    DurableLocations result;
    RuntimeTreeAccess_for_tests::capture(tree)->mapping.visit(
        &result, [](void *raw, uint64_t pid, const std::shared_ptr<const PublishedPage> &page) {
            if (page->primary && page->primary->frame.backing) {
                auto       &out     = *static_cast<DurableLocations *>(raw);
                const auto &backing = *page->primary->frame.backing;
                out.emplace(pid, std::pair{backing.address, backing.length});
            }
        });
    return result;
}

void expect_untouched_locations(const DurableLocations &before, const DurableLocations &after)
{
    size_t reused  = 0;
    size_t changed = 0;
    for (const auto &[pid, location] : before) {
        auto current = after.find(pid);
        if (current != after.end() && current->second == location) {
            ++reused;
        }
        else {
            ++changed;
        }
    }
    EXPECT_GT(reused, 0U);
    EXPECT_GT(changed, 0U);
    EXPECT_LT(changed, before.size());
}
} // namespace

TEST(IncrementalCheckpoint, SecondCheckpointWithoutChangesWritesNothing)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t first = t.last_snapshot_pages_written();
    EXPECT_GT(first, 1U); // a multi-page tree: everything was dirty

    // No mutations between snapshots -> nothing should be rewritten.
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_pages_written(), 0U);
}

TEST(IncrementalCheckpoint, SingleKeyEditRewritesOnlyItsPath)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t total = t.last_snapshot_pages_written();
    ASSERT_GT(total, 4U); // several leaves + inner level(s)

    // Touch exactly one key, flush, snapshot again.
    uint64_t slot = t.contiguous_slot() + 1;
    ASSERT_TRUE(t.apply(slot, put_one(key(7), "updated")).ok());
    t.force_advance_slot(slot);
    ASSERT_TRUE(t.flush().ok());
    oracle[key(7)] = "updated";
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    uint64_t rewritten = t.last_snapshot_pages_written();
    EXPECT_GE(rewritten, 1U);    // the touched leaf was folded + written
    EXPECT_LT(rewritten, total); // unchanged leaves/inners were retained
}

TEST(IncrementalCheckpoint, UnchangedCheckpointPreservesExactPageLocations)
{
    MemPageStore store(1);
    Config       options;
    options.page_store       = &store;
    options.leaf_split_bytes = 160;
    options.frame_bytes      = 4096;
    Crowdbtree                         tree(options);
    std::map<std::string, std::string> oracle;
    fill(&tree, 200, &oracle);
    ASSERT_TRUE(tree.snapshot().ok());
    const auto before = durable_locations(tree);
    ASSERT_GT(before.size(), 4U);
    ASSERT_TRUE(tree.snapshot().ok());
    EXPECT_EQ(durable_locations(tree), before);
    EXPECT_EQ(tree.last_snapshot_pages_written(), 0U);
    ASSERT_TRUE(tree.apply(tree.contiguous_slot() + 1, put_one(key(7), "updated")).ok());
    ASSERT_TRUE(tree.snapshot().ok());
    expect_untouched_locations(before, durable_locations(tree));
}

TEST(IncrementalCheckpoint, LargeTreeEditPreservesUntouchedPhysicalPages)
{
    MemPageStore store(1);
    Config       options;
    options.page_store       = &store;
    options.leaf_split_bytes = 160;
    options.frame_bytes      = 4096;
    Crowdbtree                         tree(options);
    std::map<std::string, std::string> oracle;
    fill(&tree, 4096, &oracle);
    const auto work          = RuntimeTreeAccess_for_tests::core(tree).publisher().work_metrics();
    const auto before_splits = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Checkpoint)]
                                                        [static_cast<size_t>(TreeWorkCounter::SplitOperations)];
    const auto started       = std::chrono::steady_clock::now();
    const auto initial       = tree.snapshot();
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    ASSERT_TRUE(initial.ok()) << initial.to_string();
    const auto windows = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Checkpoint)]
                                                  [static_cast<size_t>(TreeWorkCounter::SplitOperations)] -
                         before_splits;
    EXPECT_EQ(windows, 0U);
    RecordProperty("checkpoint_split_transactions", std::to_string(windows));
    RecordProperty("initial_checkpoint_ms", std::to_string(elapsed.count()));
    const auto before = durable_locations(tree);
    ASSERT_GT(before.size(), 1024U);
    ASSERT_TRUE(tree.apply(tree.contiguous_slot() + 1, put_one(key(7), "updated")).ok());
    const auto updated = tree.snapshot();
    ASSERT_TRUE(updated.ok()) << updated.to_string();
    expect_untouched_locations(before, durable_locations(tree));
    EXPECT_LT(tree.last_snapshot_pages_written(), before.size());
}

TEST(IncrementalCheckpoint, SpaceIsReusedAcrossManyCheckpoints)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;

    std::map<std::string, std::string> oracle;
    uint64_t                           early = 0;
    {
        Crowdbtree t(opt);
        fill(&t, 100, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok());

        // Rewrite the SAME small key set every round and snapshot. With durable-page
        // GC, freed extents from two snapshots ago are reused, so the file reaches a
        // steady size instead of growing ~linearly in the number of rounds.
        uint64_t slot = 100000;
        t.force_advance_slot(99999);
        for (int round = 0; round < 50; ++round) {
            for (int i : {1, 2, 3, 4, 5}) {
                std::string v = "r" + std::to_string(round);
                ASSERT_TRUE(t.apply(slot, put_one(key(i), v)).ok());
                ASSERT_TRUE(t.flush().ok());
                oracle[key(i)] = v;
                ++slot;
            }
            ASSERT_TRUE(t.snapshot(nullptr).ok());
            if (round == 9) {
                early = store.size();
            }
        }
        uint64_t late = store.size();
        ASSERT_GT(early, 0U);
        // Steady state: rewriting fixed-size pages reuses exactly-sized freed gaps,
        // so the file barely grows over the last 40 rounds (a small slack covers
        // two-generation retention / manifest jitter).
        EXPECT_LE(late, early + (static_cast<uint64_t>(8) * 4096U));
    }

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "key " << kv.first;
    }
}

TEST(IncrementalCheckpoint, ReopenAfterIncrementalSeesAllValues)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;

    std::map<std::string, std::string> oracle;
    {
        Crowdbtree t(opt);
        fill(&t, 200, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // snapshot 1: full image

        // Mutate a spread of keys across different leaves, then snapshot again.
        uint64_t slot = 100000;
        t.force_advance_slot(99999);
        for (int i : {3, 50, 99, 150, 199}) {
            ASSERT_TRUE(t.apply(slot, put_one(key(i), "v2_" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[key(i)] = "v2_" + std::to_string(i);
            ++slot;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());            // snapshot 2: incremental
        EXPECT_LT(t.last_snapshot_pages_written(), 200U); // not a full rewrite
    }

    // Reopen from the incremental snapshot: unchanged keys are read from the
    // first snapshot's region (retained addrs); mutated keys from the second.
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "key " << kv.first;
    }
}

TEST(IncrementalCheckpoint, SnapshotPagesTotalAccumulates)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowdbtree t(opt);
    t.init_metrics("s.0.g.0", "mem");

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    // First snapshot: all pages dirty.
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t first_written = t.last_snapshot_pages_written();
    uint64_t first_total   = t.stats().snapshot_pages_total;
    EXPECT_GT(first_written, 1U);
    EXPECT_EQ(first_total, first_written);

    // Second snapshot with no changes: per-call stays 0, cumulative persists.
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_pages_written(), 0U);
    EXPECT_EQ(t.stats().snapshot_pages_total, first_total);

    // Touch one key, flush, snapshot again: cumulative increases.
    ASSERT_TRUE(t.apply(t.contiguous_slot() + 1, put_one(key(7), "updated")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t third_written = t.last_snapshot_pages_written();
    EXPECT_GE(third_written, 1U);
    EXPECT_EQ(t.stats().snapshot_pages_total, first_total + third_written);
}
