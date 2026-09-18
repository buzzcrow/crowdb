// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT12: page split & merge integration tests.
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}
} // namespace

TEST(SplitMerge, SplitGrowsMultiLevelTree)
{
    Config opt;
    opt.max_delta_len    = 1;   // consolidate aggressively
    opt.leaf_split_bytes = 200; // small leaves -> force splits
    Crowdbtree t(opt);

    const int N = 300;
    for (int i = 0; i < N; ++i) {
        uint64_t s = i + 1;
        ASSERT_TRUE(t.apply(s, put_one(make_key(i), "value-payload-" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // The tree must have grown beyond a single leaf.
    EXPECT_GT(t.height(), 1);
    EXPECT_GT(t.leaf_count(), 1U);

    // All keys present and readable.
    for (int i = 0; i < N; ++i) {
        std::string v;
        uint64_t    slot;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &slot, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, "value-payload-" + std::to_string(i));
    }

    // Snapshot is globally key-sorted and complete.
    auto snap = t.snapshot_view();
    ASSERT_EQ(snap->size(), static_cast<size_t>(N));
    for (size_t i = 1; i < snap->size(); ++i) {
        EXPECT_LT(snap->entries()[i - 1].key, snap->entries()[i].key);
    }
}

TEST(SplitMerge, SimilarKeysKeepSplittingOneHotRange)
{
    Config opt;
    opt.max_delta_len    = 0;
    opt.leaf_split_bytes = 512;
    opt.leaf_merge_bytes = 64;
    Crowdbtree t(opt);

    // Keep every new key in one narrow lexical interval. Each flush has to
    // route through the repeatedly splitting hot range instead of distributing
    // the writes across the tree.
    std::map<std::string, std::string> oracle;
    uint64_t                           slot          = 0;
    size_t                             leaves_before = t.leaf_count();
    for (uint64_t round = 0; round < 40; ++round) {
        for (uint64_t offset = 0; offset < 4; ++offset) {
            const uint64_t       sequence = (offset * 40) + round;
            std::array<char, 32> key{};
            snprintf(key.data(), key.size(), "hot/500/%020llu", static_cast<unsigned long long>(sequence));
            const std::string value =
                "payload-" + std::to_string(round) + "-" + std::to_string(offset) + std::string(112, 'v');
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(key.data(), value)).ok());
            oracle[key.data()] = value;
        }
        ASSERT_TRUE(t.flush().ok());
        EXPECT_GT(t.leaf_count(), leaves_before) << "round=" << round;
        leaves_before = t.leaf_count();
    }

    EXPECT_GT(t.height(), 1);
    for (const auto &[key, expected] : oracle) {
        std::string value;
        uint64_t    read_slot = 0;
        ASSERT_TRUE(t.get(Slice(key), &read_slot, &value)) << "missing " << key;
        EXPECT_EQ(value, expected) << "wrong value for " << key;
    }
}

TEST(SplitMerge, MergeAndRootCollapse)
{
    Config opt;
    opt.max_delta_len    = 0; // consolidate (and check merge) on every flush
    opt.leaf_split_bytes = 200;
    opt.leaf_merge_bytes = 60;
    Crowdbtree t(opt);

    const int N    = 200;
    uint64_t  slot = 0;
    for (int i = 0; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "payload" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);
    size_t leaves_before = t.leaf_count();
    EXPECT_GT(leaves_before, 1U);

    // Allow tombstone GC so deletes actually shrink leaves.
    t.set_gc_watermark(1000000, 1000000);
    // Delete all but the first two keys.
    for (int i = 2; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }

    // Tree shrank: fewer leaves, ideally collapsed back to a single-leaf root.
    EXPECT_LT(t.leaf_count(), leaves_before);
    EXPECT_EQ(t.height(), 1);

    // Surviving keys readable; deleted keys gone.
    std::string v;
    uint64_t    s;
    EXPECT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    EXPECT_TRUE(t.get(Slice(make_key(1)), &s, &v));
    for (int i = 2; i < N; ++i) {
        EXPECT_FALSE(t.get(Slice(make_key(i)), &s, &v)) << "should be deleted: " << make_key(i);
    }
    auto snap = t.snapshot_view();
    EXPECT_EQ(snap->size(), 2U);
}

// Regression (plan-tree #14c/#14d): a merged-away leaf/inner's own PID is
// orphaned (its mapping slot never gets a replacement store()) -- see
// Crowdbtree::retire_orphaned_page's doc comment. snapshot() discovers dirty
// pages/segments by scanning every mapping-table slot directly (no
// reachable-page tree walk), so a stale slot left pointing at a since-freed
// page is a use-after-free the moment a merge-heavy tree gets snapshotted.
TEST(SplitMerge, SnapshotSucceedsAfterHeavyMergeAndRootCollapse)
{
    MemPageStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 0; // consolidate (and check merge) on every flush
    opt.leaf_split_bytes = 200;
    opt.leaf_merge_bytes = 60;
    opt.inner_max_keys   = 4; // force inner splits/merges too, not just leaves
    Crowdbtree t(opt);

    const int N    = 200;
    uint64_t  slot = 0;
    for (int i = 0; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "payload" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);

    t.set_gc_watermark(1000000, 1000000);
    for (int i = 2; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    EXPECT_GT(t.leaf_count(), 0U); // sanity: still a valid tree after the merge storm

    ASSERT_TRUE(t.snapshot().ok());

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    std::string v;
    uint64_t    s;
    EXPECT_TRUE(t2->get(Slice(make_key(0)), &s, &v));
    EXPECT_TRUE(t2->get(Slice(make_key(1)), &s, &v));
    for (int i = 2; i < N; ++i) {
        EXPECT_FALSE(t2->get(Slice(make_key(i)), &s, &v)) << "should be deleted: " << make_key(i);
    }
}

TEST(SplitMerge, LargeFlushSpanningLeavesSplitsMidFlush)
{
    // Regression: one flush() drains keys spanning many existing leaves and
    // triggers splits mid-flush. Each per-leaf group must be routed against the
    // CURRENT tree (after prior groups' SMOs), not a routing snapshot captured
    // before the flush began. Otherwise later keys land in a just-split leaf.
    Config opt;
    opt.max_delta_len    = 0;   // consolidate on every flush
    opt.leaf_split_bytes = 200; // small leaves -> splits during the big flush
    opt.leaf_merge_bytes = 40;
    // Keep auto-flush from firing so we control exactly when the big flush runs.
    opt.memtable_flush_bytes   = 1ULL << 40;
    opt.memtable_flush_entries = 1U << 30;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;

    // Phase A: build a multi-level tree with incremental flushes.
    const int N = 400;
    for (int i = 0; i < N; i += 2) { // even keys first
        ++slot;
        std::string val = "a" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);
    ASSERT_GT(t.leaf_count(), 2U);

    // Phase B: stage many keys interleaved across the whole keyspace WITHOUT
    // flushing, so a single flush() drains a set that spans every existing leaf
    // and grows several of them past the split threshold in one pass.
    for (int i = 1; i < N; i += 2) { // odd keys interleave between existing keys
        ++slot;
        std::string val = "b" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
    }
    // Also overwrite a spread of even keys so groups are non-trivial.
    for (int i = 0; i < N; i += 8) {
        ++slot;
        std::string val = "c" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
    }
    // The single flush that exercises mid-flush re-routing.
    ASSERT_TRUE(t.flush().ok());

    // Every key must be present and correct.
    for (int i = 0; i < N; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &s, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, oracle[make_key(i)]) << "wrong value " << make_key(i);
    }

    // Snapshot is globally key-sorted and complete (no entry lost to a stale
    // route into a split leaf).
    auto snap = t.snapshot_view();
    ASSERT_EQ(snap->size(), oracle.size());
    for (size_t i = 1; i < snap->size(); ++i) {
        EXPECT_LT(snap->entries()[i - 1].key, snap->entries()[i].key);
    }
}

TEST(SplitMerge, ParityWithOracleUnderSplits)
{
    Config opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 150;
    opt.leaf_merge_bytes = 40;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(12345);
    uint64_t                           slot = 0;
    for (int round = 0; round < 2000; ++round) {
        int         k   = static_cast<int>(rng() % 150);
        std::string key = make_key(k);
        ++slot;
        if (rng() % 4 == 0) {
            ASSERT_TRUE(t.apply(slot, del_one(key)).ok());
            oracle.erase(key);
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
            oracle[key] = val;
        }
        if (round % 7 == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    // compare every key.
    for (int k = 0; k < 150; ++k) {
        std::string key = make_key(k);
        std::string v;
        uint64_t    s;
        bool        found = t.get(Slice(key), &s, &v);
        auto        it    = oracle.find(key);
        if (it == oracle.end()) {
            EXPECT_FALSE(found) << "extra key " << key;
        }
        else {
            ASSERT_TRUE(found) << "missing key " << key;
            EXPECT_EQ(v, it->second) << "value mismatch " << key;
        }
    }
}

// Regression: consolidation folds an entire delta chain into one leaf, which
// can be many times larger than leaf_split_bytes (up to max_delta_bytes).
// maybe_split_or_merge_locked must split iteratively until every child fits
// under the threshold — a single halving leaves oversized leaves when the
// consolidated leaf is > 2x the split threshold.
TEST(SplitMerge, ConsolidationSplitsIterativelyToThreshold)
{
    Config opt;
    opt.max_delta_len    = 0;   // consolidate on every flush
    opt.leaf_split_bytes = 200; // small threshold so many splits are needed
    opt.leaf_merge_bytes = 50;  // well below split to avoid merge-after-split
    // Prevent auto-freeze so we control exactly when each flush runs.
    opt.memtable_flush_bytes   = 1ULL << 40;
    opt.memtable_flush_entries = 1U << 30;
    Crowdbtree t(opt);

    // Phase 1: write the highest key first and flush. With max_delta_len=0,
    // the delta immediately consolidates into the base leaf, giving it
    // high_key="key00999" so that subsequent (smaller) keys are grouped by
    // sort-aware descent instead of published one-per-key (which happens
    // when the base leaf is empty and high_key is an empty Slice).
    ASSERT_TRUE(t.apply(1, put_one("key00999", "first")).ok());
    ASSERT_TRUE(t.flush().ok());

    // Phase 2: write 200 keys in one batch, all < "key00999". They're
    // grouped into one delta and immediately consolidated (max_delta_len=0).
    // The consolidated leaf has ~201 entries (~7000 bytes = ~35x the
    // 200-byte split threshold).
    Batch big;
    for (int i = 0; i < 200; ++i) {
        big.ops.push_back({.key = make_key(i), .kind = OpKind::kPut, .value = "val-" + std::to_string(i)});
    }
    ASSERT_TRUE(t.apply(2, big).ok());
    ASSERT_TRUE(t.flush().ok());

    // With iterative splitting, ~7000 bytes / 200-byte threshold => ~35
    // leaves. With the bug (single split), only 2 leaves of ~3500 bytes
    // each remain.
    EXPECT_GE(t.leaf_count(), 10U);

    // All keys must be present and readable.
    for (int i = 0; i < 200; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &s, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, "val-" + std::to_string(i));
    }
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice("key00999"), &s, &v));
    EXPECT_EQ(v, "first");
}

// Count the inner pages reachable from the root (test helper).
static size_t inner_count_walk(Crowdbtree &t)
{
    std::function<size_t(uint64_t)> rec = [&](uint64_t page_id) -> size_t {
        PageBase *head = t.mapping().get_resident(page_id);
        if (head == nullptr) {
            return 0;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type == page_type::kLeafBase) {
            return 0;
        }
        size_t n = 1;
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            n += rec(c);
        }
        return n;
    };
    return rec(t.root_page_id());
}

// O(1) atomic leaf/inner counters must match the tree walk after splits.
TEST(SplitMerge, LeafInnerCountParityAfterSplits)
{
    Config opt;
    opt.max_delta_len      = 1;   // consolidate aggressively
    opt.leaf_split_bytes   = 200; // small leaves -> force splits
    opt.max_memtable_count = 6;
    Crowdbtree t(opt);

    // Fresh tree: 1 leaf, 0 inner.
    EXPECT_EQ(t.leaf_count_atomic(), 1U);
    EXPECT_EQ(t.inner_count_atomic(), 0U);

    const int N = 300;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "val-" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    EXPECT_GT(t.height(), 1);
    EXPECT_EQ(t.leaf_count_atomic(), t.leaf_count());
    EXPECT_EQ(t.inner_count_atomic(), inner_count_walk(t));
}

// O(1) atomic leaf/inner counters must match the tree walk after merges
// and root collapse.
TEST(SplitMerge, LeafInnerCountParityAfterMerges)
{
    Config opt;
    opt.max_delta_len      = 1;
    opt.leaf_split_bytes   = 200;
    opt.leaf_merge_bytes   = 40;
    opt.max_memtable_count = 6;
    Crowdbtree t(opt);

    // Build a multi-level tree.
    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;
    for (int i = 0; i < 200; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "v" + std::to_string(slot))).ok());
        oracle[make_key(i)] = "v" + std::to_string(slot);
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);

    // Delete half to trigger merges + root collapse.
    int deleted = 0;
    for (auto it = oracle.begin(); it != oracle.end() && deleted < 150; ++it, ++deleted) {
        ++slot;
        Batch del{{{.key = it->first, .kind = OpKind::kDelete, .value = ""}}};
        ASSERT_TRUE(t.apply(slot, del).ok());
        ASSERT_TRUE(t.flush().ok());
    }

    // Counters must match the walk regardless of how many merges/collapses
    // happened.
    EXPECT_EQ(t.leaf_count_atomic(), t.leaf_count());
    EXPECT_EQ(t.inner_count_atomic(), inner_count_walk(t));
}

TEST(SplitMerge, SplitSharedMemtableSurvivesOrdinaryFlush)
{
    Config opt;
    opt.memtable_flush_bytes   = 1ULL << 40;
    opt.memtable_flush_entries = 1U << 30;
    Crowdbtree t(opt);

    ASSERT_TRUE(t.apply(1, put_one("before", "shared")).ok());
    uint64_t generation       = 0;
    uint64_t journal_frontier = 0;
    ASSERT_TRUE(t.begin_split_memtable_view(&generation, &journal_frontier).ok());
    ASSERT_NE(generation, 0U);
    EXPECT_EQ(journal_frontier, 1U);
    ASSERT_TRUE(t.apply(2, put_one("after", "private")).ok());

    ASSERT_TRUE(t.flush().ok());
    uint64_t    slot = 0;
    std::string value;
    EXPECT_TRUE(t.get(Slice("before"), &slot, &value));
    EXPECT_EQ(value, "shared");
    EXPECT_TRUE(t.get(Slice("after"), &slot, &value));
    EXPECT_EQ(value, "private");

    EXPECT_FALSE(t.release_split_memtable_view(generation + 1).ok());
    ASSERT_TRUE(t.release_split_memtable_view(generation).ok());
}

TEST(SplitMerge, SplitSharedMemtablePublishesTwoRangeTreesInBulk)
{
    Config source_opt;
    source_opt.memtable_flush_bytes   = 1ULL << 40;
    source_opt.memtable_flush_entries = 1U << 30;
    Crowdbtree source(source_opt);
    ASSERT_TRUE(source.apply(1, put_one("apple", "left")).ok());
    ASSERT_TRUE(source.apply(2, put_one("zebra", "right")).ok());

    uint64_t generation       = 0;
    uint64_t journal_frontier = 0;
    ASSERT_TRUE(source.begin_split_memtable_view(&generation, &journal_frontier).ok());
    EXPECT_EQ(journal_frontier, 2U);
    ASSERT_TRUE(source.apply(3, put_one("kiwi", "post-view")).ok());
    Config left_opt;
    left_opt.key_range = KeyRange::bounded(std::nullopt, std::string("m"));
    Config right_opt;
    right_opt.key_range = KeyRange::bounded(std::string("m"), std::nullopt);
    Crowdbtree left(left_opt);
    Crowdbtree right(right_opt);
    ASSERT_TRUE(source.publish_split_memtable_view(generation, journal_frontier, left, left_opt.key_range).ok());
    ASSERT_TRUE(source.publish_split_memtable_view(generation, journal_frontier, right, right_opt.key_range).ok());

    uint64_t    slot = 0;
    std::string value;
    EXPECT_TRUE(left.get(Slice("apple"), &slot, &value));
    EXPECT_EQ(value, "left");
    EXPECT_FALSE(left.get(Slice("zebra"), &slot, &value));
    EXPECT_TRUE(right.get(Slice("zebra"), &slot, &value));
    EXPECT_EQ(value, "right");
    EXPECT_FALSE(right.get(Slice("apple"), &slot, &value));
    EXPECT_FALSE(left.get(Slice("kiwi"), &slot, &value));
    EXPECT_EQ(left.last_applied_slot(), 2U);
    EXPECT_EQ(right.last_applied_slot(), 2U);
    ASSERT_TRUE(left.apply(3, put_one("lemon", "replayed-after-frontier")).ok());
    EXPECT_TRUE(left.get(Slice("lemon"), &slot, &value));
    EXPECT_EQ(value, "replayed-after-frontier");
    EXPECT_EQ(left.contiguous_slot(), 3U);
    EXPECT_TRUE(source.get(Slice("apple"), &slot, &value));
    ASSERT_TRUE(source.release_split_memtable_view(generation).ok());
}

TEST(SplitMerge, SplitOverlayServesBeforeSourceMemtableSeal)
{
    Config source_opt;
    source_opt.memtable_flush_bytes   = 1ULL << 40;
    source_opt.memtable_flush_entries = 1U << 30;
    Crowdbtree source(source_opt);
    ASSERT_TRUE(source.apply(1, put_one("apple", "left-before")).ok());
    ASSERT_TRUE(source.apply(2, put_one("zebra", "right-before")).ok());

    Config left_opt;
    left_opt.key_range = KeyRange::bounded(std::nullopt, std::string("m"));
    Config right_opt;
    right_opt.key_range = KeyRange::bounded(std::string("m"), std::nullopt);
    Crowdbtree left(left_opt);
    Crowdbtree right(right_opt);
    ASSERT_TRUE(left.install_split_memtable_overlay(source, 2).ok());
    ASSERT_TRUE(right.install_split_memtable_overlay(source, 2).ok());

    uint64_t    slot = 0;
    std::string value;
    EXPECT_TRUE(left.get(Slice("apple"), &slot, &value));
    EXPECT_EQ(value, "left-before");
    EXPECT_TRUE(right.get(Slice("zebra"), &slot, &value));
    EXPECT_EQ(value, "right-before");
    ASSERT_TRUE(right.apply(3, put_one("zebra", "right-after")).ok());

    uint64_t generation       = 0;
    uint64_t journal_frontier = 0;
    ASSERT_TRUE(source.begin_split_memtable_view(&generation, &journal_frontier).ok());
    EXPECT_EQ(journal_frontier, 2U);
    ASSERT_TRUE(source.publish_split_memtable_view(generation, 2, left, left_opt.key_range).ok());
    ASSERT_TRUE(source.publish_split_memtable_view(generation, 2, right, right_opt.key_range).ok());
    ASSERT_TRUE(left.clear_split_memtable_overlay(source).ok());
    ASSERT_TRUE(right.clear_split_memtable_overlay(source).ok());
    ASSERT_TRUE(source.release_split_memtable_view(generation).ok());

    EXPECT_TRUE(left.get(Slice("apple"), &slot, &value));
    EXPECT_EQ(value, "left-before");
    EXPECT_TRUE(right.get(Slice("zebra"), &slot, &value));
    EXPECT_EQ(slot, 3U);
    EXPECT_EQ(value, "right-after");
}

TEST(SplitMerge, RepeatedSharedViewsKeepBothReplayFrontiersContinuous)
{
    constexpr uint64_t kRounds   = 32;
    constexpr uint64_t kWrites   = 20'000;
    constexpr uint64_t kReads    = 20'000;
    constexpr uint64_t kSeedKeys = 4'096;
    Config             source_opt;
    source_opt.memtable_flush_bytes   = 1ULL << 10;
    source_opt.memtable_flush_entries = 32;
    source_opt.max_memtable_count     = 1'024;
    Crowdbtree source(source_opt);
    source.init_metrics("split-mixed", "");
    for (uint64_t slot = 1; slot <= kSeedKeys; ++slot) {
        ASSERT_TRUE(source.apply(slot, put_one("seed-" + std::to_string(slot), "tree-resident")).ok());
    }
    ASSERT_TRUE(source.flush().ok());
    std::atomic<bool>     writer_failed{false};
    std::atomic<bool>     reader_failed{false};
    std::atomic<bool>     writer_done{false};
    std::atomic<bool>     flush_failed{false};
    std::atomic<uint64_t> written{kSeedKeys};
    std::atomic<uint64_t> completed_splits{0};
    std::vector<uint64_t> write_latencies_us;
    std::vector<uint64_t> read_latencies_us;
    write_latencies_us.reserve(kWrites);
    read_latencies_us.reserve(kReads);
    std::thread writer([&] {
        for (uint64_t write = 1; write <= kWrites; ++write) {
            const uint64_t    slot   = kSeedKeys + write;
            const std::string prefix = write % 2 == 0 ? "left-" : "right-";
            written.store(slot, std::memory_order_release);
            const auto started = std::chrono::steady_clock::now();
            if (!source.apply(slot, put_one(prefix + std::to_string(slot), "injected")).ok()) {
                writer_failed.store(true);
                writer_done.store(true, std::memory_order_release);
                return;
            }
            const auto elapsed = std::chrono::steady_clock::now() - started;
            write_latencies_us.push_back(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
        }
        writer_done.store(true, std::memory_order_release);
    });
    std::thread reader([&] {
        for (uint64_t read = 0; read < kReads || completed_splits.load(std::memory_order_acquire) < kRounds; ++read) {
            uint64_t          revision = 0;
            std::string       value;
            const std::string key     = "seed-" + std::to_string(read % kSeedKeys + 1);
            const auto        started = std::chrono::steady_clock::now();
            if (!source.get(Slice(key), &revision, &value) || value != "tree-resident") {
                reader_failed.store(true);
                return;
            }
            read_latencies_us.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started)
                    .count());
        }
    });
    std::thread flusher([&] {
        while (!writer_done.load(std::memory_order_acquire)) {
            if (!source.flush().ok()) {
                flush_failed.store(true);
                return;
            }
            std::this_thread::yield();
        }
    });

    while (written.load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
    }
    uint64_t previous_frontier = 0;
    for (uint64_t round = 0; round < kRounds; ++round) {
        uint64_t generation       = 0;
        uint64_t journal_frontier = 0;
        ASSERT_TRUE(source.begin_split_memtable_view(&generation, &journal_frontier).ok());
        EXPECT_GE(journal_frontier, previous_frontier);
        EXPECT_LE(journal_frontier, written.load(std::memory_order_acquire));

        Config left_opt;
        left_opt.key_range = KeyRange::bounded(std::nullopt, std::string("m"));
        Config right_opt;
        right_opt.key_range = KeyRange::bounded(std::string("m"), std::nullopt);
        Crowdbtree left(left_opt);
        Crowdbtree right(right_opt);
        ASSERT_TRUE(source.publish_split_memtable_view(generation, journal_frontier, left, left_opt.key_range).ok());
        ASSERT_TRUE(source.publish_split_memtable_view(generation, journal_frontier, right, right_opt.key_range).ok());

        const uint64_t replay_slot = journal_frontier + 1;
        ASSERT_TRUE(left.apply(replay_slot, put_one("left-replay-" + std::to_string(round), "replayed")).ok());
        ASSERT_TRUE(right.apply(replay_slot, put_one("right-replay-" + std::to_string(round), "replayed")).ok());
        EXPECT_EQ(left.contiguous_slot(), replay_slot);
        EXPECT_EQ(right.contiguous_slot(), replay_slot);
        ASSERT_TRUE(source.release_split_memtable_view(generation).ok());
        previous_frontier = journal_frontier;
        completed_splits.store(round + 1, std::memory_order_release);
    }
    writer.join();
    reader.join();
    flusher.join();
    EXPECT_FALSE(writer_failed.load());
    EXPECT_FALSE(reader_failed.load());
    EXPECT_FALSE(flush_failed.load());
    EXPECT_EQ(written.load(), kSeedKeys + kWrites);
    ASSERT_EQ(write_latencies_us.size(), kWrites);
    std::sort(write_latencies_us.begin(), write_latencies_us.end());
    const size_t p99_index = write_latencies_us.size() * 99 / 100;
    EXPECT_LT(write_latencies_us[p99_index], 10'000U);
    EXPECT_LT(write_latencies_us.back(), 100'000U);
    ASSERT_GE(read_latencies_us.size(), kReads);
    std::sort(read_latencies_us.begin(), read_latencies_us.end());
    const size_t read_p99_index = read_latencies_us.size() * 99 / 100;
    EXPECT_LT(read_latencies_us[read_p99_index], 10'000U);
    EXPECT_LT(read_latencies_us.back(), 100'000U);
    const std::string metrics = source.flush_metrics_str(1.0, "test");
    EXPECT_NE(metrics.find("split-mixed.l1.get.c"), std::string::npos);
}
