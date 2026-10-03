// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/memtable/skip_list.h"

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
std::shared_ptr<const CellVersion> version(uint64_t slot, std::string value = "v", bool deleted = false)
{
    return std::make_shared<CellVersion>(encode_cell_buf(slot, deleted ? OpKind::kDelete : OpKind::kPut, Slice(value)),
                                         slot, static_cast<uint8_t>(deleted ? kFlagTombstone : 0));
}
} // namespace

TEST(SkipList, OrderedInsertionAndBounds)
{
    ConcurrentSkipList list;
    EXPECT_TRUE(list.empty());
    for (const auto *key : {"d", "b", "a", "c"}) {
        EXPECT_TRUE(list.upsert(key, version(1), 0));
    }
    EXPECT_EQ(list.count(), 4U);
    std::string keys;
    for (auto cur = list.cursor({}); cur.valid(); cur.advance()) {
        keys += cur.key().to_string();
    }
    EXPECT_EQ(keys, "abcd");
    EXPECT_EQ(list.cursor("b").key().to_string(), "c");
    EXPECT_EQ(list.cursor_from("b", true).key().to_string(), "b");
    EXPECT_EQ(list.cursor_reverse("b", true, false).key().to_string(), "a");
    EXPECT_EQ(list.cursor_reverse({}, false, false).key().to_string(), "d");
    EXPECT_EQ(list.find("missing"), nullptr);
}

TEST(SkipList, HighestSlotReplayAndTombstone)
{
    ConcurrentSkipList list;
    EXPECT_TRUE(list.upsert("x", version(5), 5));
    EXPECT_TRUE(list.upsert("x", version(8, "", true), 8));
    EXPECT_FALSE(list.upsert("x", version(3), 8));
    EXPECT_FALSE(list.upsert("x", version(8, "replay"), 8));
    EXPECT_EQ(list.find("x")->slot, 8U);
    EXPECT_EQ(list.find("x")->flags, kFlagTombstone);
    EXPECT_EQ(list.count(), 1U);
}

TEST(SkipList, SelectivePrefixRetentionInEitherArrivalOrder)
{
    for (const auto &slots : {
             std::vector<uint64_t>{2,   80,  102, 105},
             std::vector<uint64_t>{105, 102, 80,  2  }
    }) {
        ConcurrentSkipList list;
        for (auto slot : slots) {
            list.upsert("x", version(slot), 100);
        }
        EXPECT_EQ(list.find("x")->slot, 105U);
        EXPECT_FALSE(list.prefix_cursor(79).valid());
        EXPECT_EQ(list.prefix_cursor(100).cell_version()->slot, 80U);
        EXPECT_EQ(list.prefix_cursor(102).cell_version()->slot, 102U);
        EXPECT_EQ(list.prefix_cursor(105).cell_version()->slot, 105U);
        EXPECT_EQ(list.count(), 1U);
        list.prune("x", 105);
        EXPECT_FALSE(list.prefix_cursor(104).valid());
        EXPECT_EQ(list.approx_bytes(), 1U + kCellHeaderSize + 1U);
    }
}

TEST(SkipList, DelayedBoundCannotReintroduceRedundantHistory)
{
    ConcurrentSkipList list;
    list.upsert("x", version(80), 80);
    list.upsert("x", version(105), 105);
    EXPECT_FALSE(list.upsert("x", version(90), 0));
    EXPECT_FALSE(list.prefix_cursor(100).valid());
    EXPECT_TRUE(list.upsert("x", version(110), 0));
    EXPECT_EQ(list.prefix_cursor(105).cell_version()->slot, 105U);
}

TEST(SkipList, FutureTombstoneDoesNotSuppressEligibleValue)
{
    ConcurrentSkipList list;
    list.upsert("x", version(102, "", true), 100);
    list.upsert("x", version(2), 100);
    EXPECT_EQ(list.prefix_cursor(100).cell_version()->slot, 2U);
    EXPECT_EQ(list.find("x")->flags, kFlagTombstone);
}

TEST(SkipList, CursorKeepsOneCoherentCandidate)
{
    ConcurrentSkipList list;
    list.upsert("x", version(1, "old"), 1);
    auto cur = list.cursor({});
    list.upsert("x", version(2, "new"), 2);
    EXPECT_EQ(cur.cell_version()->slot, 1U);
    EXPECT_EQ(CellView{cur.cell_version()->cell.slice()}.value().to_string(), "old");
    EXPECT_EQ(list.find("x")->slot, 2U);
}

TEST(SkipList, ConcurrentInsertionUpdatesAndCollection)
{
    EpochManager             epoch;
    ConcurrentSkipList       list(&epoch);
    std::barrier             start(5);
    std::atomic<bool>        stop{false};
    std::vector<std::thread> writers;
    writers.reserve(4);
    for (int w = 0; w < 4; ++w) {
        writers.emplace_back([&, w] {
            start.arrive_and_wait();
            for (uint64_t i = 1; i <= 1000; ++i) {
                list.upsert("key" + std::to_string(i), version((i * 4) + w), UINT64_MAX);
                list.upsert("hot", version((i * 4) + w), UINT64_MAX);
            }
        });
    }
    start.arrive_and_wait();
    std::thread reader([&] {
        while (!stop.load()) {
            auto        guard = epoch.enter();
            std::string previous;
            for (auto cur = list.cursor({}); cur.valid(); cur.advance()) {
                EXPECT_LT(previous, cur.key().to_string());
                previous = cur.key().to_string();
                EXPECT_EQ(CellView{cur.cell_version()->cell.slice()}.slot(), cur.cell_version()->slot);
            }
        }
    });
    std::thread collector([&] {
        while (!stop.load()) {
            epoch.try_reclaim();
            std::this_thread::yield();
        }
    });
    for (auto &writer : writers) {
        writer.join();
    }
    stop.store(true);
    reader.join();
    collector.join();
    EXPECT_EQ(list.count(), 1001U);
    EXPECT_EQ(list.find("hot")->slot, 4003U);
    EXPECT_EQ(list.approx_bytes(), (1001U * (kCellHeaderSize + 1)) + 3 + 5893);
}

TEST(SkipList, BorrowedPayloadSurvivesSourceDestruction)
{
    EpochManager       epoch;
    auto               guard = epoch.enter();
    const CellVersion *borrowed;
    {
        ConcurrentSkipList list(&epoch);
        list.upsert("x", version(1, "value"), 0);
        borrowed = list.find("x");
    }
    EXPECT_EQ(epoch.try_reclaim(), 0U);
    EXPECT_EQ(CellView{borrowed->cell.slice()}.value().to_string(), "value");
    guard = {};
    EXPECT_GT(epoch.try_reclaim(), 0U);
    EXPECT_EQ(epoch.pending_retired(), 0U);
}

#ifdef CROWDB_TREE_TEST_UTIL
#    include <future>
#    include <semaphore>

TEST(SkipList, PausedInsertionDoesNotOwnOtherKeysMutation)
{
    using Point = ConcurrentSkipList::PausePoint;
    for (auto point : {Point::kBeforeSearch, Point::kAfterLevelZero}) {
        struct Pause
        {
            Point                 point;
            std::binary_semaphore entered{0};
            std::binary_semaphore resume{0};
        } pause{.point = point};

        ConcurrentSkipList list;
        list.set_hook_for_tests(&pause, [](void *context, Point current, Slice key) {
            auto &state = *static_cast<Pause *>(context);
            if (current == state.point && key.compare("paused") == 0) {
                state.entered.release();
                state.resume.acquire();
            }
        });
        std::thread paused([&] { list.upsert("paused", version(1), 0); });
        pause.entered.acquire();
        auto       independent = std::async(std::launch::async, [&] { return list.upsert("other", version(2), 0); });
        const auto progress    = independent.wait_for(std::chrono::seconds(5));
        EXPECT_EQ(progress, std::future_status::ready);
        if (progress == std::future_status::ready) {
            EXPECT_TRUE(independent.get());
            EXPECT_EQ(list.find("other")->slot, 2U);
        }
        pause.resume.release();
        paused.join();
    }
}

TEST(SkipList, RetriedLowerInsertionUsesTheWinningPruningBound)
{
    for (uint64_t bound : {100U, 105U}) {
        ConcurrentSkipList list;
        list.upsert("x", version(80), 0);
        list.upsert("x", version(105), 0);

        struct Pause
        {
            std::atomic<bool>     once{true};
            std::binary_semaphore entered{0};
            std::binary_semaphore resume{0};
        } pause;

        list.set_hook_for_tests(&pause, [](void *context, ConcurrentSkipList::PausePoint point, Slice) {
            auto &state = *static_cast<Pause *>(context);
            if (point == ConcurrentSkipList::PausePoint::kBeforeVersionCas && state.once.exchange(false)) {
                state.entered.release();
                state.resume.acquire();
            }
        });
        MutationStats stats{.measure_copy = true};
        auto writer = std::async(std::launch::async, [&] { return list.upsert("x", version(102), 0, &stats); });
        pause.entered.acquire();
        list.prune("x", bound);
        pause.resume.release();
        EXPECT_EQ(writer.get(), bound == 100);
        EXPECT_EQ(list.find("x")->slot, 105U);
        EXPECT_EQ(list.count(), 1U);
        EXPECT_EQ(stats.keep, bound == 100 ? 1U : 0U);
        EXPECT_GE(stats.cas_retries, 1U);
        EXPECT_EQ(stats.copy_count, stats.cas_retries + 1);
        EXPECT_GE(stats.copy_ns, stats.copy_max_ns);
        if (bound == 100) {
            EXPECT_EQ(list.prefix_cursor(100).cell_version()->slot, 80U);
            EXPECT_EQ(list.prefix_cursor(102).cell_version()->slot, 102U);
        }
        else {
            EXPECT_FALSE(list.prefix_cursor(102).valid());
        }
    }
}
#endif
