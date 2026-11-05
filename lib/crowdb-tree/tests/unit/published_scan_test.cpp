// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_read_test_support.h"

#include <algorithm>

namespace crowdb::tree
{
using namespace test_read;

namespace
{
std::shared_ptr<const PublishedTreeState> test_branch()
{
    auto result                                 = std::make_shared<PublishedTreeState>();
    auto root                                   = std::make_shared<PublishedPage>();
    auto index                                  = std::make_shared<InnerPageState>();
    index->separators                           = {"m", "t"};
    index->children                             = {1, 2, 3};
    root->level                                 = 1;
    root->inner                                 = index;
    auto                                 left   = published(page(
        {
            {.key = "",  .value = "empty"},
            {.key = "a", .value = "a"    },
            {.key = "l", .value = "l"    }
    },
        {.upper = "m"}));
    auto                                 middle = published(page(
        {
            {.key = "m", .value = "m"},
            {.key = "n", .value = "n"}
    },
        {.lower = "m", .upper = "t"}));
    auto                                 right  = published(page(
        {
            {.key = "t", .value = "t"},
            {.key = "z", .value = "z"}
    },
        {.lower = "t"}));
    const std::array<PageReplacement, 4> changes{
        {
         {.page_id = 0, .expected = {}, .replacement = root},
         {.page_id = 1, .expected = {}, .replacement = left},
         {.page_id = 2, .expected = {}, .replacement = middle},
         {.page_id = 3, .expected = {}, .replacement = right},
         }
    };
    MappingView::CopyStats copied;
    result->mapping      = result->mapping.replace(changes, copied);
    result->root_page_id = 0;
    result->sources      = std::make_shared<PublishedSources>();
    return result;
}

std::vector<std::string> keys(const std::vector<PublishedScanEntry> &entries)
{
    std::vector<std::string> result;
    result.reserve(entries.size());
    for (const auto &entry : entries) {
        result.push_back(Slice(entry.key).to_string());
    }
    return result;
}
} // namespace

TEST(PublishedScan, BothDirectionsMergeTwoMemTablesOverlayAndBase)
{
    auto first  = std::make_shared<MemTable>();
    auto second = std::make_shared<MemTable>();
    ASSERT_TRUE(first->upsert("b", 12, encode_cell_buf(12, OpKind::kPut, "first")));
    ASSERT_TRUE(first->upsert("c", 2, encode_cell_buf(2, OpKind::kPut, "covered")));
    ASSERT_TRUE(second->upsert("b", 13, encode_cell_buf(13, OpKind::kPut, "second")));
    ASSERT_TRUE(second->upsert("d", 14, encode_cell_buf(14, OpKind::kDelete, {}), kFlagTombstone));
    auto leaf = std::make_shared<PageState>(*page({
        {.key = "b", .slot = 10, .value = "overlay"},
        {.key = "c", .slot = 11, .value = "live"   }
    }));
    leaf->bases.push_back({
        .identity = 1,
        .resident =
            base({{.key = "a", .value = "base"}, {.key = "b", .value = "old"}, {.key = "d", .value = "removed"}}
            )
    });
    for (bool reverse : {false, true}) {
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(view(leaf, {first, second}, 8), {}, {}, reverse,
                                          {.max_work = 3, .max_entries = 1}, &scan)
                        .ok());
        auto entries = collect(scan);
        if (reverse) {
            std::reverse(entries.begin(), entries.end());
        }
        EXPECT_EQ(keys(entries), (std::vector<std::string>{"a", "b", "c"}));
        ASSERT_EQ(entries.size(), 3U);
        EXPECT_EQ(entries[1].cell.slot, 13U);
        EXPECT_EQ(entries[1].cell.value.to_string(), "second");
        EXPECT_EQ(entries[2].cell.value.to_string(), "live");
    }
}

TEST(PublishedScan, SeparatorBoundsAreHalfOpenInBothDirections)
{
    const auto snapshot = test_branch();
    for (bool reverse : {false, true}) {
        for (const auto &range : std::array<PageKeyRange, 4>{
                 {{}, {.upper = "m"}, {.lower = "m", .upper = "t"}, {.lower = "t"}}
        }) {
            std::shared_ptr<PublishedScan> scan;
            ASSERT_TRUE(
                PublishedScan::create(snapshot, {}, range, reverse, {.max_work = 1, .max_entries = 1}, &scan).ok());
            auto                     actual = keys(collect(scan));
            std::vector<std::string> expected;
            for (const auto *key : {"", "a", "l", "m", "n", "t", "z"}) {
                if (range.contains(key)) {
                    expected.emplace_back(key);
                }
            }
            if (reverse) {
                std::reverse(expected.begin(), expected.end());
            }
            EXPECT_EQ(actual, expected);
        }
    }
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(snapshot, {}, "m", {}, &get).ok());
    complete_inline(get->sender());
    EXPECT_EQ(get->result().cell.value.to_string(), "m");
}

TEST(PublishedScan, CoveredEntriesAndTombstonesYieldBoundedEmptyWindows)
{
    auto           table = std::make_shared<MemTable>();
    auto           leaf  = std::make_shared<PageState>();
    OverlayBuilder builder({});
    for (size_t i = 0; i < 80; ++i) {
        const auto key = "k" + std::to_string(100 + i);
        ASSERT_TRUE(table->upsert(Slice(key), 2, encode_cell_buf(2, OpKind::kPut, "covered")));
        auto cell = std::make_shared<CellVersion>(encode_cell_buf(3, OpKind::kDelete, {}), 3, kFlagTombstone);
        ASSERT_TRUE(builder.append(Slice(key), std::move(cell)).ok());
    }
    std::shared_ptr<const OverlayBatch> overlay;
    ASSERT_TRUE(builder.finish(1, &overlay).ok());
    leaf->overlays.push_back(std::move(overlay));
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(
        PublishedScan::create(view(leaf, {table}, 2), {}, {}, false, {.max_work = 4, .max_entries = 1}, &scan).ok());
    size_t windows = 0;
    do {
        complete_inline(scan->next_sender());
        EXPECT_TRUE(scan->result().entries.empty());
        EXPECT_LE(scan->result().work, 4U);
        ASSERT_LT(++windows, 100U);
    } while (!scan->result().done);
    EXPECT_GT(windows, 20U);
}

TEST(PublishedScan, ContinuationKeepsCapturedMappingAndOwners)
{
    auto                           original = test_branch();
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(original, {}, {}, false, {.max_entries = 1}, &scan).ok());
    complete_inline(scan->next_sender());
    ASSERT_EQ(scan->result().entries.size(), 1U);
    auto kept = scan->result().entries[0];
    original.reset();
    EXPECT_EQ(keys(collect(scan)), (std::vector<std::string>{"a", "l", "m", "n", "t", "z"}));
    scan.reset();
    EXPECT_EQ(kept.cell.value.to_string(), "empty");
}

TEST(PublishedScan, ColdLateErrorTerminatesAndRemainsSticky)
{
    auto leaf = std::make_shared<PageState>();
    leaf->bases.push_back(cold(1));
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "a", .value = "disk"}
    });
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(view(leaf), loader, {}, false, {}, &scan).ok());
    Completion done;
    auto       operation = stdexec::connect(scan->next_sender(), Receiver{.done = &done});
    stdexec::start(operation);
    EXPECT_EQ(done.values.load(), 0U);
    loader->finish(Status::io_error("cold read failed"));
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(scan->result().status.ok());
    Completion again;
    auto       next = stdexec::connect(scan->next_sender(), Receiver{.done = &again});
    stdexec::start(next);
    EXPECT_EQ(again.errors.load(), 1U);
    EXPECT_EQ(loader->calls, 1U);
}

TEST(PublishedScan, ByteWindowRetainsPendingCandidateAndReportsOversizedRecord)
{
    const auto                     snapshot = view(page({
        {.key = "a", .value = "123"},
        {.key = "b", .value = "456"}
    }));
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(snapshot, {}, {}, false, {.max_result_bytes = 5}, &scan).ok());
    EXPECT_EQ(keys(collect(scan)), (std::vector<std::string>{"a", "b"}));
    ASSERT_TRUE(PublishedScan::create(snapshot, {}, {}, false, {.max_result_bytes = 3}, &scan).ok());
    Completion done;
    auto       operation = stdexec::connect(scan->next_sender(), Receiver{.done = &done});
    stdexec::start(operation);
    EXPECT_EQ(done.values.load(), 1U);
    EXPECT_EQ(done.errors.load(), 0U);
    EXPECT_TRUE(scan->result().status.ok());
    EXPECT_TRUE(scan->result().byte_limit_reached);
    EXPECT_TRUE(scan->result().entries.empty());
    EXPECT_FALSE(scan->result().done);
}
} // namespace crowdb::tree
