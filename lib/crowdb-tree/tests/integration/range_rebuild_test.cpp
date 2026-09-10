// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/page_store.h"
#include "crowdb-tree/range_rebuild.h"

#include <gtest/gtest.h>

#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace crowdb::tree
{
namespace
{

std::map<std::string, std::string> live_entries(Crowdbtree &tree)
{
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    EXPECT_TRUE(tree.scan(Slice(), Slice(), Slice(), 0, 0, false, 0, &entries, &truncated).ok());
    std::map<std::string, std::string> result;
    for (auto &entry : entries) {
        result.emplace(std::move(entry.key), std::move(entry.value));
    }
    return result;
}

TEST(RangeRebuild, AdjacentChildrenHaveExactUnionAndEmptyIntersection)
{
    MemPageStore source_store(1);
    Options      source_options;
    source_options.page_store       = &source_store;
    source_options.frame_bytes      = 4096;
    source_options.leaf_split_bytes = 256;
    Crowdbtree source(source_options);
    for (uint64_t i = 0; i < 100; ++i) {
        const std::string key = "k" + std::to_string(1000 + i);
        ASSERT_TRUE(source.put(Slice(key), Slice("value" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(source.flush().ok());
    ASSERT_TRUE(source.snapshot().ok());
    const auto expected = live_entries(source);

    MemPageStore left_store(1);
    MemPageStore right_store(1);
    Options      left_options  = source_options;
    Options      right_options = source_options;
    left_options.page_store    = &left_store;
    right_options.page_store   = &right_store;
    std::unique_ptr<Crowdbtree> left;
    std::unique_ptr<Crowdbtree> right;
    ASSERT_TRUE(rebuild_range(source, KeyRange::bounded(std::nullopt, std::string("k1050")), left_options, &left).ok());
    ASSERT_TRUE(
        rebuild_range(source, KeyRange::bounded(std::string("k1050"), std::nullopt), right_options, &right).ok());

    auto left_entries  = live_entries(*left);
    auto right_entries = live_entries(*right);
    for (const auto &[key, value] : right_entries) {
        EXPECT_FALSE(left_entries.contains(key));
        left_entries.emplace(key, value);
    }
    EXPECT_EQ(left_entries, expected);
    EXPECT_EQ(left->get(Slice("k1050"), nullptr, nullptr), false);
    EXPECT_EQ(right->get(Slice("k1049"), nullptr, nullptr), false);
}

TEST(RangeRebuild, ConcurrentWorkersPublishIndependentTrees)
{
    MemPageStore source_store(1);
    Options      source_options;
    source_options.page_store = &source_store;
    Crowdbtree source(source_options);
    for (uint64_t i = 0; i < 20; ++i) {
        ASSERT_TRUE(source.put(Slice("k" + std::to_string(i + 10)), Slice("v")).ok());
    }
    ASSERT_TRUE(source.flush().ok());
    ASSERT_TRUE(source.snapshot().ok());

    MemPageStore low_store(1);
    MemPageStore high_store(1);
    auto         worker = [&source, &source_options](MemPageStore *store, KeyRange range) {
        Options options    = source_options;
        options.page_store = store;
        std::unique_ptr<Crowdbtree> result;
        Status                      status = rebuild_range(source, range, options, &result);
        return std::pair<Status, std::unique_ptr<Crowdbtree>>(status, std::move(result));
    };
    auto low_future =
        std::async(std::launch::async, worker, &low_store, KeyRange::bounded(std::nullopt, std::string("k20")));
    auto high_future =
        std::async(std::launch::async, worker, &high_store, KeyRange::bounded(std::string("k20"), std::nullopt));
    auto low  = low_future.get();
    auto high = high_future.get();
    ASSERT_TRUE(low.first.ok());
    ASSERT_TRUE(high.first.ok());
    EXPECT_FALSE(live_entries(*low.second).empty());
    EXPECT_FALSE(live_entries(*high.second).empty());
    EXPECT_EQ(live_entries(source).size(), 20U);
}

} // namespace
} // namespace crowdb::tree
