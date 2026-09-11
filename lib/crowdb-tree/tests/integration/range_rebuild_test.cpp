// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "crowdb-tree/c_api.h"

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
    RangeRebuildStats           left_stats;
    RangeRebuildStats           right_stats;
    ASSERT_TRUE(
        rebuild_range(source, KeyRange::bounded(std::nullopt, std::string("k1050")), left_options, &left, &left_stats)
            .ok());
    ASSERT_TRUE(rebuild_range(source, KeyRange::bounded(std::string("k1050"), std::nullopt), right_options, &right,
                              &right_stats)
                    .ok());

    auto left_entries  = live_entries(*left);
    auto right_entries = live_entries(*right);
    for (const auto &[key, value] : right_entries) {
        EXPECT_FALSE(left_entries.contains(key));
        left_entries.emplace(key, value);
    }
    EXPECT_EQ(left_entries, expected);
    EXPECT_EQ(left->get(Slice("k1050"), nullptr, nullptr), false);
    EXPECT_EQ(right->get(Slice("k1049"), nullptr, nullptr), false);
    EXPECT_GT(left_stats.pages_reused, 0U);
    EXPECT_GT(left_stats.pages_rebuilt, 0U);
    EXPECT_GT(right_stats.pages_reused, 0U);
    EXPECT_GT(right_stats.pages_rebuilt, 0U);
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

TEST(RangeRebuild, WhollyContainedTreeReusesNativePageFrames)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store       = &source_store;
    options.frame_bytes      = 4096;
    options.leaf_split_bytes = 256;
    Crowdbtree source(options);
    for (uint64_t i = 0; i < 40; ++i) {
        ASSERT_TRUE(source.put(Slice("k" + std::to_string(i + 100)), Slice("value")).ok());
    }
    ASSERT_TRUE(source.flush().ok());
    ASSERT_TRUE(source.snapshot().ok());

    MemPageStore destination_store(1);
    options.page_store = &destination_store;
    std::unique_ptr<Crowdbtree> destination;
    RangeRebuildStats           stats;
    ASSERT_TRUE(rebuild_range(source, KeyRange::bounded(std::string("k100"), std::string("k999")), options,
                              &destination, &stats)
                    .ok());
    EXPECT_GT(stats.pages_reused, 0U);
    EXPECT_EQ(stats.pages_rebuilt, 0U);
    EXPECT_EQ(stats.entries_emitted, 40U);
    EXPECT_EQ(live_entries(*destination), live_entries(source));
}

TEST(RangeRebuild, CopiesOnlyOverflowChainsReferencedByTheChildRange)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store       = &source_store;
    options.frame_bytes      = 4096;
    options.max_inline_value = 32;
    Crowdbtree        source(options);
    const std::string low_value(6000, 'l');
    const std::string kept_value(7000, 'k');
    const std::string high_value(8000, 'h');
    ASSERT_TRUE(source.put(Slice("a"), Slice(low_value)).ok());
    ASSERT_TRUE(source.put(Slice("m"), Slice(kept_value)).ok());
    ASSERT_TRUE(source.put(Slice("z"), Slice(high_value)).ok());
    ASSERT_TRUE(source.flush().ok());
    ASSERT_TRUE(source.snapshot().ok());

    MemPageStore destination_store(1);
    options.page_store = &destination_store;
    std::unique_ptr<Crowdbtree> destination;
    RangeRebuildStats           stats;
    ASSERT_TRUE(
        rebuild_range(source, KeyRange::bounded(std::string("m"), std::string("n")), options, &destination, &stats)
            .ok());

    const auto rebuilt = live_entries(*destination);
    ASSERT_EQ(rebuilt.size(), 1U);
    EXPECT_EQ(rebuilt.at("m"), kept_value);
    EXPECT_EQ(stats.entries_examined, 3U);
    EXPECT_EQ(stats.entries_emitted, 1U);
    EXPECT_EQ(stats.entries_filtered, 2U);
    EXPECT_EQ(live_entries(source).size(), 3U);
}

TEST(RangeRebuild, RewrittenRootAllocatesAboveSourcePageIdHighWater)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store       = &source_store;
    options.frame_bytes      = 4096;
    options.leaf_split_bytes = 256;
    Crowdbtree source(options);
    for (uint64_t index = 0; index < 80; ++index) {
        ASSERT_TRUE(source.put(Slice("k" + std::to_string(index + 1000)), Slice("value")).ok());
    }
    ASSERT_TRUE(source.flush().ok());

    std::vector<NativeFrame> source_frames;
    uint64_t                 source_root      = kInvalidPageId;
    uint64_t                 source_slot      = 0;
    uint64_t                 source_highwater = 0;
    ASSERT_TRUE(source.collect_native_frames(&source_frames, &source_root, &source_slot, &source_highwater).ok());

    MemPageStore destination_store(1);
    options.page_store = &destination_store;
    std::unique_ptr<Crowdbtree> destination;
    ASSERT_TRUE(
        rebuild_range(source, KeyRange::bounded(std::string("k1021"), std::string("k1063")), options, &destination)
            .ok());

    std::vector<NativeFrame> destination_frames;
    uint64_t                 destination_root = kInvalidPageId;
    ASSERT_TRUE(destination->collect_native_frames(&destination_frames, &destination_root, nullptr).ok());
    EXPECT_GE(destination_root, source_highwater);
    EXPECT_EQ(source_root < source_highwater, true);
}

TEST(RangeRebuild, EmptyAndUnboundedEndpointsUseTheSameHalfOpenPredicate)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store = &source_store;
    Crowdbtree                     source(options);
    const std::vector<std::string> keys = {"", "a", "aa", "m", "z", std::string(1, static_cast<char>(0xff))};
    for (const std::string &key : keys) {
        ASSERT_TRUE(source.put(Slice(key), Slice("v")).ok());
    }
    ASSERT_TRUE(source.flush().ok());

    auto rebuild_keys = [&source, &options](KeyRange range) {
        MemPageStore destination_store(1);
        Options      destination_options = options;
        destination_options.page_store   = &destination_store;
        std::unique_ptr<Crowdbtree> destination;
        EXPECT_TRUE(rebuild_range(source, range, destination_options, &destination).ok());
        std::vector<std::string> result;
        if (destination != nullptr) {
            for (const auto &[key, value] : live_entries(*destination)) {
                (void)value;
                result.push_back(key);
            }
        }
        return result;
    };

    EXPECT_EQ(rebuild_keys(KeyRange::unbounded()), keys);
    EXPECT_EQ(rebuild_keys(KeyRange::bounded(std::nullopt, std::string("m"))),
              (std::vector<std::string>{"", "a", "aa"}));
    EXPECT_EQ(rebuild_keys(KeyRange::bounded(std::string("m"), std::nullopt)),
              (std::vector<std::string>{"m", "z", std::string(1, static_cast<char>(0xff))}));
    EXPECT_TRUE(rebuild_keys(KeyRange::bounded(std::string("m"), std::string("m"))).empty());
    EXPECT_EQ(rebuild_keys(KeyRange::bounded(std::string("a"), std::string("b"))),
              (std::vector<std::string>{"a", "aa"}));
}

TEST(RangeRebuild, ChildMutationDoesNotChangeTheSourceTree)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store = &source_store;
    Crowdbtree source(options);
    ASSERT_TRUE(source.put(Slice("b"), Slice("source")).ok());
    ASSERT_TRUE(source.put(Slice("z"), Slice("outside")).ok());
    ASSERT_TRUE(source.flush().ok());

    MemPageStore destination_store(1);
    options.page_store = &destination_store;
    std::unique_ptr<Crowdbtree> destination;
    ASSERT_TRUE(
        rebuild_range(source, KeyRange::bounded(std::string("a"), std::string("m")), options, &destination).ok());
    ASSERT_TRUE(destination->put(Slice("b"), Slice("child")).ok());
    ASSERT_TRUE(destination->flush().ok());

    EXPECT_EQ(live_entries(*destination).at("b"), "child");
    EXPECT_EQ(live_entries(source).at("b"), "source");
    EXPECT_EQ(live_entries(source).at("z"), "outside");
}

TEST(RangeRebuild, NativeInstallRejectsCrossingSiblingAndMissingChildReferences)
{
    MemPageStore source_store(1);
    Options      options;
    options.page_store       = &source_store;
    options.frame_bytes      = 4096;
    options.leaf_split_bytes = 256;
    Crowdbtree source(options);
    for (uint64_t index = 0; index < 80; ++index) {
        ASSERT_TRUE(source.put(Slice("k" + std::to_string(index + 1000)), Slice("value")).ok());
    }
    ASSERT_TRUE(source.flush().ok());

    std::vector<NativeFrame> frames;
    uint64_t                 root      = kInvalidPageId;
    uint64_t                 slot      = 0;
    uint64_t                 highwater = 0;
    ASSERT_TRUE(source.collect_native_frames(&frames, &root, &slot, &highwater).ok());

    auto broken_sibling = frames;
    auto leaf           = std::find_if(broken_sibling.begin(), broken_sibling.end(), [](const NativeFrame &frame) {
        return frame_page_type(frame.frame.data()) == page_type::kLeafBase &&
               LeafFrameView(frame.frame.data(), static_cast<uint32_t>(frame.frame.size())).right_sibling() !=
                   kInvalidPageId;
    });
    ASSERT_NE(leaf, broken_sibling.end());
    frame_put_u64(leaf->frame.data(), fh::kRightSibling, kInvalidPageId);
    frame_restamp_crc(leaf->frame.data(), static_cast<uint32_t>(leaf->frame.size()));

    MemPageStore sibling_store(1);
    options.page_store = &sibling_store;
    Crowdbtree sibling_destination(options);
    EXPECT_EQ(sibling_destination.install_snapshot_native(std::move(broken_sibling), root, slot, highwater).code(),
              Code::kCorruption);

    auto broken_child = frames;
    auto inner        = std::find_if(broken_child.begin(), broken_child.end(), [](const NativeFrame &frame) {
        return frame_page_type(frame.frame.data()) == page_type::kInnerBase;
    });
    ASSERT_NE(inner, broken_child.end());
    frame_put_u64(inner->frame.data(), kFrameHeaderSize, highwater + 100);
    frame_restamp_crc(inner->frame.data(), static_cast<uint32_t>(inner->frame.size()));

    MemPageStore child_store(1);
    options.page_store = &child_store;
    Crowdbtree child_destination(options);
    EXPECT_EQ(child_destination.install_snapshot_native(std::move(broken_child), root, slot, highwater).code(),
              Code::kCorruption);
}

TEST(RangeRebuild, LazyRecoveryRejectsAResolvedPageOutsideTheTreeRange)
{
    MemPageStore store(1);
    Options      options;
    options.page_store = &store;
    {
        Crowdbtree source(options);
        ASSERT_TRUE(source.put(Slice("b"), Slice("inside")).ok());
        ASSERT_TRUE(source.put(Slice("z"), Slice("outside")).ok());
        ASSERT_TRUE(source.flush().ok());
        ASSERT_TRUE(source.snapshot().ok());
    }

    options.key_range = KeyRange::bounded(std::string("a"), std::string("m"));
    std::unique_ptr<Crowdbtree> bounded;
    ASSERT_TRUE(Crowdbtree::open(options, &bounded).ok());
    EXPECT_FALSE(bounded->get(Slice("b"), nullptr, nullptr));
    EXPECT_TRUE(bounded->io_failed());
}

TEST(RangeRebuild, CApiBuildsAnIndependentTreeOnAnInjectedStore)
{
    ct_options source_options = {};
    ct_tree   *source         = nullptr;
    ASSERT_EQ(ct_open(&source_options, &source), 0);
    for (uint64_t slot = 1; slot <= 4; ++slot) {
        const char key = "abmz"[slot - 1];
        ASSERT_EQ(ct_apply_put(source, slot, reinterpret_cast<const uint8_t *>(&key), 1,
                               reinterpret_cast<const uint8_t *>("v"), 1),
                  0);
    }
    ASSERT_EQ(ct_flush(source), 0);

    ct_page_store *store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &store), 0);
    const uint8_t start                 = 'b';
    const uint8_t end                   = 'm';
    ct_options    destination_options   = {};
    destination_options.page_store      = store;
    destination_options.range_bounded   = 1;
    destination_options.range_start     = &start;
    destination_options.range_start_len = 1;
    destination_options.range_end       = &end;
    destination_options.range_end_len   = 1;
    ct_tree               *destination  = nullptr;
    ct_range_rebuild_stats stats        = {};
    ASSERT_EQ(ct_rebuild_range(source, &destination_options, &destination, &stats), 0);
    ct_page_store_free(store);
    EXPECT_EQ(stats.entries_examined, 4U);
    EXPECT_EQ(stats.entries_emitted, 1U);
    EXPECT_EQ(stats.entries_filtered, 3U);

    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   value = {};
    ASSERT_EQ(ct_get(destination, &start, 1, &found, &slot, &value), 0);
    EXPECT_EQ(found, 1);
    EXPECT_EQ(slot, 2U);
    ct_free_buf(&value);

    ct_close(destination);
    ct_close(source);
}

} // namespace
} // namespace crowdb::tree
