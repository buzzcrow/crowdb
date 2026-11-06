// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/mapping_geometry.h"
#include "local_checkpoint_test_support.h"
#include "runtime_tree_access.h"

namespace crowdb::tree
{
using namespace test_write;

namespace
{
std::shared_ptr<PublishedTreeState> sparse_captured_tree(size_t leaves, bool first_pair_shared = false)
{
    auto       captured = std::make_shared<PublishedTreeState>();
    auto       index    = std::make_shared<InnerPageState>();
    const auto page_id  = [first_pair_shared](size_t i) {
        return first_pair_shared && i == 1 ? 1 : i * kMappingSegmentSize;
    };
    std::vector<PageReplacement> replacements;
    for (size_t i = 0; i < leaves; ++i) {
        auto         key = std::to_string(1000 + i);
        PageKeyRange range;
        if (i != 0) {
            range.lower = key;
            index->separators.emplace_back(key);
        }
        if (i + 1 < leaves) {
            range.upper = std::to_string(1001 + i);
        }
        const auto pid = page_id(i);
        index->children.push_back(pid);
        auto leaf           = std::make_shared<PageState>(*page(
            {
                {.key = key, .value = "v"}
        },
            range));
        leaf->right_sibling = i + 1 == leaves ? kInvalidPageId : page_id(i + 1);
        replacements.push_back({.page_id = pid, .expected = {}, .replacement = published(std::move(leaf))});
    }
    auto root              = std::make_shared<PublishedPage>();
    root->level            = 1;
    root->inner            = index;
    root->metrics.leaf     = false;
    captured->root_page_id = leaves * kMappingSegmentSize;
    captured->next_page_id = captured->root_page_id + 1;
    captured->coverage     = 10;
    replacements.push_back({.page_id = captured->root_page_id, .expected = {}, .replacement = root});
    MappingView::CopyStats copied;
    captured->mapping = captured->mapping.replace(replacements, copied);
    return captured;
}

DurableCapturedFrame rewrite_leaf(TestCheckpoint &test, const PublishedTreeState &tree, uint64_t pid, std::string key,
                                  std::string value)
{
    const auto old      = tree.mapping.get(pid)->leaf;
    auto       leaf     = std::make_shared<PageState>(*page(
        {
            {.key = std::move(key), .value = std::move(value)}
    },
        old->range));
    leaf->right_sibling = old->right_sibling;
    CapturedWriteLimits limits{
        .frames = {.encode = {.frame_bytes = 4096, .metadata = test.source.credits.credits.reservation()}},
        .write  = {.max_frame_bytes = 4096}};
    std::shared_ptr<CapturedFrameStream> stream;
    EXPECT_TRUE(CapturedFrameStream::create({.page         = published(std::move(leaf)),
                                             .page_id      = pid,
                                             .coverage     = 11,
                                             .gc_floor     = 0,
                                             .next_page_id = tree.next_page_id,
                                             .context      = {.range = old->range}},
                                            {}, test.source.runtime, test.source.lane, limits.frames, {}, &stream)
                    .ok());
    auto pages          = std::make_shared<CapturedDurablePages>(test.source.credits.credits.reservation());
    pages->root_page_id = pid;
    pages->next_page_id = tree.next_page_id;
    pages->coverage     = 11;
    pages->generation   = tree.generation;
    EXPECT_TRUE(CapturedWriteOperation::create(
                    std::move(stream), std::move(pages), test.source.store, test.source.runtime, test.source.lane,
                    limits, test.source.credits.credits,
                    extent_write_reservation(test.source.ledger, test.source.credits.credits.reservation()), {},
                    &test.source.operation)
                    .ok());
    Completion done;
    test.source.run(&done);
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    return test.source.operation->result().pages->frames.at(pid);
}
} // namespace

TEST(LocalCheckpoint, SparseMappingStreamsDirectoryAcrossWindowsAndEnforcesSegmentCap)
{
    TestCheckpoint test;
    // 385 segment entries require 12,340 logical directory bytes, crossing
    // the single 12,288-byte aligned metadata window.
    constexpr size_t leaves = 384;
    ASSERT_TRUE(test.source
                    .prepare(sparse_captured_tree(leaves), {},
                             {.frames = {.encode = {.frame_bytes = 16384}}, .write = {.max_frame_bytes = 16384}})
                    .ok());
    Completion materialized;
    test.source.run(&materialized);
    ASSERT_EQ(materialized.values.load(), 1U) << materialized.status.to_string();
    auto pages = test.source.operation->result().pages;
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    const auto &anchor = test.operation->result().checkpoint->anchor;
    EXPECT_EQ(anchor.segdir_len, 12340U);
    EXPECT_EQ(anchor.leaf_count, leaves);
    EXPECT_EQ(anchor.inner_count, 1U);
    EXPECT_EQ(test.store->events.size(), leaves + 6);
    Config config;
    config.page_store     = test.store->device.get();
    config.frame_bytes    = 16384;
    config.inner_max_keys = leaves - 1;
    std::unique_ptr<Crowdbtree> tree;
    const auto                  opened = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    for (size_t i = 0; i < leaves; ++i) {
        const auto key = std::to_string(1000 + i);
        GetView    result;
        const auto read = RuntimeTreeAccess_for_tests::core(*tree).get(Slice(key), &result);
        ASSERT_TRUE(read.ok()) << key << ": " << read.to_string();
        ASSERT_TRUE(result.found()) << key;
        EXPECT_EQ(result.value().to_string(), "v");
    }
    const auto protected_before = test.source.ledger->stats().protected_ranges;
    ASSERT_TRUE(test.prepare(pages, {.max_segments = 2}).ok());
    Completion capped;
    test.run(&capped);
    EXPECT_EQ(capped.errors.load(), 1U);
    EXPECT_EQ(capped.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.registry->snapshot_sequence(), 1U);
    EXPECT_FALSE(test.registry->needs_recovery());
    EXPECT_EQ(test.source.ledger->stats().protected_ranges, protected_before);
}

TEST(LocalCheckpoint, MultiplePageUpdatesWriteOneSegmentAndRetainUnchangedImages)
{
    TestCheckpoint test;
    auto           tree = sparse_captured_tree(3, true);
    ASSERT_TRUE(
        test.source.prepare(tree, {}, {.frames = {.encode = {.frame_bytes = 4096}}, .write = {.max_frame_bytes = 4096}})
            .ok());
    Completion materialized;
    test.source.run(&materialized);
    ASSERT_EQ(materialized.values.load(), 1U) << materialized.status.to_string();
    auto original = test.source.operation->result().pages;
    ASSERT_TRUE(test.prepare(original).ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto reader = test.operation->result().checkpoint;
    ASSERT_EQ(reader->metadata->segments.size(), 3U);
    std::weak_ptr<const void> changed_owner = reader->metadata->segments[0].second.owner;
    auto                      updated       = std::make_shared<CapturedDurablePages>(*original);
    updated->frames.at(0)                   = rewrite_leaf(test, *tree, 0, "1000", "left");
    updated->frames.at(1)                   = rewrite_leaf(test, *tree, 1, "1001", "right");
    updated->coverage                       = 11;
    const auto before                       = test.store->events.size();
    ASSERT_TRUE(test.prepare(updated).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U) << second.status.to_string();
    const auto current = test.operation->result().checkpoint;
    EXPECT_EQ(test.store->events.size() - before, 5U);
    ASSERT_EQ(current->metadata->segments.size(), 3U);
    EXPECT_NE(current->metadata->segments[0].first.image_addr, reader->metadata->segments[0].first.image_addr);
    for (size_t i = 1; i < current->metadata->segments.size(); ++i) {
        const auto &old = reader->metadata->segments[i];
        const auto &now = current->metadata->segments[i];
        EXPECT_EQ(now.first.image_addr, old.first.image_addr);
        EXPECT_EQ(now.first.image_crc, old.first.image_crc);
        EXPECT_EQ(now.first.generation, old.first.generation);
        EXPECT_EQ(now.second.owner, old.second.owner);
    }
    auto advanced      = std::make_shared<CapturedDurablePages>(*updated);
    advanced->coverage = 12;
    ASSERT_TRUE(test.prepare(advanced).ok());
    Completion third;
    test.run(&third);
    ASSERT_EQ(third.values.load(), 1U) << third.status.to_string();
    EXPECT_FALSE(changed_owner.expired());
    reader.reset();
    EXPECT_TRUE(changed_owner.expired());
    Config config;
    config.page_store  = test.store->device.get();
    config.frame_bytes = 4096;
    std::unique_ptr<Crowdbtree> reopened;
    const auto                  opened = Crowdbtree::open(config, &reopened);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    for (const auto &[key, expected] : std::array<std::pair<std::string, std::string>, 3>{
             {{"1000", "left"}, {"1001", "right"}, {"1002", "v"}}
    }) {
        GetView value;
        ASSERT_TRUE(RuntimeTreeAccess_for_tests::core(*reopened).get(key, &value).ok());
        ASSERT_TRUE(value.found());
        EXPECT_EQ(value.value().to_string(), expected);
    }
}

} // namespace crowdb::tree
