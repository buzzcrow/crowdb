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
std::shared_ptr<PublishedTreeState> sparse_captured_tree(size_t leaves)
{
    auto                         captured = std::make_shared<PublishedTreeState>();
    auto                         index    = std::make_shared<InnerPageState>();
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
        const auto pid = i * kMappingSegmentSize;
        index->children.push_back(pid);
        auto leaf           = std::make_shared<PageState>(*page(
            {
                {.key = key, .value = "v"}
        },
            range));
        leaf->right_sibling = i + 1 == leaves ? kInvalidPageId : (i + 1) * kMappingSegmentSize;
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
} // namespace crowdb::tree
