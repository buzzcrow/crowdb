// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"

#include <map>

namespace crowdb::tree
{
namespace
{
using test_chunk::TestCoreChunk;
using TestContents = std::map<std::string, std::string>;

TestContents seed(TestCoreChunk &test)
{
    TestContents expected;
    Batch        batch;
    for (size_t i = 0; i < 96; ++i) {
        const auto key   = std::to_string(1000 + i);
        const auto value = std::string(i % 31 == 0 ? 12000 : 48, static_cast<char>('a' + (i % 26)));
        expected.emplace(key, value);
        batch.ops.push_back({.key = key, .kind = OpKind::kPut, .value = value});
    }
    EXPECT_TRUE(test.tree->apply(1, batch).ok());
    const auto status = test.tree->snapshot();
    EXPECT_TRUE(status.ok()) << status.to_string();
    return expected;
}

void check_scan(const Crowdbtree &tree, const TestContents &expected, bool reverse, bool keys_only = false)
{
    TestContents actual;
    std::string  cursor;
    bool         truncated = true;
    size_t       windows   = 0;
    while (truncated && windows++ <= expected.size()) {
        std::vector<scan_entry> entries;
        const auto status = reverse ? tree.scan_reverse(cursor, !cursor.empty(), false, {}, 7, 0, &entries, &truncated)
                                    : tree.scan({}, cursor, {}, 7, 0, keys_only, 0, &entries, &truncated);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (truncated) {
            ASSERT_FALSE(entries.empty());
        }
        for (const auto &entry : entries) {
            if (!cursor.empty()) {
                EXPECT_TRUE(reverse ? entry.key < cursor : entry.key > cursor);
            }
            cursor = entry.key;
            EXPECT_TRUE(actual.emplace(entry.key, entry.value).second);
        }
    }
    ASSERT_FALSE(truncated);
    ASSERT_EQ(actual.size(), expected.size());
    for (const auto &[key, value] : expected) {
        const auto found = actual.find(key);
        ASSERT_NE(found, actual.end()) << key;
        EXPECT_EQ(found->second, keys_only ? std::string{} : value) << key;
    }
}

TEST(TreeCoreChunkRead, ColdForwardReverseAndKeyOnlyWindowsMatchTheCommittedKeyspace)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    const auto expected = seed(test);
    ASSERT_GT(test.tree->tree_state_stats().logical.leaf_pages, 1U);
    test.tree.reset();
    auto status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto metrics = TreeCoreAccess::get(*test.tree)->publisher().work_metrics();
    const auto encoded = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    check_scan(*test.tree, expected, false, true);
    check_scan(*test.tree, expected, false);
    check_scan(*test.tree, expected, true);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encoded);
    const auto summary = test.tree->tree_summary();
    EXPECT_TRUE(summary.exact);
    EXPECT_EQ(summary.live_kv, expected.size());
    EXPECT_EQ(test.tree->tree_state_stats().logical.kv_unknown_pages, 0U);
}

TEST(TreeCoreChunkRead, OverlayTombstonesAndOldSnapshotKeepDifferentFixedViewsAfterClose)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    const auto previous = seed(test);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    auto snapshot = test.tree->snapshot_view();
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->at_slot(), 1U);
    auto  expected = previous;
    Batch batch;
    for (size_t i = 0; i < 96; i += 7) {
        const auto key = std::to_string(1000 + i);
        batch.ops.push_back({.key = key, .kind = OpKind::kDelete, .value = {}});
        expected.erase(key);
    }
    batch.ops.push_back({.key = "1031", .kind = OpKind::kPut, .value = "new"});
    expected["1031"] = "new";
    ASSERT_TRUE(test.tree->apply(2, batch).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    check_scan(*test.tree, expected, false);
    check_scan(*test.tree, expected, true);
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    for (const auto &[key, value] : previous) {
        std::string actual;
        uint64_t    slot = 0;
        ASSERT_TRUE(snapshot->get(key, &slot, &actual)) << key;
        EXPECT_EQ(slot, 1U);
        EXPECT_EQ(actual, value) << key;
    }
    snapshot.reset();
    ASSERT_TRUE(test.open().ok());
    check_scan(*test.tree, expected, false);
    check_scan(*test.tree, expected, true);
}

TEST(TreeCoreChunkRead, ColdInspectionUsesOneIoPermitAndPreservesTheCapturedRevision)
{
    TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    const auto expected = seed(test);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    NativeFrame frame;
    uint64_t    version = 0;
    uint64_t    root    = 0;
    uint32_t    deltas  = 0;
    auto        status  = test.tree->inspect_page({}, kInvalidPageId, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(frame_page_type(frame.frame.data()), page_type::kInnerBase);
    const auto captured = version;
    status              = test.tree->inspect_page({0}, captured, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(frame_page_type(frame.frame.data()), page_type::kLeafBase);
    ASSERT_TRUE(frame_validate(frame.frame.data(), frame.frame.size()));
    LeafFrameView leaf(frame.frame.data(), frame.frame.size());
    ASSERT_GT(leaf.count(), 0U);
    EXPECT_TRUE(expected.contains(leaf.key(0).to_string()));
    ASSERT_TRUE(test.tree->apply(2, {}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_EQ(test.tree->inspect_page({}, captured, &frame, &version, &root, &deltas).code(), Code::kUnavailable);
}
} // namespace
} // namespace crowdb::tree
