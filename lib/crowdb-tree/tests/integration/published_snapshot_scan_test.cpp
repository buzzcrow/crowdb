// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "snapshot/published_snapshot.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>

namespace crowdb::tree
{
namespace
{
std::string key(size_t index)
{
    std::array<char, 32> text{};
    snprintf(text.data(), text.size(), "key%04zu", index);
    return text.data();
}

void expect_entries(const ScanPackedBuf &packed, size_t count, bool reverse, bool tombstones)
{
    size_t offset = 0;
    for (size_t index = 0; index < count; ++index) {
        const auto expected_index = reverse ? 512 - index : index;
        const auto expected_key   = key(expected_index);
        const bool tombstone      = tombstones && (expected_index % 2 != 0);
        const auto value_size     = tombstone ? 0U : 5U;
        ASSERT_GE(packed.size() - offset, expected_key.size() + value_size + 17);
        EXPECT_EQ(packed.data()[offset], expected_key.size());
        EXPECT_EQ(std::memcmp(packed.data() + offset + 4, expected_key.data(), expected_key.size()), 0);
        offset += 4 + expected_key.size() + 8;
        EXPECT_EQ(packed.data()[offset], tombstone ? 1 : 0);
        EXPECT_EQ(packed.data()[offset + 1], value_size);
        offset += 5;
        if (!tombstone) {
            EXPECT_EQ(std::memcmp(packed.data() + offset, "value", value_size), 0);
        }
        offset += value_size;
    }
    EXPECT_EQ(offset, packed.size());
}

TEST(PublishedSnapshotScan, TotalCountsSpanFixedWindowsInBothDirectionsAfterTreeClose)
{
    auto tree = std::make_unique<Crowdbtree>();
    for (size_t index = 0; index < 513; ++index) {
        ASSERT_TRUE(tree->put(key(index), "value").ok());
    }
    ASSERT_TRUE(tree->flush().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(*tree)->capture_snapshot(&snapshot).ok());
    ASSERT_TRUE(tree->put("later", "outside the captured L1").ok());
    tree.reset();
    for (const size_t limit : {size_t{0}, size_t{129}, size_t{257}}) {
        for (const bool reverse : {false, true}) {
            CoreScanOptions options;
            options.limit   = limit;
            options.reverse = reverse;
            ScanPackedBuf packed;
            size_t        count     = 0;
            bool          truncated = false;
            const auto    status    = snapshot->scan(options, &packed, &count, &truncated);
            ASSERT_TRUE(status.ok()) << status.to_string();
            ASSERT_EQ(count, limit == 0 ? 513 : limit);
            EXPECT_EQ(truncated, limit != 0);
            expect_entries(packed, count, reverse, false);
        }
    }
}

TEST(PublishedSnapshotScan, TombstonesUseThePhysicalCountBoundAcrossWindows)
{
    Crowdbtree tree;
    Batch      batch;
    for (size_t index = 0; index < 513; ++index) {
        const bool tombstone = index % 2 != 0;
        batch.ops.push_back(
            {.key = key(index), .kind = tombstone ? OpKind::kDelete : OpKind::kPut, .value = tombstone ? "" : "value"});
    }
    ASSERT_TRUE(tree.apply(1, batch).ok());
    ASSERT_TRUE(tree.flush().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(tree)->capture_snapshot(&snapshot).ok());
    for (const size_t limit : {size_t{0}, size_t{129}, size_t{257}}) {
        for (const bool reverse : {false, true}) {
            CoreScanOptions options;
            options.limit              = limit;
            options.reverse            = reverse;
            options.include_tombstones = true;
            ScanPackedBuf packed;
            size_t        count     = 0;
            bool          truncated = false;
            const auto    status    = snapshot->scan(options, &packed, &count, &truncated);
            ASSERT_TRUE(status.ok()) << status.to_string();
            ASSERT_EQ(count, limit == 0 ? 513 : limit);
            EXPECT_EQ(truncated, limit != 0);
            expect_entries(packed, count, reverse, true);
        }
    }
}

TEST(PublishedSnapshotScan, PublicByteLimitsSpanWindowsAndPermitOneLargerWindowEntry)
{
    Crowdbtree          tree;
    std::vector<size_t> sizes{400 * 1024, 400 * 1024, 1500 * 1024, 400 * 1024, 400 * 1024};
    for (size_t index = 0; index < sizes.size(); ++index) {
        ASSERT_TRUE(tree.put(key(index), std::string(sizes[index], 'v')).ok());
    }
    ASSERT_TRUE(tree.flush().ok());
    auto                                     *core = TreeCoreAccess::get(tree);
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(core->capture_snapshot(&snapshot).ok());
    for (const bool reverse : {false, true}) {
        for (const size_t budget : {size_t{0}, size_t{3 * 1024 * 1024}}) {
            CoreScanOptions options;
            options.reverse            = reverse;
            options.byte_budget        = budget;
            options.strict_byte_budget = budget != 0;
            size_t expected_count      = 0;
            size_t expected_bytes      = 0;
            for (size_t index = 0; index < sizes.size(); ++index) {
                const auto next = sizes[reverse ? sizes.size() - 1 - index : index] + key(index).size();
                if (budget != 0 && next > budget - expected_bytes) {
                    break;
                }
                ++expected_count;
                expected_bytes += next;
            }
            for (const bool fixed : {false, true}) {
                ScanPackedBuf packed;
                size_t        count     = 0;
                bool          truncated = false;
                const auto    status    = fixed ? snapshot->scan(options, &packed, &count, &truncated)
                                                : core->scan(options, nullptr, &truncated, &packed, &count);
                ASSERT_TRUE(status.ok()) << status.to_string();
                EXPECT_EQ(count, expected_count);
                EXPECT_EQ(packed.size(), expected_bytes + (expected_count * 17));
                EXPECT_EQ(truncated, expected_count != sizes.size());
            }
        }
    }
}

TEST(PublishedSnapshotScan, StrictFirstOverflowReturnsEmptyTruncatedAndResumes)
{
    Crowdbtree tree;
    ASSERT_TRUE(tree.put("a", std::string(128 * 1024, 'v')).ok());
    ASSERT_TRUE(tree.put("b", "small").ok());
    ASSERT_TRUE(tree.flush().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(tree)->capture_snapshot(&snapshot).ok());
    CoreScanOptions options;
    options.byte_budget        = 1024;
    options.strict_byte_budget = true;
    ScanPackedBuf packed;
    size_t        count     = 0;
    bool          truncated = false;
    auto          status    = snapshot->scan(options, &packed, &count, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(packed.size(), 0U);
    EXPECT_TRUE(truncated);
    options.start           = "a";
    options.has_start_bound = true;
    options.byte_budget     = 6;
    status                  = snapshot->scan(options, &packed, &count, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(packed.size(), 6U + 17);
    EXPECT_FALSE(truncated);
    packed                     = {};
    options.start              = {};
    options.has_start_bound    = false;
    options.byte_budget        = 1024;
    options.strict_byte_budget = false;
    status                     = snapshot->scan(options, &packed, &count, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(packed.size(), (128U * 1024) + 1 + 17);
    EXPECT_TRUE(truncated);
}
} // namespace
} // namespace crowdb::tree
