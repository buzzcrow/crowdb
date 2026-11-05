// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/immutable_snapshot_c_api.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

#include <utility>

namespace crowdb::tree
{
namespace
{
struct TestSnapshotTree
{
    ct_runtime            *runtime  = nullptr;
    ct_tree               *tree     = nullptr;
    ct_immutable_snapshot *snapshot = nullptr;

    TestSnapshotTree()
    {
        EXPECT_EQ(ct_runtime_create(nullptr, &runtime), 0);
        ct_options options{};
        EXPECT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    }

    ~TestSnapshotTree()
    {
        ct_immutable_snapshot_free(snapshot);
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    void put(uint64_t slot, const std::string &key, const std::string &value) const
    {
        ASSERT_EQ(ct_apply_put(tree, slot, reinterpret_cast<const uint8_t *>(key.data()), key.size(),
                               reinterpret_cast<const uint8_t *>(value.data()), value.size()),
                  0);
    }
};
} // namespace

TEST(ImmutableSnapshotCAbi, FixedViewAndResultOwnersSurviveTreeAndRuntimeClose)
{
    TestSnapshotTree test;
    test.put(1, "", "empty");
    test.put(2, "key", "old");
    ASSERT_EQ(ct_flush(test.tree), 0);
    ASSERT_EQ(ct_immutable_snapshot_capture(test.tree, 0, 2, &test.snapshot), 0);
    test.put(3, "key", "new");
    ASSERT_EQ(ct_flush(test.tree), 0);
    ct_close(std::exchange(test.tree, nullptr));
    ct_runtime_free(std::exchange(test.runtime, nullptr));

    ct_future    *get   = nullptr;
    const uint8_t key[] = {'k', 'e', 'y'};
    ASSERT_EQ(ct_immutable_snapshot_get(test.snapshot, key, sizeof(key), &get), 0);
    int32_t  done  = 0;
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   bytes{};
    ASSERT_EQ(ct_future_poll(get, &done, &found, &slot, &bytes), 0);
    ASSERT_EQ(done, 1);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(slot, 2U);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(bytes.data), bytes.len), "old");
    ct_scan_options_v2 scan{};
    scan.struct_size       = sizeof(scan);
    scan.has_start_bound   = 1;
    scan.start_inclusive   = 1;
    scan.limit             = 1;
    ct_scan_result *result = nullptr;
    ASSERT_EQ(ct_immutable_snapshot_scan(test.snapshot, &scan, &result), 0);
    ct_immutable_snapshot_free(std::exchange(test.snapshot, nullptr));
    const uint8_t *packed    = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &packed, &len, &count, &truncated), 0);
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(truncated, 1);
    ASSERT_GE(len, 5U);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(packed + len - 5), 5), "empty");
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(bytes.data), bytes.len), "old");
    ct_scan_result_free(result);
    ct_future_free(get);
}

TEST(ImmutableSnapshotCAbi, CaptureRejectsWrongCoverageAndStrictScanTruncatesOversizedFirstEntry)
{
    TestSnapshotTree test;
    test.put(1, "key", "value");
    ASSERT_EQ(ct_flush(test.tree), 0);
    EXPECT_EQ(ct_immutable_snapshot_capture(test.tree, 0, 2, &test.snapshot),
              static_cast<ct_status>(Code::kUnavailable));
    EXPECT_EQ(test.snapshot, nullptr);
    ASSERT_EQ(ct_immutable_snapshot_capture(test.tree, 0, 1, &test.snapshot), 0);
    ct_scan_options_v2 scan{};
    scan.struct_size        = sizeof(scan);
    scan.strict_byte_budget = 1;
    scan.byte_budget        = 1;
    ct_scan_result *result  = nullptr;
    ASSERT_EQ(ct_immutable_snapshot_scan(test.snapshot, &scan, &result), 0);
    const uint8_t *packed    = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &packed, &len, &count, &truncated), 0);
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(truncated, 1);
    ct_scan_result_free(result);
    result                  = nullptr;
    scan.strict_byte_budget = 0;
    scan.reverse            = 1;
    ASSERT_EQ(ct_immutable_snapshot_scan(test.snapshot, &scan, &result), 0);
    ct_scan_result_free(result);
}
} // namespace crowdb::tree
