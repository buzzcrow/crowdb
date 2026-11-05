// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/status.h"
#include "crowdb-tree/tree_stats_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeStatsCAbi, VersionedSnapshotPreservesUnknownFieldsAndDoesNotMutateOnError)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ct_tree_stats_snapshot_v1 result{};
    result.struct_size = sizeof(result);
    result.version     = 1;
    ASSERT_EQ(ct_get_tree_state_stats(tree, &result), 0);
    EXPECT_NE(result.instance_generation, 0U);
    EXPECT_EQ(result.logical.leaf_pages, 1U);
    EXPECT_EQ(result.logical.kv_unknown_pages, 0U);
    EXPECT_GT(result.resources.allocated_bytes, 0U);
    EXPECT_GT(result.resources.reserved_bytes, 0U);
    const auto generation = result.instance_generation;
    const auto sequence   = result.commit_seq;
    ASSERT_EQ(ct_get_tree_state_stats(tree, &result), 0);
    EXPECT_EQ(result.instance_generation, generation);
    EXPECT_EQ(result.commit_seq, sequence);
    result.version = 2;
    EXPECT_EQ(ct_get_tree_state_stats(tree, &result), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(result.instance_generation, generation);
    EXPECT_EQ(result.version, 2U);
    result.version = 1;
    --result.struct_size;
    EXPECT_EQ(ct_get_tree_state_stats(tree, &result), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(result.commit_seq, sequence);
    ct_close(tree);
    ct_runtime_free(runtime);
}

TEST(TreeStatsCAbi, StandaloneTreeReportsItsRuntimePublicationStats)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    ct_tree_stats_snapshot_v1 result{};
    result.struct_size = sizeof(result);
    result.version     = 1;
    result.commit_seq  = 123;
    EXPECT_EQ(ct_get_tree_state_stats(tree, &result), 0);
    EXPECT_NE(result.instance_generation, 0U);
    EXPECT_EQ(result.logical.leaf_pages, 1U);
    EXPECT_GT(result.resources.allocated_bytes, 0U);
    EXPECT_NE(result.commit_seq, 123U);
    ct_close(tree);
}
} // namespace crowdb::tree
