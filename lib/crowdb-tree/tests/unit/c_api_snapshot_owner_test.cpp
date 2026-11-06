// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/c_api.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(SnapshotOwnerCAbi, ChunkAndChargeOutliveTheStreamTreeAndRuntimeHandles)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    Config config;
    ASSERT_TRUE(configure_tree_runtime(runtime, nullptr, &config).ok());
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    const uint8_t     key = 'k';
    const std::string value(8192, 'v');
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, reinterpret_cast<const uint8_t *>(value.data()), value.size()), 0);
    ASSERT_EQ(ct_flush(tree), 0);
    ct_export *stream = nullptr;
    ASSERT_EQ(ct_snapshot_export_begin(tree, 64, &stream), 0);
    ct_snapshot_chunk *chunk = nullptr;
    int32_t            done  = 0;
    ASSERT_EQ(ct_snapshot_export_next_v2(stream, 0, &chunk, &done), 0);
    EXPECT_EQ(done, 0);
    const uint8_t *data   = nullptr;
    size_t         length = 0;
    ASSERT_EQ(ct_snapshot_chunk_data(chunk, &data, &length), 0);
    ASSERT_EQ(length, 64U);
    const std::string retained(reinterpret_cast<const char *>(data), length);
    ct_buf            copied{};
    ASSERT_EQ(ct_snapshot_export_next(stream, length, &copied, &done), 0);
    EXPECT_EQ(copied.len, 64U);
    ct_free_buf(&copied);
    ct_snapshot_export_end(stream);
    ct_close(tree);
    ct_runtime_free(runtime);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(data), length), retained);
    const auto held = config.runtime->snapshot().allocated.routing_bytes;
    ct_snapshot_chunk_free(chunk);
    EXPECT_LT(config.runtime->snapshot().allocated.routing_bytes, held);
}

TEST(SnapshotOwnerCAbi, InvalidOffsetDoesNotAdvanceAndLegacyCopiedBuffersRemainOwned)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    ASSERT_EQ(ct_flush(tree), 0);
    ct_export *stream = nullptr;
    ASSERT_EQ(ct_snapshot_export_begin(tree, 16, &stream), 0);
    ct_snapshot_chunk *chunk = nullptr;
    int32_t            done  = 0;
    EXPECT_EQ(ct_snapshot_export_next_v2(stream, 1, &chunk, &done), static_cast<ct_status>(Code::kInvalidArgument));
    EXPECT_EQ(chunk, nullptr);
    EXPECT_EQ(ct_snapshot_export_offset(stream), 0U);
    ct_buf copied{};
    ASSERT_EQ(ct_snapshot_export_next(stream, 0, &copied, &done), 0);
    ASSERT_GT(copied.len, 0U);
    const std::string retained(reinterpret_cast<const char *>(copied.data), copied.len);
    ct_snapshot_export_end(stream);
    ct_close(tree);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(copied.data), copied.len), retained);
    ct_free_buf(&copied);
}
} // namespace crowdb::tree
