// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/c_api.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

#include <thread>

namespace crowdb::tree
{
TEST(ScanOwnerCAbi, PackedBytesAndChargeOutliveTreeAndRuntimeHandles)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    Config config;
    ASSERT_TRUE(configure_tree_runtime(runtime, nullptr, &config).ok());
    const auto baseline = config.runtime->snapshot().allocated.routing_bytes;
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    const std::string key   = "key";
    const std::string value = "value";
    ASSERT_EQ(ct_apply_put(tree, 1, reinterpret_cast<const uint8_t *>(key.data()), key.size(),
                           reinterpret_cast<const uint8_t *>(value.data()), value.size()),
              0);
    ct_scan_options_v2 scan{};
    scan.struct_size       = sizeof(scan);
    ct_scan_result *result = nullptr;
    ASSERT_EQ(ct_scan_v2(tree, &scan, &result), 0);
    ASSERT_NE(result, nullptr);
    ct_close(tree);
    ct_runtime_free(runtime);
    const uint8_t *data      = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &data, &len, &count, &truncated), 0);
    ASSERT_EQ(count, 1U);
    ASSERT_EQ(len, key.size() + value.size() + 17);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(data + len - value.size()), value.size()), value);
    EXPECT_GT(config.runtime->snapshot().allocated.routing_bytes, baseline);
    ct_scan_result_free(result);
    EXPECT_LT(config.runtime->snapshot().allocated.routing_bytes, baseline);
}

TEST(ScanOwnerCAbi, AsyncPollTransfersOwnerAndAbandonedFutureDrainsSafely)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    auto *abandoned = ct_scan_async(tree, nullptr, 0, nullptr, 0, nullptr, 0, 1, 128, 0, 0);
    ASSERT_NE(abandoned, nullptr);
    ct_future_free(abandoned);
    ct_close(tree);
    tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    auto *future = ct_scan_async(tree, nullptr, 0, nullptr, 0, nullptr, 0, 1, 128, 0, 0);
    ASSERT_NE(future, nullptr);
    int32_t         done   = 0;
    ct_scan_result *result = nullptr;
    while (done == 0) {
        ASSERT_EQ(ct_future_poll_scan_v2(future, &done, &result), 0);
        std::this_thread::yield();
    }
    ASSERT_NE(result, nullptr);
    ct_close(tree);
    ct_runtime_free(runtime);
    const uint8_t *data      = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &data, &len, &count, &truncated), 0);
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(data[len - 1], value);
    ct_scan_result_free(result);
}
} // namespace crowdb::tree
