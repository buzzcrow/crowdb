// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/status.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

namespace crowdb::tree
{
namespace
{
class ScanAsyncV2CAbi : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
        ct_options options{};
        ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
        scan.struct_size = sizeof(scan);
        scan.limit       = 1;
    }

    void TearDown() override
    {
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    void expect_key(const std::string &key, bool more)
    {
        ct_future *future = nullptr;
        ASSERT_EQ(ct_scan_async_v2(tree, &scan, &future), 0);
        ASSERT_NE(future, nullptr);
        int32_t         done   = 0;
        ct_scan_result *result = nullptr;
        while (done == 0) {
            ASSERT_EQ(ct_future_poll_scan_v2(future, &done, &result), 0);
            std::this_thread::yield();
        }
        std::unique_ptr<ct_scan_result, decltype(&ct_scan_result_free)> owner(result, ct_scan_result_free);
        const uint8_t                                                  *data      = nullptr;
        size_t                                                          len       = 0;
        uint64_t                                                        count     = 0;
        int32_t                                                         truncated = 0;
        ASSERT_EQ(ct_scan_result_data(result, &data, &len, &count, &truncated), 0);
        ASSERT_EQ(count, 1U);
        ASSERT_GE(len, key.size() + 17);
        EXPECT_EQ(data[0], key.size());
        EXPECT_EQ(std::string(reinterpret_cast<const char *>(data + 4), key.size()), key);
        EXPECT_EQ(truncated != 0, more);
    }

    ct_runtime        *runtime = nullptr;
    ct_tree           *tree    = nullptr;
    ct_scan_options_v2 scan{};
};
} // namespace

TEST_F(ScanAsyncV2CAbi, EmptyCursorResumesWithoutRepeatingTheFirstWindow)
{
    const uint8_t key   = 'a';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, nullptr, 0, &value, 1), 0);
    ASSERT_EQ(ct_apply_put(tree, 2, &key, 1, &value, 1), 0);
    expect_key("", true);
    scan.has_start_bound = 1;
    expect_key("a", false);
    scan.reverse         = 1;
    scan.start_inclusive = 1;
    expect_key("", false);
}

TEST_F(ScanAsyncV2CAbi, InvalidOptionsFailBeforeReturningAnOwnedFuture)
{
    ct_future *future = nullptr;
    --scan.struct_size;
    EXPECT_EQ(ct_scan_async_v2(tree, &scan, &future), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(future, nullptr);
    ++scan.struct_size;
    scan.strict_byte_budget = 1;
    EXPECT_EQ(ct_scan_async_v2(tree, &scan, &future), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(future, nullptr);
    scan.strict_byte_budget = 0;
    scan.start_len          = 1;
    EXPECT_EQ(ct_scan_async_v2(tree, &scan, &future), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(future, nullptr);
}

TEST_F(ScanAsyncV2CAbi, AbandonedFutureRetainsItsAcceptedOperationUntilClose)
{
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, nullptr, 0, &value, 1), 0);
    ct_future *future = nullptr;
    ASSERT_EQ(ct_scan_async_v2(tree, &scan, &future), 0);
    ASSERT_NE(future, nullptr);
    ct_future_free(future);
    ct_close(tree);
    tree = nullptr;
}
} // namespace crowdb::tree
