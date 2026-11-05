// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/apply_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class AdmissionCAbi : public testing::Test
{
  protected:
    ct_runtime *runtime = nullptr;
    ct_tree    *tree    = nullptr;

    void SetUp() override
    {
        ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
        ct_options config{};
        ASSERT_EQ(ct_open_with_runtime(&config, runtime, nullptr, &tree), 0);
    }

    void TearDown() override
    {
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    TreeCore &core()
    {
        return *TreeCoreAccess::get(*c_api_tree(tree));
    }
};

TEST_F(AdmissionCAbi, UnarmedHandleOutlivesTreeWithoutBlockingClose)
{
    ct_apply_permit *permit = nullptr;
    ASSERT_EQ(ct_apply_prepare(tree, nullptr, 0, 0, &permit), 0);
    ct_close(tree);
    tree = nullptr;
    ct_runtime_free(runtime);
    runtime = nullptr;
    EXPECT_EQ(ct_apply_arm(permit), static_cast<int32_t>(Code::kUnavailable));
    ct_apply_permit_free(permit);
}

TEST_F(AdmissionCAbi, AcceptedCopyOutlivesBothCallerHandles)
{
    uint8_t          key   = 'k';
    uint8_t          value = 'v';
    ct_kv_ref        input{.key = &key, .key_len = 1, .value = &value, .value_len = 1, .kind = 0};
    ct_apply_permit *permit = nullptr;
    ASSERT_EQ(ct_apply_prepare(tree, &input, 1, 0, &permit), 0);
    key   = 'x';
    value = 'y';
    ASSERT_EQ(ct_apply_arm(permit), 0);
    ct_future *future = nullptr;
    ASSERT_EQ(ct_apply_submit(permit, 1, &future), 0);
    ASSERT_NE(future, nullptr);
    ct_apply_permit_free(permit);
    ct_future_free(future);
    core().binding()->lane()->drain();
    GetView result;
    ASSERT_TRUE(core().get(Slice("k", 1), &result).ok());
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.value().to_view(), "v");
    EXPECT_EQ(core().contiguous_slot(), 1U);
}

TEST_F(AdmissionCAbi, PollConsumesOneTerminalAndCancellationReleasesArm)
{
    ct_apply_permit *cancelled = nullptr;
    ASSERT_EQ(ct_apply_prepare(tree, nullptr, 0, 0, &cancelled), 0);
    ASSERT_EQ(ct_apply_arm(cancelled), 0);
    ct_apply_permit_free(cancelled);
    ASSERT_TRUE(core().clear().ok());
    ct_apply_permit *permit = nullptr;
    ASSERT_EQ(ct_apply_prepare(tree, nullptr, 0, 1, &permit), 0);
    ASSERT_EQ(ct_apply_arm(permit), 0);
    ct_future *future = nullptr;
    ASSERT_EQ(ct_apply_submit(permit, 1, &future), 0);
    ct_future *duplicate = nullptr;
    EXPECT_EQ(ct_apply_submit(permit, 1, &duplicate), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(duplicate, nullptr);
    core().binding()->lane()->drain();
    int32_t done = 0;
    EXPECT_EQ(ct_future_poll(future, &done, nullptr, nullptr, nullptr), 0);
    EXPECT_EQ(done, 1);
    ct_apply_permit_free(permit);
    EXPECT_EQ(core().contiguous_slot(), 1U);
}
} // namespace
} // namespace crowdb::tree
