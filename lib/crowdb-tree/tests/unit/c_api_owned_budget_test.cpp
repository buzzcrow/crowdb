// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"
#include "crowdb-tree/runtime_c_api.h"

#include <gtest/gtest.h>

#include <limits>

namespace crowdb::tree
{
TEST(OwnedBudgetCAbi, ReturnedScanControlWindowOutlivesEveryOriginalHandle)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto          account = TreeCoreAccess::get(*c_api_tree(tree))->binding()->account();
    const uint8_t key     = 'k';
    const uint8_t value   = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    ct_scan_options_v2 scan{};
    scan.struct_size       = sizeof(scan);
    ct_scan_result *result = nullptr;
    ASSERT_EQ(ct_scan_v2(tree, &scan, &result), 0);
    ct_owned_budget *budget = nullptr;
    ASSERT_EQ(ct_scan_result_owned_budget(result, &budget), 0);
    ASSERT_NE(budget, nullptr);
    ct_scan_result_free(result);
    ct_close(tree);
    ct_runtime_free(runtime);
    const auto index  = static_cast<size_t>(AllocationKind::kMetadata);
    const auto before = account->snapshot().physical[index];
    ASSERT_EQ(ct_owned_budget_claim(budget, 64), 0);
    auto *control = new uint8_t[64];
    ct_owned_budget_record(budget, 64, 1);
    EXPECT_EQ(account->snapshot().physical[index], before + 64);
    delete[] control;
    ct_owned_budget_record(budget, 64, 0);
    ct_owned_budget_release(budget, 64);
    EXPECT_EQ(account->snapshot().physical[index], before);
    EXPECT_NE(ct_owned_budget_claim(budget, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(account->snapshot().physical[index], before);
    ct_owned_budget_free(budget);
    EXPECT_EQ(account->snapshot().physical[index], 0U);
}

TEST(OwnedBudgetCAbi, LegacyCallerOwnedBudgetDoesNotClaimNativeCapacity)
{
    ct_owned_budget *budget = nullptr;
    ASSERT_EQ(c_api_owned_budget({}, &budget), 0);
    EXPECT_EQ(budget, nullptr);
    EXPECT_EQ(ct_owned_budget_claim(budget, 64), 0);
    ct_owned_budget_record(budget, 64, 1);
    ct_owned_budget_record(budget, 64, 0);
    ct_owned_budget_release(budget, 64);
    ct_owned_budget_free(budget);
}
} // namespace crowdb::tree
