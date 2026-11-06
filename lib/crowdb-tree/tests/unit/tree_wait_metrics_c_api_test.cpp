// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_wait_metrics_c_api.h"
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeWaitMetricsCAbi, InvalidSchemaNeverMutatesOutputAndCollectorRetainsLateWait)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    auto         metrics = TreeCoreAccess::get(*c_api_tree(tree))->publisher().work_metrics();
    TreeWaitSpan wait;
    wait.begin(metrics, TreeWaitClass::Routing, 100);
    ct_tree_wait_snapshot_v1 totals{};
    totals.struct_size         = sizeof(totals);
    totals.version             = 2;
    totals.classes[1].attempts = 99;
    EXPECT_EQ(ct_tree_wait_totals(tree, &totals), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(totals.classes[1].attempts, 99U);
    totals.version = 1;
    --totals.struct_size;
    EXPECT_EQ(ct_work_metrics_collector_wait_totals(collector, &totals), static_cast<int32_t>(Code::kInvalidArgument));
    ++totals.struct_size;
    ASSERT_EQ(ct_tree_wait_totals(tree, &totals), 0);
    EXPECT_EQ(totals.classes[1].inflight, 1U);
    ct_close(tree);
    metrics.reset();
    ASSERT_EQ(ct_work_metrics_collector_wait_totals(collector, &totals), 0);
    EXPECT_EQ(totals.classes[1].inflight, 1U);
    wait.finish(TreeWaitOutcome::Granted, 175);
    ASSERT_EQ(ct_work_metrics_collector_wait_totals(collector, &totals), 0);
    EXPECT_EQ(totals.classes[1].inflight, 0U);
    EXPECT_EQ(totals.classes[1].total_count, 1U);
    EXPECT_EQ(totals.classes[1].total_sum_ns, 75U);
    EXPECT_EQ(totals.classes[1].outcomes[0], 1U);
    ct_work_metrics_collector_free(collector);
}
} // namespace crowdb::tree
