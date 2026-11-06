// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "snapshot/operation_wait.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeCoreFlushReadBudget, CapturedLeafOnlyTreeLeavesDefaultRoutingCreditForReads)
{
    Config options;
    ASSERT_TRUE(TreeRuntime::create({}, &options.runtime).ok());
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.put("key", "value").ok());
    auto *core = TreeCoreAccess::get(tree);
    ASSERT_EQ(core->capture()->stats.inner_pages, 0U);
    ASSERT_EQ(core->cache()->retained_page_limit(), 256U);
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(core->prepare_flush(&flush).ok());
    auto capture = stdexec::sync_wait(flush->capture_sender());
    if (!capture) {
        FAIL() << "flush capture did not complete";
    }
    ASSERT_TRUE(std::get<0>(*capture).handoff_status.ok());
    GetView    value;
    const auto read = core->get("key", &value);
    EXPECT_TRUE(read.ok()) << read.to_string();
    EXPECT_EQ(value.value().to_string(), "value");
    // Always finish accepted capture before the host's blocking close.
    EXPECT_TRUE(detail::wait_snapshot_operation(flush).ok());
}
} // namespace crowdb::tree
