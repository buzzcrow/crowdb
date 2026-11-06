// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
auto use_remaining_memory(Crowdbtree &tree)
{
    auto *core    = TreeCoreAccess::get(tree);
    auto  account = core->binding()->account();
    return detail::test_allocation_account::use_remaining_memory(account, core->read_limits().metadata);
}
} // namespace

TEST(RuntimeRegistry, TreeRegistryAndNamesUseActualRoutingQuotaAndFailedReinitPreservesTheOldRegistry)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    Crowdbtree tree(config);
    const auto before = tree.tree_state_stats().resources;
    tree.init_metrics("runtime", "mem");
    const auto after = tree.tree_state_stats().resources;
    EXPECT_GT(after.metadata_bytes, before.metadata_bytes);
    EXPECT_GT(after.allocated_bytes, before.allocated_bytes);
    EXPECT_GT(after.allocated_bytes + after.reserved_bytes, before.allocated_bytes + before.reserved_bytes);
    const auto width = tree.max_name_len();
    {
        auto held = use_remaining_memory(tree);
        EXPECT_THROW(tree.init_metrics("replacement", "mem"), std::bad_alloc);
        EXPECT_EQ(tree.max_name_len(), width);
    }
    const auto output = tree.flush_metrics_str(1.0, "2026-11-02T00:00:00Z", 0);
    for (const auto *name :
         {"runtime.frontier.published.g", "runtime.frontier.durable.g", "runtime.flush.repair.pending.g",
          "runtime.checkpoint.inflight.g", "runtime.cache.frames.g", "runtime.io.read.inflight.bytes.g",
          "runtime.io.write.inflight.bytes.g", "runtime.candidate.oldest.age.ns.g"}) {
        EXPECT_NE(output.find(name), std::string::npos) << name;
    }
}

TEST(RuntimeRegistry, ExhaustedFlushScratchNeverThrowsAcrossEitherCEntryAndCanRetry)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_tree   *tree = nullptr;
    ct_options options{};
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    c_api_tree(tree)->init_metrics("runtime", "mem");
    {
        auto held = use_remaining_memory(*c_api_tree(tree));
        EXPECT_EQ(ct_flush_metrics_str(tree, 1.0, "now", 0), nullptr);
        EXPECT_EQ(ct_flush_metrics_str_ext(tree, 1.0, "now", 0, 7, 7), nullptr);
    }
    auto *output = ct_flush_metrics_str(tree, 1.0, "now", 0);
    ASSERT_NE(output, nullptr);
    ct_free_string(output);
    ct_close(tree);
    ct_runtime_free(runtime);
}
} // namespace crowdb::tree
