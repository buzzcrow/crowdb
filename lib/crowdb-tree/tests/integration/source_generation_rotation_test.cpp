// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_generation_lease.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(TreeSourceControls, SoftHandoffKeepsProgressAfterClearAndPreparedReplacement)
{
    Config             config;
    TreeRuntimeOptions runtime;
    runtime.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime, &config.runtime).ok());
    config.memtable_flush_entries = 1;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 2;
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_TRUE(tree.put("old", "value").ok());
    for (size_t round = 0; round < 2; ++round) {
        if (round == 0) {
            ASSERT_TRUE(tree.clear().ok());
        }
        else {
            std::shared_ptr<TreeGenerationLease> lease;
            ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
            ASSERT_TRUE(lease->acquire().ok());
            ASSERT_TRUE(lease->commit().ok());
            // Drop the old controller before any automatic maintenance starts.
            lease.reset();
        }
        EXPECT_FALSE(tree.get_view("old").found());
        EXPECT_EQ(core->capture()->coverage, 0U);
        for (size_t i = 0; i < 24; ++i) {
            const auto status = tree.put("key" + std::to_string(i), "value");
            ASSERT_TRUE(status.ok()) << "round=" << round << " key=" << i << " " << status.to_string();
            EXPECT_LT(tree.frozen_table_count(), config.max_memtable_count);
        }
        // No explicit flush drives the assertion: the replacement's soft
        // controller must reach its finite handoff by itself.
        EXPECT_GT(core->capture()->coverage, 0U);
        for (size_t i = 0; i < 24; ++i) {
            auto value = tree.get_view("key" + std::to_string(i));
            ASSERT_TRUE(value.found());
            EXPECT_EQ(value.value().to_string(), "value");
        }
    }
}
} // namespace
} // namespace crowdb::tree
