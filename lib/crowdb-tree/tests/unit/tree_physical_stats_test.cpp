// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/tree.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreePhysicalStats, ContainerCapacitiesRemainClassifiedAcrossHandoffAndSplit)
{
    Config config;
    config.leaf_split_bytes = 1024;
    config.frame_bytes      = 4096;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_NE(core, nullptr);
    for (size_t i = 0; i < 32; ++i) {
        ASSERT_TRUE(tree.put(std::string(64, 'k') + std::to_string(i), std::string(256, 'v')).ok());
    }
    for (size_t phase = 0; phase < 2; ++phase) {
        if (phase != 0) {
            ASSERT_TRUE(tree.flush().ok());
            ASSERT_GT(tree.leaf_count(), 1U);
        }
        core->binding()->drain();
        const auto  stats = tree.tree_state_stats();
        const auto &r     = stats.resources;
        EXPECT_GE(r.payload_bytes, 32U * 256);
        EXPECT_GT(r.metadata_bytes, 0U);
        EXPECT_EQ(r.allocated_bytes, r.payload_bytes + r.frame_bytes + r.metadata_bytes + r.io_bytes + r.scratch_bytes);
    }
}

TEST(TreePhysicalStats, RetainedValueStaysChargedAcrossClearUntilItsActualOwnerDrops)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_NE(core, nullptr);
    ASSERT_TRUE(tree.put("key", std::string(8192, 'v')).ok());
    auto retained = tree.get_view("key");
    ASSERT_TRUE(retained.found());
    const auto before = tree.tree_state_stats();
    EXPECT_GE(before.resources.payload_bytes, 8192U);
    ASSERT_TRUE(tree.clear().ok());
    core->binding()->drain();
    const auto held = tree.tree_state_stats();
    EXPECT_GE(held.resources.payload_bytes, 8192U);
    EXPECT_EQ(retained.value().size(), 8192U);
    const auto &r = held.resources;
    EXPECT_EQ(r.allocated_bytes, r.payload_bytes + r.frame_bytes + r.metadata_bytes + r.io_bytes + r.scratch_bytes);
    retained = {};
    core->binding()->drain();
    const auto released = tree.tree_state_stats();
    EXPECT_LT(released.resources.payload_bytes, held.resources.payload_bytes);
}
} // namespace crowdb::tree
