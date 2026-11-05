// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_maintenance.h"
#include "btree/tree_core_geometry.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

#include <limits>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreGeometry, SmallRecordThresholdsIncludeOneFrameOverhead)
{
    Config config;
    config.frame_bytes = 4096;
    for (const size_t target : {80U, 128U, 160U}) {
        config.leaf_split_bytes = target;
        config.leaf_merge_bytes = target / 4;
        PageMaintenanceLimits pages;
        ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
        EXPECT_EQ(pages.target_page_bytes, target + kFrameHeaderSize + kFrameTrailerSize);
        EXPECT_EQ(pages.merge_trigger_bytes, (target / 4) + kFrameHeaderSize + kFrameTrailerSize);
        EXPECT_LT(pages.merge_target_bytes, pages.target_page_bytes);
        EXPECT_EQ(pages.primary_page_bytes, 4096U);
    }
}

TEST(TreeCoreGeometry, ExplicitSplitCapsAtTreeFrameIndependentlyOfTransportFrames)
{
    Config                config;
    PageMaintenanceLimits pages;
    config.leaf_split_bytes = 65536;
    ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
    EXPECT_EQ(pages.target_page_bytes, config.frame_bytes);
    config.frame_bytes      = 128 * 1024;
    config.leaf_split_bytes = 128 * 1024;
    ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
    EXPECT_EQ(pages.target_page_bytes, config.frame_bytes);
    config.frame_bytes = 4096;
    ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
    EXPECT_EQ(pages.target_page_bytes, 4096U);
    config.leaf_merge_bytes = 0;
    ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
    EXPECT_EQ(pages.merge_trigger_bytes, kFrameHeaderSize + kFrameTrailerSize);
}

TEST(TreeCoreGeometry, ExceptionalReadCapacityDoesNotChangeOrdinaryPageTargets)
{
    Config config;
    config.frame_bytes = 4096;
    EXPECT_EQ(physical_frame_capacity(config), 4096U);
    config.max_key_size = 6000;
    EXPECT_EQ(physical_frame_capacity(config), 8192U);
    PageMaintenanceLimits pages;
    ASSERT_TRUE(configure_leaf_geometry(config, &pages).ok());
    EXPECT_EQ(pages.primary_page_bytes, 4096U);
    config.max_key_size = SIZE_MAX;
    EXPECT_EQ(physical_frame_capacity(config), kMaxPhysicalPageBytes);
}

TEST(TreeCoreGeometry, InvalidAndOverflowingGeometryFailsBeforePlanning)
{
    Config                config;
    PageMaintenanceLimits pages;
    for (const size_t target : {size_t{0}, std::numeric_limits<size_t>::max()}) {
        config.leaf_split_bytes = target;
        EXPECT_EQ(configure_leaf_geometry(config, &pages).code(), Code::kInvalidArgument);
    }
    config.leaf_split_bytes = 128;
    config.leaf_merge_bytes = std::numeric_limits<size_t>::max();
    EXPECT_EQ(configure_leaf_geometry(config, &pages).code(), Code::kInvalidArgument);
}
} // namespace
} // namespace crowdb::tree
