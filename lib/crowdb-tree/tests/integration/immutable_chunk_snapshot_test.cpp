// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/runtime_range_rebuild.h"
#include "common/test_tree_core_chunk.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
TEST(ImmutableChunkSnapshot, ColdPackCanRebuildFixedRangeAfterSourceWriterCloses)
{
    test_chunk::TestCoreChunk source;
    ASSERT_TRUE(source.open().ok());
    Batch batch{
        .ops = {{.key = "a", .kind = OpKind::kPut, .value = "left"},
                {.key = "z", .kind = OpKind::kPut, .value = std::string(9000, 'v')}}
    };
    ASSERT_TRUE(source.tree->apply(1, batch).ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    source.tree.reset();
    ASSERT_TRUE(source.open().ok());
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(*source.tree)->capture_snapshot(&snapshot).ok());
    ASSERT_TRUE(source.tree->apply(2, Batch{.ops = {{.key = "z", .kind = OpKind::kPut, .value = "new"}}}).ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    source.tree.reset();

    GetView value;
    ASSERT_TRUE(snapshot->get("z", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(9000, 'v'));
    test_chunk::TestCoreChunk destination;
    destination.config.key_range = KeyRange::bounded("m", {});
    ASSERT_TRUE(destination.open().ok());
    auto              *core = TreeCoreAccess::get(*destination.tree);
    RuntimeRangeImport staging;
    // The fixed-range planner reserves its observed import shape.
    auto status = copy_snapshot_range(snapshot, *core, destination.config.key_range, staging);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(core->install_readable(std::move(staging.catalog), staging.credit).ok());
    ASSERT_TRUE(destination.tree->snapshot().ok());
    EXPECT_EQ(staging.stats.entries_emitted, 1U);
    EXPECT_EQ(staging.stats.pages_reused, 0U);
    EXPECT_GT(staging.stats.pages_rebuilt, 0U);
    snapshot.reset();
    value = {};
    destination.tree.reset();
    ASSERT_TRUE(destination.open().ok());
    EXPECT_EQ(destination.tree->get_view("z").value().to_string(), std::string(9000, 'v'));
}
} // namespace crowdb::tree
