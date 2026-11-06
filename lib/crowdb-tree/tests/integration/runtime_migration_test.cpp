// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void expect_canonical_native(Crowdbtree &tree, bool expect_inner, bool expect_overflow)
{
    std::unique_ptr<NativeFrameIterator> cursor;
    auto                                 status = tree.open_native_frame_iterator(nullptr, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    uint64_t inner    = 0;
    uint64_t overflow = 0;
    uint64_t frames   = 0;
    bool     done     = false;
    while (!done && frames < 1024) {
        std::optional<NativeFrameView> frame;
        status = cursor->next_owned(&frame, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (!frame) {
            continue;
        }
        ++frames;
        EXPECT_TRUE(frame_validate(frame->bytes.data(), frame->bytes.size()));
        EXPECT_EQ(frame_u64(frame->bytes.data(), fh::kSelfpage_id), frame->page_id);
        inner += static_cast<uint64_t>(frame_page_type(frame->bytes.data()) == page_type::kInnerBase);
        overflow += static_cast<uint64_t>(frame_page_type(frame->bytes.data()) == page_type::kOverflowFrame);
    }
    EXPECT_TRUE(done);
    EXPECT_GT(frames, 0U);
    EXPECT_EQ(inner != 0, expect_inner);
    EXPECT_EQ(overflow != 0, expect_overflow);
}

TEST(RuntimeMigration, LegacyBlockCheckpointOpensOnTheSamePathAndRemainsDurable)
{
    tree_test::TempDir              directory("runtime_migrate_legacy_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    config.page_store       = store.get();
    config.frame_bytes      = 4096;
    config.max_inline_value = 64;
    config.compression      = compress_algo::kNone;
    std::unique_ptr<Crowdbtree> tree;
    auto                        status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string large(12000, 'v');
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "alpha", .kind = OpKind::kPut, .value = large}}}).ok());
    ASSERT_TRUE(tree->apply(2, Batch{.ops = {{.key = "beta", .kind = OpKind::kPut, .value = "deleted"}}}).ok());
    ASSERT_TRUE(tree->apply(3, Batch{.ops = {{.key = "beta", .kind = OpKind::kDelete, .value = {}}}}).ok());
    ASSERT_TRUE(tree->flush().ok());
    uint64_t coverage = 0;
    uint64_t sequence = 0;
    status            = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(coverage, 3U);
    tree.reset();
    store.reset();

    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    config.page_store = store.get();
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(tree->durable_snapshot_last_applied_slot(), 3U);
    auto *core = TreeCoreAccess::get(*tree);
    ASSERT_NE(core, nullptr);
    GetView found;
    status = core->get("alpha", &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), large);
    EXPECT_EQ(found.slot(), 1U);
    found  = {};
    status = core->get("beta", &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(found.found());
    std::vector<scan_entry> rows;
    bool                    truncated = true;
    status                            = tree->scan({}, {}, {}, 16, 64 * 1024, false, 0, &rows, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_FALSE(truncated);
    EXPECT_EQ(rows.front().key, "alpha");
    EXPECT_EQ(rows.front().value, large);
    expect_canonical_native(*tree, false, true);
    // A checkpoint with no writes reuses the legacy primary and overflow extents.
    status = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(coverage, 3U);
    tree.reset();
    store.reset();
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    config.page_store = store.get();
    status            = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    expect_canonical_native(*tree, false, true);
    ASSERT_TRUE(tree->apply(4, Batch{.ops = {{.key = "gamma", .kind = OpKind::kPut, .value = "runtime"}}}).ok());
    status = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 4U);
    tree.reset();
    store.reset();
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    config.page_store = store.get();
    status            = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->durable_snapshot_last_applied_slot(), 4U);
    core   = TreeCoreAccess::get(*tree);
    status = core->get("gamma", &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), "runtime");
}

TEST(RuntimeMigration, LegacyInnerTreeRemainsColdReadableAndExportsCanonicalFrames)
{
    tree_test::TempDir              directory("runtime_migrate_inner_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    config.page_store       = store.get();
    config.frame_bytes      = 4096;
    config.max_inline_value = 64;
    config.leaf_split_bytes = 512;
    config.max_delta_len    = 0;
    config.leaf_merge_bytes = 128;
    config.inner_max_keys   = 3;
    config.compression      = compress_algo::kNone;
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    const std::string value(32, 'v');
    for (uint64_t slot = 1; slot <= 64; ++slot) {
        ASSERT_TRUE(
            tree->apply(
                    slot,
                    Batch{.ops = {{.key = "key" + std::to_string(1000 + slot), .kind = OpKind::kPut, .value = value}}})
                .ok());
    }
    ASSERT_TRUE(tree->flush().ok());
    uint64_t coverage = 0;
    ASSERT_TRUE(tree->snapshot(&coverage).ok());
    ASSERT_EQ(coverage, 64U);
    ASSERT_GT(tree->tree_summary().reachable_inner_pages, 0U);
    tree.reset();
    store.reset();
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    for (size_t reopen = 0; reopen < 2; ++reopen) {
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
        config.page_store = store.get();
        auto status       = Crowdbtree::open(config, &tree);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(tree->durable_snapshot_last_applied_slot(), 64U);
        EXPECT_GT(tree->tree_state_stats().logical.inner_pages, 0U);
        GetView found;
        status = TreeCoreAccess::get(*tree)->get("key1064", &found);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(found.found());
        EXPECT_EQ(found.value().to_string(), value);
        EXPECT_EQ(found.slot(), 64U);
        found = {};
        std::vector<scan_entry> rows;
        bool                    truncated = true;
        status                            = tree->scan({}, {}, {}, 64, 64 * 1024, false, 0, &rows, &truncated);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_FALSE(truncated);
        EXPECT_EQ(rows.size(), 64U);
        expect_canonical_native(*tree, true, false);
        status = tree->snapshot(&coverage);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(coverage, 64U);
        tree.reset();
        store.reset();
    }
}

} // namespace
} // namespace crowdb::tree
