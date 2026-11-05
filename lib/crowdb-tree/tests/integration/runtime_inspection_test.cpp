// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestInspection
{
    Config                        config;
    std::shared_ptr<MemPageStore> store = std::make_shared<MemPageStore>(4096);
    std::unique_ptr<Crowdbtree>   tree;
    uint64_t                      runtime_bytes = 0;

    TestInspection()
    {
        EXPECT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
        runtime_bytes                               = config.runtime->snapshot().allocated.routing_bytes;
        config.page_store                           = store.get();
        config.backend_owner                        = store;
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 3000;
        config.runtime_binding.limits.io_operations = 1;
        EXPECT_TRUE(Crowdbtree::open(config, &tree).ok());
    }
};

TEST(RuntimeInspection, LogicalOverlayIsEncodedWithoutChangingPublicationOrAllocatingPids)
{
    TestInspection test;
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    auto        metrics         = TreeCoreAccess::get(*test.tree)->publisher().work_metrics();
    const auto  encoding_before = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    const auto  before          = test.tree->version();
    NativeFrame frame;
    uint64_t    version = 0;
    uint64_t    root    = 0;
    uint32_t    deltas  = 0;
    auto        status  = test.tree->inspect_page({}, before, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encoding_before + 1);
    metrics.reset();
    EXPECT_EQ(before, version);
    EXPECT_EQ(before, test.tree->version());
    EXPECT_EQ(root, frame.page_id);
    ASSERT_TRUE(frame_validate(frame.frame.data(), frame.frame.size()));
    LeafFrameView leaf(frame.frame.data(), frame.frame.size());
    ASSERT_EQ(leaf.count(), 1U);
    EXPECT_EQ(leaf.key(0).to_string(), "key");
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "value");
    test.tree.reset();
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "value");
    EXPECT_GE(test.config.runtime->snapshot().allocated.routing_bytes, test.runtime_bytes + frame.frame.capacity());
    frame = {};
    EXPECT_EQ(test.config.runtime->snapshot().allocated.routing_bytes, test.runtime_bytes);
}

TEST(RuntimeInspection, ColdIndexAndLeafUseOneIoPermitAndRejectStaleOrInvalidPaths)
{
    TestInspection test;
    Batch          batch;
    for (size_t i = 0; i < 100; ++i) {
        batch.ops.push_back({.key = std::to_string(1000 + i), .kind = OpKind::kPut, .value = std::string(100, 'v')});
    }
    ASSERT_TRUE(test.tree->apply(1, batch).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    ASSERT_TRUE(Crowdbtree::open(test.config, &test.tree).ok());
    NativeFrame frame;
    uint64_t    version = 0;
    uint64_t    root    = 0;
    uint32_t    deltas  = 0;
    auto        status  = test.tree->inspect_page({}, kInvalidPageId, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(frame_page_type(frame.frame.data()), page_type::kInnerBase);
    const auto revision = version;
    status              = test.tree->inspect_page({0}, revision, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(frame_page_type(frame.frame.data()), page_type::kLeafBase);
    EXPECT_EQ(test.tree->inspect_page({UINT32_MAX}, revision, &frame, &version, &root, &deltas).code(),
              Code::kInvalidArgument);
    ASSERT_TRUE(test.tree->apply(2, {}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_EQ(test.tree->inspect_page({}, revision, &frame, &version, &root, &deltas).code(), Code::kUnavailable);
    EXPECT_EQ(
        test.tree->inspect_page(std::vector<uint32_t>(33), kInvalidPageId, &frame, &version, &root, &deltas).code(),
        Code::kInvalidArgument);
}
} // namespace
} // namespace crowdb::tree
