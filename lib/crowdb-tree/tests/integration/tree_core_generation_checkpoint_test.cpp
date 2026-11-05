// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
namespace
{
struct TestGenerationBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestGenerationBackend()
    {
        store.device = device;
    }
};

TEST(TreeCoreGenerationCheckpoint, ClearPersistsZeroCoverageWithoutInvalidatingOldOwnedViews)
{
    auto   backend = std::make_shared<TestGenerationBackend>();
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store                           = backend->device.get();
    config.async_page_store                     = &backend->store;
    config.backend_owner                        = backend;
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.io_operations = 1;
    std::unique_ptr<Crowdbtree> tree;
    auto                        status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'p');
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "old", .kind = OpKind::kPut, .value = value}}}).ok());
    uint64_t coverage = 0;
    uint64_t sequence = 0;
    status            = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(coverage, 1U);
    ASSERT_EQ(sequence, 1U);

    auto                                     *core       = TreeCoreAccess::get(*tree);
    const auto                                generation = core->capture()->generation;
    std::shared_ptr<PublishedSnapshotContext> captured;
    status = core->capture_snapshot(&captured);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView retained;
    status = captured->get("old", &retained);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(retained.found());

    status = tree->clear();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->capture()->generation, generation + 1);
    EXPECT_EQ(core->capture()->coverage, 0U);
    status = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 0U);
    EXPECT_EQ(sequence, 2U);
    EXPECT_EQ(tree->durable_snapshot_last_applied_slot(), 0U);
    GetView absent;
    status = core->get("old", &absent);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(absent.found());
    GetView old_again;
    status = captured->get("old", &old_again);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(old_again.found());
    EXPECT_EQ(old_again.value().to_string(), value);
    EXPECT_EQ(retained.value().to_string(), value);

    tree.reset();
    captured.reset();
    status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->durable_snapshot_last_applied_slot(), 0U);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    core   = TreeCoreAccess::get(*tree);
    status = core->get("old", &absent);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(absent.found());
    EXPECT_EQ(retained.value().to_string(), value);
    ASSERT_TRUE(tree->put("new", "after-clear").ok());
    status = tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 1U);
    EXPECT_EQ(sequence, 3U);
}
} // namespace
} // namespace crowdb::tree
