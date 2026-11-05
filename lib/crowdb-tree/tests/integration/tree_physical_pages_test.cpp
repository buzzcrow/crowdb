// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_core_batch.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "common/test_tree_core_chunk.h"
#include "crowdb-tree/backend/page_store.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/readable_page_catalog.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
namespace
{
struct TestPhysicalPages
{
    Config                        config;
    std::shared_ptr<MemPageStore> store = std::make_shared<MemPageStore>(4096);
    std::shared_ptr<TreeCore>     core;

    TestPhysicalPages()
    {
        TreeRuntimeOptions host;
        host.workers         = 1;
        host.backend_workers = 1;
        EXPECT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.page_store       = store.get();
        config.backend_owner    = store;
        config.frame_bytes      = 4096;
        config.max_inline_value = 64;
        config.max_key_size     = 6000;
        config.leaf_split_bytes = 3000;
        const auto status       = TreeCore::open(config, &core);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
};
} // namespace

TEST(TreePhysicalPages, ExceptionalLeafAndIndexPagesPersistWithoutAnotherStructuralSplit)
{
    TestPhysicalPages test;
    Batch             batch;
    for (const auto prefix : {'a', 'b', 'c'}) {
        batch.ops.push_back({.key = std::string(5000, prefix), .kind = OpKind::kPut, .value = "value"});
    }
    ASSERT_TRUE(test.core->apply(1, batch).ok());
    const auto flushed = test.core->flush();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    const auto before = test.core->capture();
    ASSERT_GE(before->mapping.get(before->root_page_id)->level, 1U);
    const auto persisted = test.core->checkpoint();
    ASSERT_TRUE(persisted.ok()) << persisted.to_string();
    const auto after = test.core->capture();
    EXPECT_EQ(after->root_page_id, before->root_page_id);
    EXPECT_EQ(after->next_page_id, before->next_page_id);
    ASSERT_TRUE(after->snapshot_baseline);
    size_t exceptional = 0;
    for (const auto &[pid, frame] : after->snapshot_baseline->pages->frames) {
        (void)pid;
        exceptional += frame.frame_bytes > test.config.frame_bytes ? 1 : 0;
        EXPECT_LE(frame.frame_bytes, kMaxPhysicalPageBytes);
    }
    EXPECT_GE(exceptional, 4U);
    test.core.reset();
    const auto reopened = TreeCore::open(test.config, &test.core);
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    for (const auto &op : batch.ops) {
        GetView value;
        ASSERT_TRUE(test.core->get(op.key, &value).ok());
        ASSERT_TRUE(value.found());
        EXPECT_EQ(value.value().to_string(), "value");
    }
    uint64_t sequence = 0;
    ASSERT_TRUE(test.core->checkpoint(nullptr, &sequence).ok());
    EXPECT_EQ(sequence, 1U);
}

TEST(TreePhysicalPages, ChunkFramingAndRecoveryAcceptAnExceptionalTreePage)
{
    test_chunk::TestCoreChunk test;
    test.config.max_key_size = 100000;
    ASSERT_TRUE(test.open().ok());
    const std::string key(90000, 'k');
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = key, .kind = OpKind::kPut, .value = "value"}}}).ok());
    const auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.tree.reset();
    const auto reopened = test.open();
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    const auto value = test.tree->get_view(key);
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), "value");
    uint64_t sequence = 0;
    ASSERT_TRUE(test.tree->snapshot(nullptr, &sequence).ok());
    EXPECT_EQ(sequence, 1U);
}

TEST(TreePhysicalPages, FinalPageAboveSafetyBoundReturnsErrorAndPreservesTheCommittedRoot)
{
    TestPhysicalPages test;
    ASSERT_TRUE(test.core->apply(1, Batch{.ops = {{.key = "old", .kind = OpKind::kPut, .value = "durable"}}}).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_FALSE(primary_write_fits(test.config, kMaxPhysicalPageBytes + 1, 1, false));
    const auto view               = test.core->capture();
    auto       page               = std::make_shared<PublishedPage>(*view->mapping.get(view->root_page_id));
    page->metrics.page_size_known = true;
    page->metrics.page_size_bytes = kMaxPhysicalPageBytes + 1;
    auto preparation              = std::make_shared<checkpoint_detail::Preparation>();
    preparation->core             = test.core.get();
    preparation->options          = checkpoint_detail::defaults(*test.core);
    auto   persistence            = std::make_shared<FlushPersistence>(preparation);
    size_t completions            = 0;
    Status result;
    persistence->write({.page         = page,
                        .page_id      = view->root_page_id,
                        .coverage     = view->coverage,
                        .gc_floor     = view->gc_floor,
                        .next_page_id = view->next_page_id,
                        .context      = {}},
                       {}, [&](Status status, std::shared_ptr<const PublishedPage> replacement) {
                           ++completions;
                           result = std::move(status);
                           EXPECT_FALSE(replacement);
                       });
    EXPECT_EQ(completions, 1U);
    EXPECT_EQ(result.code(), Code::kInvalidArgument) << result.to_string();
    EXPECT_EQ(test.core->capture(), view);
    EXPECT_EQ(test.core->durable_coverage(), 1U);
    EXPECT_EQ(test.core->snapshot_sequence(), 1U);
    persistence.reset();
    preparation.reset();
    test.core.reset();
    const auto reopened = TreeCore::open(test.config, &test.core);
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    GetView old;
    ASSERT_TRUE(test.core->get("old", &old).ok());
    ASSERT_TRUE(old.found());
    EXPECT_EQ(old.value().to_string(), "durable");
    EXPECT_EQ(test.core->durable_coverage(), 1U);
}
} // namespace crowdb::tree
