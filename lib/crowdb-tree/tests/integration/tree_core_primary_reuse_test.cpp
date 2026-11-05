// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"
#include "snapshot/primary_backing.h"
#include "snapshot/published_native.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestCountedStore : MemPageStore
{
    TestCountedStore() : MemPageStore(4096)
    {
    }

    mutable std::atomic<size_t> reads{0};
    std::atomic<size_t>         primary_writes{0};
    std::atomic<size_t>         total_writes{0};
    std::atomic<size_t>         barriers{0};

    Status write_at(uint64_t address, const uint8_t *data, size_t bytes) override
    {
        total_writes.fetch_add(1);
        return MemPageStore::write_at(address, data, bytes);
    }

    Status sync() override
    {
        barriers.fetch_add(1);
        return MemPageStore::sync();
    }

    Status read_at(uint64_t address, uint8_t *data, size_t bytes) const override
    {
        reads.fetch_add(1);
        return MemPageStore::read_at(address, data, bytes);
    }

    Status write_typed_at(PagePurpose purpose, uint64_t address, const uint8_t *data, size_t bytes) override
    {
        if (purpose == PagePurpose::kBtreePage) {
            primary_writes.fetch_add(1);
        }
        return write_at(address, data, bytes);
    }
};

struct TestPrimaryReuse
{
    Config                            config;
    std::shared_ptr<TestCountedStore> store = std::make_shared<TestCountedStore>();
    std::shared_ptr<TreeCore>         core;

    TestPrimaryReuse()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.page_store                           = store.get();
        config.backend_owner                        = store;
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 3000;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
        const auto status                           = TreeCore::open(config, &core);
        EXPECT_TRUE(status.ok()) << status.to_string();
    }

    [[nodiscard]] std::shared_ptr<const PublishedPage> page() const
    {
        auto view = core->capture();
        return view->mapping.get(view->root_page_id);
    }
};

TEST(TreeCorePrimaryReuse, ColdInlineAndOverflowCheckpointReusesWithoutPrimaryIo)
{
    TestPrimaryReuse  test;
    const std::string large(12000, 'v');
    ASSERT_TRUE(test.core
                    ->apply(1,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = large},
                                        {.key = "b", .kind = OpKind::kPut, .value = "small"}}
    })
                    .ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    const auto before = test.page();
    ASSERT_TRUE(before->primary);
    const auto reads  = test.store->reads.load();
    const auto writes = test.store->primary_writes.load();
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_EQ(test.store->reads.load(), reads);
    EXPECT_EQ(test.store->primary_writes.load(), writes);
    EXPECT_EQ(test.page()->leaf->bases.front().backing, before->leaf->bases.front().backing);
    EXPECT_EQ(test.page(), before);
    test.core.reset();
    auto status = TreeCore::open(test.config, &test.core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView value;
    ASSERT_TRUE(test.core->get("a", &value).ok());
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), large);
    const auto reopened_reads = test.store->reads.load();
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_EQ(test.store->reads.load(), reopened_reads);
    EXPECT_EQ(test.store->primary_writes.load(), writes);
}

TEST(TreeCorePrimaryReuse, UnchangedCheckpointIsZeroIoAndKeepsSequenceAfterReopen)
{
    TestPrimaryReuse test;
    ASSERT_TRUE(
        test.core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = std::string(12000, 'v')}}}).ok());
    uint64_t sequence = 0;
    ASSERT_TRUE(test.core->checkpoint(nullptr, &sequence).ok());
    ASSERT_GT(sequence, 0U);
    const auto writes    = test.store->total_writes.load();
    const auto barriers  = test.store->barriers.load();
    uint64_t   unchanged = 0;
    ASSERT_TRUE(test.core->checkpoint(nullptr, &unchanged).ok());
    EXPECT_EQ(unchanged, sequence);
    EXPECT_EQ(test.store->total_writes.load(), writes);
    EXPECT_EQ(test.store->barriers.load(), barriers);
    test.core.reset();
    ASSERT_TRUE(TreeCore::open(test.config, &test.core).ok());
    ASSERT_TRUE(test.core->checkpoint(nullptr, &unchanged).ok());
    EXPECT_EQ(unchanged, sequence);
    EXPECT_EQ(test.store->total_writes.load(), writes);
    EXPECT_EQ(test.store->barriers.load(), barriers);
}

TEST(TreeCorePrimaryReuse, ExactReceiptReuseIsNotReportedAsDeferredRepresentationWork)
{
    TestPrimaryReuse test;
    ASSERT_TRUE(test.core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    CheckpointOptions options;
    options.write.frames.encode.frame_bytes        = 4096;
    options.write.frames.encode.inline_value_bytes = 64;
    options.write.write.max_frame_bytes            = 4096;
    std::shared_ptr<CheckpointOperation> operation;
    ASSERT_TRUE(test.core->prepare_checkpoint(options, &operation).ok());
    ASSERT_TRUE(detail::wait_snapshot_operation(operation).ok());
    EXPECT_TRUE(operation->result().durable_committed);
    EXPECT_EQ(operation->result().primary_frames, 1U);
    EXPECT_EQ(operation->result().reused_frames, 1U);
    EXPECT_EQ(operation->result().written_frames, 0U);
    EXPECT_EQ(operation->result().rebased_pages, 0U);
    EXPECT_EQ(operation->result().rebase_deferred, 0U);
}

TEST(TreeCorePrimaryReuse, LaterOverlayAndGcFloorInvalidateOldReceipts)
{
    TestPrimaryReuse test;
    ASSERT_TRUE(test.core
                    ->apply(1,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = "old"},
                                        {.key = "b", .kind = OpKind::kDelete, .value = ""}}
    })
                    .ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    auto old = test.page();
    ASSERT_TRUE(old->primary);
    const auto &old_summary = old->primary->frame.summary;
    if (!old_summary) {
        FAIL() << "checkpoint omitted its primary summary";
    }
    EXPECT_EQ(old_summary->encoded_entries, 2U);
    auto writes = test.store->primary_writes.load();
    ASSERT_TRUE(test.core->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "new"}}}).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_GT(test.store->primary_writes.load(), writes);
    EXPECT_NE(test.page()->leaf->bases.front().backing, old->leaf->bases.front().backing);
    writes = test.store->primary_writes.load();
    test.core->request_gc_floor(2);
    ASSERT_TRUE(test.core->complete_noop(3).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_GT(test.store->primary_writes.load(), writes);
    EXPECT_EQ(test.core->capture()->gc_floor, 2U);
    const auto latest = test.page();
    ASSERT_TRUE(latest->primary);
    const auto &summary = latest->primary->frame.summary;
    if (!summary) {
        FAIL() << "checkpoint omitted its primary summary";
    }
    EXPECT_EQ(summary->encoded_entries, 1U);
    GetView value;
    ASSERT_TRUE(test.core->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), "new");
    ASSERT_TRUE(test.core->get("b", &value).ok());
    EXPECT_FALSE(value.found());
}

TEST(TreeCorePrimaryReuse, NativeExportStillReturnsActualFramesAfterCleanCheckpoint)
{
    TestPrimaryReuse test;
    ASSERT_TRUE(test.core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedNativeCursor> cursor;
    const auto                             status = PublishedNativeCursor::create(test.core, {}, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::optional<NativeFrameView> frame;
    bool                           done = false;
    auto                           next = cursor->next(&frame, &done);
    ASSERT_TRUE(next.ok()) << next.to_string();
    if (!frame) {
        FAIL() << "native cursor omitted its frame";
    }
    EXPECT_EQ(frame->bytes.size(), test.config.frame_bytes);
    EXPECT_TRUE(frame_validate(frame->bytes.data(), frame->bytes.size()));
    EXPECT_TRUE(frame->owner);
}

TEST(TreeCorePrimaryReuse, InnerAndLeavesKeepTheirDurableFramesAcrossCheckpointAndRecovery)
{
    TestPrimaryReuse test;
    Batch            batch;
    for (size_t i = 0; i < 160; ++i) {
        batch.ops.push_back({.key   = std::string(4 - std::to_string(i).size(), '0') + std::to_string(i),
                             .kind  = OpKind::kPut,
                             .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(test.core->apply(1, batch).ok());
    const auto first = test.core->checkpoint();
    ASSERT_TRUE(first.ok()) << first.to_string();
    ASSERT_GT(test.page()->level, 0U);
    ASSERT_TRUE(test.page()->primary);
    const auto writes  = test.store->primary_writes.load();
    const auto backing = test.page()->inner_backing;
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_EQ(test.store->primary_writes.load(), writes);
    EXPECT_EQ(test.page()->inner_backing, backing);
    test.core.reset();
    const auto open = TreeCore::open(test.config, &test.core);
    ASSERT_TRUE(open.ok()) << open.to_string();
    ASSERT_TRUE(test.page()->primary);
    EXPECT_FALSE(test.page()->inner);
    const auto next = test.core->checkpoint();
    ASSERT_TRUE(next.ok()) << next.to_string();
    EXPECT_EQ(test.store->primary_writes.load(), writes);
    GetView value;
    ASSERT_TRUE(test.core->get("0159", &value).ok());
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), std::string(32, 'v'));
}
} // namespace
} // namespace crowdb::tree
