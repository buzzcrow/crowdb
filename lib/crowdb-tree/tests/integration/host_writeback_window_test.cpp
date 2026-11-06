// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "local_checkpoint_test_support.h"
#include "native_import_test_support.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestHostWriteback
{
    Config                        config;
    std::shared_ptr<MemPageStore> store = std::make_shared<MemPageStore>(4096);
    std::shared_ptr<TreeCore>     core;

    Status open()
    {
        if (!config.runtime) {
            auto status = TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime);
            if (!status.ok()) {
                return status;
            }
        }
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.page_store                           = store.get();
        config.backend_owner                        = store;
        config.runtime_binding.limits.io_operations = 1;
        return TreeCore::open(config, &core);
    }

    void populate()
    {
        const std::string value(512, 'v');
        for (uint64_t slot = 1; slot <= 256; ++slot) {
            const auto status = core->apply(
                slot,
                Batch{.ops = {{.key = "key" + std::to_string(1000 + slot), .kind = OpKind::kPut, .value = value}}});
            ASSERT_TRUE(status.ok()) << status.to_string();
        }
        const auto status = core->flush();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
};

TEST(HostWritebackWindow, OnePrimaryPerPassConvergesAfterTheLastLeafAndRemainsCheckpointable)
{
    TestHostWriteback test;
    auto              status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.populate();
    auto initial = test.core->capture();
    ASSERT_GT(initial->stats.writeback_candidate_pages, 1U);
    ASSERT_GT(initial->stats.inner_pages, 0U);
    const auto pending        = initial->stats.checkpoint_pending_pages;
    size_t     passes         = 0;
    bool       saw_inner_only = false;
    while (test.core->capture()->stats.writeback_candidate_pages != 0 && passes < 64) {
        const auto before = test.core->capture();
        if (!before->mapping.oldest_writeback_leaf()) {
            saw_inner_only = true;
        }
        std::shared_ptr<CheckpointOperation> operation;
        status = test.core->prepare_writeback(&operation);
        ASSERT_TRUE(status.ok()) << status.to_string();
        status = detail::wait_snapshot_operation(operation);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(operation->result().writeback_completed);
        EXPECT_FALSE(operation->result().durable_committed);
        EXPECT_EQ(operation->result().primary_frames, 1U);
        EXPECT_EQ(operation->result().rebased_pages, 1U);
        EXPECT_EQ(operation->result().durable_coverage, 0U);
        EXPECT_EQ(operation->result().captured_coverage, 256U);
        const auto after = test.core->capture();
        EXPECT_LT(after->stats.writeback_candidate_pages, before->stats.writeback_candidate_pages);
        EXPECT_EQ(after->stats.checkpoint_pending_pages, pending);
        ++passes;
    }
    EXPECT_GT(passes, 1U);
    EXPECT_TRUE(saw_inner_only);
    EXPECT_EQ(test.core->capture()->stats.writeback_candidate_pages, 0U);
    EXPECT_EQ(test.core->durable_coverage(), 0U);
    ASSERT_TRUE(test.core->checkpoint().ok());
    initial.reset();
    test.core.reset();
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView found;
    ASSERT_TRUE(test.core->get("key1256", &found).ok());
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), std::string(512, 'v'));
    EXPECT_EQ(test.core->durable_coverage(), 256U);
}

TEST(HostWritebackWindow, NoCandidateCompletesWithoutCapturingOrAdvancingDurableState)
{
    TestHostWriteback test;
    auto              status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto before = test.core->capture();
    for (size_t pass = 0; pass < 3; ++pass) {
        std::shared_ptr<CheckpointOperation> operation;
        ASSERT_TRUE(test.core->prepare_writeback(&operation).ok());
        status = detail::wait_snapshot_operation(operation);
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto &result = operation->result();
        EXPECT_TRUE(result.writeback_completed);
        EXPECT_FALSE(result.durable_committed);
        EXPECT_EQ(result.primary_frames, 0U);
        EXPECT_EQ(result.written_frames, 0U);
        EXPECT_EQ(result.durable_coverage, 0U);
        EXPECT_EQ(result.snapshot_sequence, 0U);
        EXPECT_EQ(test.core->capture()->checkpoint_token, before->checkpoint_token);
        EXPECT_FALSE(test.core->capture()->checkpoint_inflight);
    }
}

TEST(HostWritebackWindow, ImportedRawChildrenKeepTheirCapturedFenceProofWithoutBackendReads)
{
    TestHostWriteback test;
    ASSERT_TRUE(test.open().ok());
    std::vector<NativeFrame> frames;
    frames.push_back({.page_id = 0, .frame = std::vector<uint8_t>(4096)});
    ASSERT_TRUE(inner_frame_build(frames[0].frame.data(), 4096, 0, {1, 2}, {"m"}));
    frame_set_inner_fence_pages(frames[0].frame.data(), 4096, 1, 2);
    const auto left = encode_cell_buf(7, OpKind::kPut, "old-left");
    frames.push_back(test_import::leaf(1, "a", Slice(left), 2));
    frames.push_back(test_import::inline_leaf(2, "z", "old-right"));
    auto info   = test_import::info(frames);
    auto status = test.core->install_native(std::move(frames), info);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (uint64_t slot = 8; slot <= 263; ++slot) {
        status = test.core->apply(slot, Batch{.ops = {{.key   = "key" + std::to_string(1000 + slot),
                                                       .kind  = OpKind::kPut,
                                                       .value = std::string(512, 'v')}}});
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    status = test.core->flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (size_t pass = 0; test.core->capture()->stats.writeback_candidate_pages != 0 && pass < 64; ++pass) {
        std::shared_ptr<CheckpointOperation> operation;
        ASSERT_TRUE(test.core->prepare_writeback(&operation).ok());
        status = detail::wait_snapshot_operation(operation);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(operation->result().primary_frames, 1U);
        EXPECT_EQ(operation->result().durable_coverage, 0U);
    }
    EXPECT_EQ(test.core->capture()->stats.writeback_candidate_pages, 0U);
    status = test.core->checkpoint();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.core.reset();
    ASSERT_TRUE(test.open().ok());
    GetView found;
    ASSERT_TRUE(test.core->get("z", &found).ok());
    EXPECT_EQ(found.value().to_string(), "old-right");
    EXPECT_EQ(test.core->durable_coverage(), 263U);
}

TEST(HostWritebackWindow, FailedAndCancelledWindowsLeaveTheCandidateForALaterPass)
{
    test_write::TestCheckpointStore store;
    TestHostWriteback               test;
    store.device                 = test.store;
    test.config.async_page_store = &store;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(test.core->flush().ok());
    const auto before = test.core->capture();
    ASSERT_EQ(before->stats.writeback_candidate_pages, 1U);
    store.fail_at = 1;
    std::shared_ptr<CheckpointOperation> operation;
    ASSERT_TRUE(test.core->prepare_writeback(&operation).ok());
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kIoError);
    EXPECT_FALSE(operation->result().writeback_completed);
    EXPECT_EQ(test.core->capture()->mapping.get(before->root_page_id), before->mapping.get(before->root_page_id));
    operation.reset();
    store.fail_at  = 0;
    store.defer_at = store.events.size() + 1;
    ASSERT_TRUE(test.core->prepare_writeback(&operation).ok());
    test_write::Completion done;
    auto                   connected = stdexec::connect(operation->sender(), test_write::Receiver{.done = &done});
    stdexec::start(connected);
    test.core->binding()->cpu()->drain();
    // Backend submission runs on a separate worker from the CPU continuation.
    store.wait_deferred();
    ASSERT_TRUE(store.pending);
    operation->request_stop();
    test.core->binding()->cpu()->drain();
    EXPECT_EQ(done.stopped.load(), 0U);
    store.finish();
    test.core->binding()->cpu()->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_EQ(test.core->capture()->stats.writeback_candidate_pages, 1U);
    EXPECT_EQ(test.core->durable_coverage(), 0U);
    operation.reset();
    store.defer_at = 0;
    ASSERT_TRUE(test.core->prepare_writeback(&operation).ok());
    ASSERT_TRUE(detail::wait_snapshot_operation(operation).ok());
    EXPECT_EQ(operation->result().primary_frames, 1U);
    EXPECT_EQ(test.core->capture()->stats.writeback_candidate_pages, 0U);
    EXPECT_EQ(test.core->durable_coverage(), 0U);
}
} // namespace
} // namespace crowdb::tree
