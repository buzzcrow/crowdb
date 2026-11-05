// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
namespace
{
struct TestPublicCheckpoint
{
    std::shared_ptr<TreeRuntime>    runtime;
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;
    Config                          config;
    std::unique_ptr<Crowdbtree>     tree;
    Status                          last_open;

    TestPublicCheckpoint()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &runtime).ok());
        store.device            = device;
        config.runtime          = runtime;
        config.page_store       = device.get();
        config.async_page_store = &store;
        config.frame_bytes      = 4096;
        config.max_inline_value = 64;
    }

    Status open()
    {
        last_open = Crowdbtree::open(config, &tree);
        return last_open;
    }

    Status get(Slice key, GetView *out) const
    {
        std::atomic<bool> done{false};
        Status            status;
        tree->get_async(key, [&](Status result, GetView view) {
            status = std::move(result);
            *out   = std::move(view);
            done.store(true, std::memory_order_release);
            done.notify_one();
        });
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
        return status;
    }

    void read(const std::string &expected, uint64_t expected_slot) const
    {
        GetView    view;
        const auto status = get("a", &view);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(view.found());
        EXPECT_EQ(view.value().to_string(), expected);
        EXPECT_EQ(view.slot(), expected_slot);
    }
};

TEST(TreeCoreCheckpoint, PublicFreshCheckpointColdReadAndReopen)
{
    TestPublicCheckpoint test;
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    const std::string large(20000, 'v');
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = large}}}).ok());
    uint64_t coverage = 0;
    uint64_t sequence = 0;
    auto     status   = test.tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 1U);
    EXPECT_EQ(sequence, 1U);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
    test.read(large, 1);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    test.read(large, 1);
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "new"}}}).ok());
    status = test.tree->snapshot(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 2U);
    EXPECT_EQ(sequence, 2U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    test.read("new", 2);
}

TEST(TreeCoreCheckpoint, ActualWritebackCheckpointReuseAndColdReadKeepSeparateCauses)
{
    TestPublicCheckpoint test;
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    const std::string value(20000, 'v');
    ASSERT_TRUE(test.tree->put("a", value).ok());
    auto                                *core = TreeCoreAccess::get(*test.tree);
    auto                                 work = core->publisher().work_metrics();
    std::shared_ptr<CheckpointOperation> writeback;
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    auto status = detail::wait_snapshot_operation(writeback);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_GT(writeback->result().written_frames, 0U);
    const auto written  = writeback->result().written_frames;
    auto       pressure = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::CachePressure)];
    EXPECT_EQ(pressure[static_cast<size_t>(TreeWorkCounter::WriteOperations)], written);
    EXPECT_EQ(pressure[static_cast<size_t>(TreeWorkCounter::EncodedFrameBytes)], written * test.config.frame_bytes);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 0U);
    writeback.reset();
    test.read(value, 1);
    EXPECT_GT(work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Read)]
                                       [static_cast<size_t>(TreeWorkCounter::ReadBytes)],
              0U);
    status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto checkpoint = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Checkpoint)];
    EXPECT_EQ(checkpoint[static_cast<size_t>(TreeWorkCounter::ReusedFrames)], written);
    EXPECT_GT(checkpoint[static_cast<size_t>(TreeWorkCounter::ReusedBackingBytes)], 0U);
    EXPECT_GT(checkpoint[static_cast<size_t>(TreeWorkCounter::WriteBytes)], 0U);
    EXPECT_EQ(checkpoint[static_cast<size_t>(TreeWorkCounter::EncodedFrameBytes)], 0U);
    EXPECT_EQ(work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::CachePressure)]
                                       [static_cast<size_t>(TreeWorkCounter::WriteOperations)],
              written);
}

TEST(TreeCoreCheckpoint, BackendDomainRejectsConcurrentOpenAndReleasesAtLastOwner)
{
    TestPublicCheckpoint test;
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    std::unique_ptr<Crowdbtree> second;
    auto                        status = Crowdbtree::open(test.config, &second);
    EXPECT_EQ(status.code(), Code::kUnavailable);
    EXPECT_FALSE(second);
    test.tree.reset();
    status = Crowdbtree::open(test.config, &second);
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(TreeCoreCheckpoint, FailedOpenReturnsBackendDomain)
{
    TestPublicCheckpoint test;
    test.config.runtime_binding.limits.routing_bytes = test.config.runtime_binding.lifecycle_bytes;
    EXPECT_FALSE(test.open().ok());
    EXPECT_FALSE(test.tree);
    test.config.runtime_binding = {};
    auto status                 = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(TreeCoreCheckpoint, SingleIoCreditDoesNotOverlapHandoffAndCheckpoint)
{
    TestPublicCheckpoint test;
    test.config.runtime_binding.limits.io_operations = 1;
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "one"}}}).ok());
    auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    // The second handoff retains the first checkpoint's cold base. Its
    // preflight must release the read window before reserving write I/O.
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "b", .kind = OpKind::kPut, .value = "two"}}}).ok());
    status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.read("one", 1);
    GetView view;
    status = test.get("b", &view);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(view.found());
    EXPECT_EQ(view.slot(), 2U);
    EXPECT_EQ(view.value().to_string(), "two");
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 2U);
}

TEST(TreeCoreCheckpoint, RecoveredOverflowCanBeCheckpointedWithoutGlobalResolver)
{
    TestPublicCheckpoint test;
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    const std::string large(12000, 'r');
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = large}}}).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    test.tree.reset();
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "b", .kind = OpKind::kPut, .value = "other"}}}).ok());
    auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    test.tree.reset();
    ASSERT_TRUE(test.open().ok()) << test.last_open.to_string();
    test.read(large, 1);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 2U);
}
} // namespace
} // namespace crowdb::tree
