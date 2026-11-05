// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestPendingAllocation
{
    std::promise<void> entered;
    std::atomic<bool>  release{false};

    ~TestPendingAllocation()
    {
        resume();
    }

    void resume() noexcept
    {
        release.store(true, std::memory_order_release);
        release.notify_all();
    }

    void wait()
    {
        if (release.load(std::memory_order_acquire)) {
            return;
        }
        entered.set_value();
        while (!release.load(std::memory_order_acquire)) {
            release.wait(false, std::memory_order_acquire);
        }
    }
};

struct TestResumeAllocation
{
    std::shared_ptr<TestPendingAllocation> gate;

    ~TestResumeAllocation()
    {
        gate->resume();
    }
};
} // namespace

TEST(TreeCoreChunkWriteback, ColdUncommittedPacksStayReadableButReopenUsesTheDurableWalPair)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "committed").ok());
    test.store->request_wal_offset(4096);
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto *core = TreeCoreAccess::get(*test.tree);
    ASSERT_TRUE(test.put(2, std::string(20000, 's')).ok());
    test.store->request_wal_offset(8192);
    std::shared_ptr<CheckpointOperation> writeback;
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    const auto staged = detail::wait_snapshot_operation(writeback);
    ASSERT_TRUE(staged.ok()) << staged.to_string();
    EXPECT_TRUE(writeback->result().writeback_completed);
    EXPECT_FALSE(writeback->result().durable_committed);
    EXPECT_EQ(writeback->result().captured_coverage, 2U);
    EXPECT_EQ(writeback->result().durable_coverage, 1U);
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
    EXPECT_EQ(test.store->manifest_generation(), 1U);
    EXPECT_EQ(test.backend.catalog->commit_calls, 1U);
    EXPECT_GT(core->capture()->stats.checkpoint_pending_pages, 0U);
    writeback.reset();
    (void)test.tree->evict_clean_leaves(0);
    const auto metrics = core->publisher().work_metrics();
    const auto loads   = metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count;
    GetView    actual;
    const auto cold = core->get("key", &actual);
    ASSERT_TRUE(cold.ok()) << cold.to_string();
    EXPECT_EQ(actual.value().to_string(), std::string(20000, 's'));
    EXPECT_GT(metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, loads);
    actual = {};
    test.tree.reset();
    const auto reopened = test.open();
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "committed");
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
}

TEST(TreeCoreChunkWriteback, PendingAllocationSerializesCheckpointsAndCancellationKeepsNewerApply)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "committed").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    ASSERT_TRUE(test.put(2, std::string(12000, 's')).ok());
    auto gate                                 = std::make_shared<TestPendingAllocation>();
    auto entered                              = gate->entered.get_future();
    test.backend.transport->before_allocate   = [gate] { gate->wait(); };
    auto                                *core = TreeCoreAccess::get(*test.tree);
    std::shared_ptr<CheckpointOperation> writeback;
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    detail::SnapshotWaitCompletion completion;
    auto connected = stdexec::connect(writeback->sender(), detail::SnapshotWaitReceiver{.completion = &completion});
    TestResumeAllocation resume{gate};
    stdexec::start(connected);
    const auto pending = entered.wait_for(std::chrono::seconds(2));
    if (pending != std::future_status::ready) {
        gate->resume();
    }
    ASSERT_EQ(pending, std::future_status::ready);
    EXPECT_TRUE(core->capture()->checkpoint_inflight);
    EXPECT_FALSE(completion.done.completed_for_tests());
    auto second        = std::make_shared<std::promise<Status>>();
    auto second_result = second->get_future();
    test.tree->snapshot_async([second](Status status, uint64_t) { second->set_value(std::move(status)); });
    EXPECT_EQ(second_result.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    ASSERT_TRUE(test.put(3, "newer").ok());
    writeback->request_stop();
    EXPECT_FALSE(completion.done.completed_for_tests());
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 1U);
    gate->resume();
    completion.done.wait();
    EXPECT_EQ(completion.status.code(), Code::kUnavailable);
    ASSERT_EQ(second_result.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_TRUE(second_result.get().ok());
    EXPECT_FALSE(core->capture()->checkpoint_inflight);
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "newer");
    EXPECT_EQ(test.backend.catalog->commit_calls, 2U);
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 3U);
    writeback.reset();
    test.backend.transport->before_allocate = {};
    const auto committed                    = test.tree->snapshot();
    ASSERT_TRUE(committed.ok()) << committed.to_string();
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 3U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "newer");
}
} // namespace crowdb::tree
