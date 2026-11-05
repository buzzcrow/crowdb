// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/local_compaction_operation.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "snapshot/operation_wait.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestBackendGate
{
    ApplyDispatcher::Node node{&TestBackendGate::run, this};
    std::promise<void>    entered;
    std::atomic<bool>     released{false};

    static void run(void *context) noexcept
    {
        auto &gate = *static_cast<TestBackendGate *>(context);
        gate.entered.set_value();
        while (!gate.released.load(std::memory_order_acquire)) {
            gate.released.wait(false);
        }
    }

    void release()
    {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

struct TestGateRelease
{
    std::shared_ptr<TestBackendGate> gate;

    ~TestGateRelease()
    {
        gate->release();
    }
};

struct TestCompletion
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<Status> *result;

    void set_value() const noexcept
    {
        set_error(Status::Ok());
    }

    void set_stopped() const noexcept
    {
        set_error(Status::unavailable());
    }

    void set_error(Status status) const noexcept
    {
        result->set_value(std::move(status));
    }
};

TEST(LocalCompactionAsync, BlockedBackendLeavesSingleCpuFreeForResidentReads)
{
    tree_test::TempDir              directory("async_compact_cpu_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store  = store.get();
    config.frame_bytes = 4096;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "resident"}}}).ok());
    auto            gate = std::make_shared<TestBackendGate>();
    TestGateRelease release{gate};
    auto            entered = gate->entered.get_future();
    core->binding()->backend_progress()->request(gate->node, gate);
    ASSERT_EQ(entered.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    std::shared_ptr<LocalCompactionOperation> operation;
    ASSERT_TRUE(core->prepare_compaction({}, &operation).ok());
    std::promise<Status> terminal;
    auto                 completed  = terminal.get_future();
    auto                 connection = stdexec::connect(operation->sender(), TestCompletion{.result = &terminal});
    stdexec::start(connection);
    // Starting the operation synchronously queues its paid CPU continuation.
    // Drain that lane on another host thread: it must finish while the sole
    // backend worker is still stopped, before the foreground read is issued.
    std::promise<void>   cpu_terminal;
    auto                 cpu_drained = cpu_terminal.get_future();
    std::jthread         cpu_wait([&] {
        core->binding()->lane()->drain();
        cpu_terminal.set_value();
    });
    const auto           cpu_status = cpu_drained.wait_for(std::chrono::seconds(2));
    std::promise<Status> read;
    auto                 readable = read.get_future();
    core->get_async("key", [&](Status status, GetView value) {
        if (status.ok() && (!value.found() || value.value().to_string() != "resident")) {
            status = Status::corruption();
        }
        read.set_value(std::move(status));
    });
    const auto read_status = readable.wait_for(std::chrono::seconds(2));
    const auto pending     = completed.wait_for(std::chrono::milliseconds(0));
    gate->release();
    cpu_wait.join();
    EXPECT_EQ(cpu_status, std::future_status::ready);
    ASSERT_EQ(read_status, std::future_status::ready);
    EXPECT_TRUE(readable.get().ok());
    EXPECT_EQ(pending, std::future_status::timeout);
    ASSERT_EQ(completed.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_TRUE(completed.get().ok());
    operation.reset();
}

TEST(LocalCompactionAsync, StopBeforeStartCompletesOnceAndReleasesTheSelectionGate)
{
    tree_test::TempDir              directory("async_compact_cancel_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store                      = store.get();
    config.runtime_binding.cpu_queue_limit = 2;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    std::shared_ptr<LocalCompactionOperation> operation;
    ASSERT_TRUE(core->prepare_compaction({}, &operation).ok());
    std::atomic<size_t> observed{0};
    operation->observe_completion([&observed](const MergeGcStats &) { observed.fetch_add(1); });
    operation->request_stop();
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kUnavailable);
    EXPECT_FALSE(core->local_backend()->gc_active.load());
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kUnavailable);
    EXPECT_EQ(operation->result().blocks_selected, 0U);
    EXPECT_EQ(observed.load(), 1U);
    operation.reset();
    core->binding()->drain();
    ASSERT_TRUE(core->prepare_compaction({}, &operation).ok());
    EXPECT_TRUE(detail::wait_snapshot_operation(operation).ok());
    operation.reset();
}
} // namespace
} // namespace crowdb::tree
