// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_backend_state.h"
#include "btree/tree_apply_operation.h"
#include "btree/tree_core.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestHostInput
{
    std::string key;
    std::string value;
};

struct TestHostCompletion
{
    std::promise<Status> result;
    std::future<Status>  future = result.get_future();

    static void complete(void *context, detail::CallbackSignal signal, Status status) noexcept
    {
        auto &self = *static_cast<TestHostCompletion *>(context);
        self.result.set_value(signal == detail::CallbackSignal::kStopped ? Status::unavailable() : std::move(status));
    }
};

struct TestHostBackendGate
{
    ApplyDispatcher::Node node{&TestHostBackendGate::run, this};
    std::promise<void>    entered;
    std::atomic<bool>     released{false};

    static void run(void *context) noexcept
    {
        auto &gate = *static_cast<TestHostBackendGate *>(context);
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

struct TestHostRelease
{
    std::shared_ptr<TestHostBackendGate> gate;

    ~TestHostRelease()
    {
        gate->release();
    }
};

Status prepare(TreeCore &core, uint64_t slot, std::string key, std::string value,
               std::shared_ptr<TreeApplyOperation> *out)
{
    auto input = std::make_shared<TestHostInput>(std::move(key), std::move(value));
    return core.prepare_replay(
        slot, 1,
        [raw = input.get()](size_t) { return CoreWriteInput{.key = Slice(raw->key), .value = Slice(raw->value)}; },
        input, 0, out);
}

Status apply(TreeCore &core, uint64_t slot, std::string key, std::string value)
{
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = prepare(core, slot, std::move(key), std::move(value), &operation);
    if (!status.ok()) {
        return status;
    }
    TestHostCompletion completed;
    operation->start(slot, &TestHostCompletion::complete, &completed);
    operation.reset();
    return completed.future.get();
}

TEST(TreeHostProgress, OneCpuPreservesOtherTreeProgressDuringSlowChosenWritebackAndGapRepair)
{
    tree_test::TempDir directory("shared_host_pressure_");
    TreeRuntimeOptions options;
    options.workers                 = 1;
    options.backend_workers         = 1;
    options.max_waiters_per_class   = 4;
    options.limits.gap_repair_bytes = 2 * 1024 * 1024;
    options.limits.io_operations    = 2;
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create(options, &runtime).ok());
    std::array<std::unique_ptr<BlockPageStore>, 2> stores;
    std::array<std::shared_ptr<TreeCore>, 2>       cores;
    for (size_t index = 0; index < cores.size(); ++index) {
        std::filesystem::create_directories(directory.path + "/" + std::to_string(index));
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path + "/" + std::to_string(index), 1, index + 1, 64 * 1024,
                                                4096, &stores[index])
                        .ok());
        Config config;
        config.runtime                                 = runtime;
        config.page_store                              = stores[index].get();
        config.frame_bytes                             = 4096;
        config.max_inline_value                        = 64;
        config.leaf_split_bytes                        = 3000;
        config.runtime_binding.cpu_queue_limit         = 4;
        config.runtime_binding.limits.io_operations    = 1;
        config.runtime_binding.limits.gap_repair_bytes = 1024 * 1024;
        auto status                                    = TreeCore::open(config, &cores[index]);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    auto             &slow = *cores[0];
    auto             &fast = *cores[1];
    const std::string value(256 * 1024, 'v');
    ASSERT_TRUE(apply(slow, 1, "a", value).ok());
    ASSERT_TRUE(apply(slow, 2, "b", value).ok());

    auto            gate = std::make_shared<TestHostBackendGate>();
    TestHostRelease release{gate};
    auto            entered = gate->entered.get_future();
    slow.binding()->backend_progress()->request(gate->node, gate);
    ASSERT_EQ(entered.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    std::array<std::shared_ptr<TreeApplyOperation>, 4> operations;
    std::array<TestHostCompletion, 4>                  completed;
    // A durable later chosen value waits for its preceding gap without taking
    // the gap payload reserve. The missing predecessor drives bounded writeback.
    ASSERT_TRUE(prepare(slow, 4, "d", value, operations.data()).ok());
    operations[0]->start(4, &TestHostCompletion::complete, completed.data());
    ASSERT_TRUE(prepare(slow, 3, "c", value, &operations[1]).ok());
    operations[1]->start(3, &TestHostCompletion::complete, &completed[1]);

    std::promise<void> cpu_terminal;
    auto               cpu_drained = cpu_terminal.get_future();
    std::jthread       cpu_wait([&] {
        slow.binding()->lane()->drain();
        cpu_terminal.set_value();
    });
    const auto         cpu_status = cpu_drained.wait_for(std::chrono::seconds(2));
    // The other Tree shares the one CPU and host budget, but its ready prefix
    // and independent gap must still complete while slow storage is stopped.
    ASSERT_TRUE(prepare(fast, 2, "second", "two", &operations[2]).ok());
    operations[2]->start(2, &TestHostCompletion::complete, &completed[2]);
    ASSERT_TRUE(prepare(fast, 1, "first", "one", &operations[3]).ok());
    operations[3]->start(1, &TestHostCompletion::complete, &completed[3]);
    const auto fast_status = completed[2].future.wait_for(std::chrono::seconds(2));
    const auto pending     = completed[1].future.wait_for(std::chrono::milliseconds(0));
    const auto local       = slow.binding()->account()->snapshot();
    gate->release();
    cpu_wait.join();
    EXPECT_EQ(cpu_status, std::future_status::ready);
    EXPECT_EQ(fast_status, std::future_status::ready);
    EXPECT_EQ(pending, std::future_status::timeout);
    const auto gap = static_cast<size_t>(detail::BudgetClass::kGapRepair);
    const auto io  = static_cast<size_t>(detail::BudgetClass::kIo);
    EXPECT_LE(local.allocated[gap] + local.reserved[gap], 1024U * 1024U);
    EXPECT_LE(local.allocated[io] + local.reserved[io], 1U);
    for (auto &completion : completed) {
        ASSERT_EQ(completion.future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        const auto status = completion.future.get();
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
    operations = {};
    EXPECT_EQ(slow.contiguous_slot(), 4U);
    EXPECT_EQ(fast.contiguous_slot(), 2U);
    EXPECT_EQ(slow.durable_coverage(), 0U);
    EXPECT_EQ(fast.durable_coverage(), 0U);
    GetView result;
    ASSERT_TRUE(fast.get("second", &result).ok());
    EXPECT_EQ(result.value().to_string(), "two");
    EXPECT_GT(slow.publisher().work_metrics()->totals()[static_cast<size_t>(TreeStage::PageWritebackPipeline)].count,
              0U);
}
} // namespace
} // namespace crowdb::tree
