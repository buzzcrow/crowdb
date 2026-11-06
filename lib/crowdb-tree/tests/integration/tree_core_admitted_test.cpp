// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"

#include <gtest/gtest.h>

#include <array>
#include <thread>
#include <vector>

namespace crowdb::tree
{
namespace
{
struct TestApplyCompletion
{
    std::atomic<bool> done{false};
    Status            status;

    static void complete(void *context, detail::CallbackSignal signal, Status result) noexcept
    {
        auto &self  = *static_cast<TestApplyCompletion *>(context);
        self.status = signal == detail::CallbackSignal::kStopped ? Status::unavailable() : std::move(result);
        self.done.store(true, std::memory_order_release);
        self.done.notify_all();
    }

    void wait() const
    {
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
    }
};

class TreeCoreAdmitted : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = 2;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes                        = 4096;
        config.leaf_split_bytes                   = 3000;
        config.buffer_pool_bytes                  = 4 * 1024 * 1024;
        config.execution.source_max_gap_entries   = 64;
        config.execution.source_max_gap_key_bytes = 128 * 1024;
        tree                                      = std::make_unique<Crowdbtree>(config);
        core                                      = TreeCoreAccess::get(*tree);
    }

    Status prepare(Slice key, Slice value, std::shared_ptr<TreeApplyOperation> *out)
    {
        return core->prepare_apply(
            1, [=](size_t) { return CoreWriteInput{.key = key, .value = value}; }, detail::BudgetClass::kAdmittedWriter,
            0, out);
    }

    Status fill_full_source()
    {
        const auto batch_for = [](uint64_t slot) {
            Batch batch;
            for (size_t index = 0; index < 64; ++index) {
                batch.ops.push_back(
                    {.key = "k" + std::to_string((slot * 64) + index), .kind = OpKind::kPut, .value = "v"});
            }
            return batch;
        };
        for (uint64_t slot = 1; slot < 128; ++slot) {
            const auto status = tree->apply(slot, batch_for(slot));
            if (!status.ok()) {
                return Status::internal_error("fill slot " + std::to_string(slot) + ": " + status.to_string());
            }
            // Preparation targets a full source and retained Handoff credit,
            // not temporary history candidates awaiting asynchronous disposal.
            // The final CPU callback below must not join its own retirement work.
            core->binding()->account()->drain_retirements();
        }
        // The final batch fills the second source from a CPU callback. Reaching the hard source
        // cap starts asynchronous soft maintenance, while this callback cannot
        // wait for the Handoff credit intentionally retained by the test.
        TestApplyCompletion completion;
        tree->get_async("k64", [&](Status read, GetView) {
            auto status = read.ok() ? tree->apply(128, batch_for(128)) : read;
            TestApplyCompletion::complete(&completion, detail::CallbackSignal::kValue, std::move(status));
        });
        completion.wait();
        core->binding()->lane()->drain();
        return completion.status;
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeCoreAdmitted, PreparationOwnsCallerBytesAndDoesNotBlockReplacementOrClose)
{
    std::shared_ptr<TreeApplyOperation> stale;
    ASSERT_TRUE(prepare("before", "value", &stale).ok());
    ASSERT_TRUE(core->clear().ok());
    EXPECT_EQ(stale->arm().code(), Code::kUnavailable);
    stale.reset();
    std::shared_ptr<TreeApplyOperation> unsubmitted;
    ASSERT_TRUE(prepare("after", "value", &unsubmitted).ok());
    tree.reset();
    EXPECT_EQ(unsubmitted->arm().code(), Code::kUnavailable);
}

TEST_F(TreeCoreAdmitted, FullBackendWindowReturnsTemporaryPressureAndRollsBackTheApplyTicket)
{
    auto                                                       &lifetime = core->operation_lifetime();
    const auto                                                  maximum  = lifetime.resource_usage().limit;
    std::vector<std::shared_ptr<TreeOperationLifetime::Ticket>> occupied;
    occupied.reserve(maximum);
    for (uint64_t index = 0; index < maximum; ++index) {
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        ASSERT_TRUE(lifetime.acquire(core->binding()->lifecycle_budget(), &ticket).ok());
        occupied.push_back(std::move(ticket));
    }
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare("admitted", "value", &operation).ok());
    EXPECT_EQ(operation->arm().code(), Code::kUnavailable);
    auto usage = core->admission_resource_usage();
    EXPECT_EQ(usage[0].used, maximum);
    EXPECT_EQ(usage[0].denied_tree, 1U);
    EXPECT_EQ(usage[1].used, 0U);
    occupied.pop_back();
    ASSERT_TRUE(operation->arm().ok());
    usage = core->admission_resource_usage();
    EXPECT_EQ(usage[0].used, maximum);
    EXPECT_EQ(usage[1].used, 1U);
    TestApplyCompletion completed;
    operation->start(1, &TestApplyCompletion::complete, &completed);
    completed.wait();
    core->binding()->lane()->drain();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    usage = core->admission_resource_usage();
    EXPECT_EQ(usage[0].used, maximum - 1);
    EXPECT_EQ(usage[1].used, 0U);
    EXPECT_EQ(usage[3].used, 1U);
    EXPECT_EQ(usage[5].used, 0U);
}

TEST_F(TreeCoreAdmitted, ArmedApplyFinishesAfterCloseFencesIngressAndCallerDropsTheHandle)
{
    std::shared_ptr<TreeApplyOperation> operation;
    std::string                         key   = "retained";
    std::string                         value = "owned before consensus";
    ASSERT_TRUE(prepare(key, value, &operation).ok());
    key.assign("changed");
    value.clear();
    ASSERT_TRUE(operation->arm().ok());
    std::atomic<bool> closed{false};
    std::jthread      closer([&] {
        core->close_and_drain();
        closed.store(true, std::memory_order_release);
    });
    while (!core->closed()) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(closed.load(std::memory_order_acquire));
    TestApplyCompletion completed;
    operation->start(1, &TestApplyCompletion::complete, &completed);
    operation.reset();
    completed.wait();
    closer.join();
    EXPECT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 1U);
}

TEST_F(TreeCoreAdmitted, DistantNoopAwaitsAnActualFrontierTransitionWithoutSkippingTheGap)
{
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter, 0, &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    TestApplyCompletion completed;
    operation->start(ApplyFrontier::kMaxWindow + 1, &TestApplyCompletion::complete, &completed);
    core->binding()->lane()->drain();
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 0U);
    core->force_advance_slot(1);
    completed.wait();
    EXPECT_TRUE(completed.status.ok());
    EXPECT_EQ(core->contiguous_slot(), 1U);
    EXPECT_EQ(core->max_seen_slot(), ApplyFrontier::kMaxWindow + 1);
}

TEST_F(TreeCoreAdmitted, FullSourceAwaitsCompleteFlushCreditThenConsumesThePrepaidWrite)
{
    tree.reset();
    config.max_memtable_count     = 2;
    config.memtable_flush_entries = UINT32_MAX;
    config.memtable_flush_bytes   = SIZE_MAX;
    tree                          = std::make_unique<Crowdbtree>(config);
    core                          = TreeCoreAccess::get(*tree);
    // Fill both configured source windows with Handoff credit held. The
    // final callback write starts soft backpressure without blocking itself.
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kHandoff,
                                                        config.runtime_binding.limits.handoff_bytes);
    ASSERT_TRUE(pressure);
    std::shared_ptr<TreeApplyOperation> operation;
    std::string                         value = "owned";
    ASSERT_TRUE(prepare("last", value, &operation).ok());
    value.clear();
    ASSERT_TRUE(operation->arm().ok());
    const auto filled = fill_full_source();
    ASSERT_TRUE(filled.ok()) << filled.to_string();
    ASSERT_EQ(core->capture()->sources->tables.back()->count(), 4096U);
    TestApplyCompletion completed;
    operation->start(129, &TestApplyCompletion::complete, &completed);
    operation.reset();
    core->binding()->lane()->drain();
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire))
        << completed.status.to_string() << " tables=" << core->capture()->sources->tables.size()
        << " active=" << core->capture()->sources->tables.back()->count();
    const auto waiting = core->publisher().work_metrics()->totals();
    EXPECT_EQ(waiting[static_cast<size_t>(TreeStage::FlushAdmissionWait)].inflight, 1U);
    EXPECT_EQ(waiting[static_cast<size_t>(TreeStage::BudgetWait)].inflight, 1U);
    pressure.reset();
    completed.wait();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_EQ(tree->get_view("last").value().to_string(), "owned");
    EXPECT_EQ(core->contiguous_slot(), 129U);
    const auto terminal = core->publisher().work_metrics()->totals();
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::FlushAdmissionWait)].count, 1U);
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::BudgetWait)].count,
              detail::FlushReserveBounds::categories.size());
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::BudgetWait)].inflight, 0U);
}

TEST_F(TreeCoreAdmitted, CompleteBatchMustFitTheGapSourceBeforeItCanBeProposed)
{
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_apply(
        65,
        [](size_t index) {
            static const std::array<std::string, 65> keys = [] {
                std::array<std::string, 65> result;
                for (size_t item = 0; item < result.size(); ++item) {
                    result[item] = std::to_string(item);
                }
                return result;
            }();
            return CoreWriteInput{.key = Slice(keys[index]), .value = Slice("value")};
        },
        detail::BudgetClass::kAdmittedWriter, 0, &operation);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeCoreAdmitted, FullOrdinaryCpuQueueCannotRejectAcceptedHandoff)
{
    tree.reset();
    config.max_memtable_count     = 2;
    config.memtable_flush_entries = UINT32_MAX;
    config.memtable_flush_bytes   = SIZE_MAX;
    tree                          = std::make_unique<Crowdbtree>(config);
    core                          = TreeCoreAccess::get(*tree);
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kHandoff,
                                                        config.runtime_binding.limits.handoff_bytes);
    ASSERT_TRUE(pressure);
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare("accepted", "paid", &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    const auto filled = fill_full_source();
    ASSERT_TRUE(filled.ok()) << filled.to_string();
    ASSERT_EQ(core->capture()->sources->tables.back()->count(), 4096U);
    auto runtime = core->binding()->cpu();
    auto other   = std::make_shared<detail::FlushRuntime::Lane>(1024);
    std::vector<std::shared_ptr<detail::FlushRuntime::Progress>> occupied;
    for (;;) {
        std::shared_ptr<detail::FlushRuntime::Progress> slot;
        auto status = runtime->reserve_progress(other, [](void *) noexcept {}, nullptr, {}, &slot);
        if (!status.ok()) {
            ASSERT_EQ(status.code(), Code::kResourceExhausted);
            break;
        }
        occupied.push_back(std::move(slot));
    }
    ASSERT_FALSE(occupied.empty());
    EXPECT_EQ(
        runtime->try_submit(core->binding()->lane(), [](void *) noexcept {}, nullptr).code(), Code::kResourceExhausted);
    pressure.reset();
    TestApplyCompletion completed;
    operation->start(129, &TestApplyCompletion::complete, &completed);
    completed.wait();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_EQ(core->contiguous_slot(), 129U);
    occupied.clear();
    EXPECT_EQ(tree->get_view("accepted").value().to_string(), "paid");
}

} // namespace
} // namespace crowdb::tree
