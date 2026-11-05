// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace crowdb::tree
{
namespace
{
class SourceRotationWait : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers         = 1;
        host.backend_workers = 1;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.memtable_flush_entries = 1;
        config.memtable_flush_bytes   = SIZE_MAX;
        config.max_memtable_count     = 2;
        tree                          = std::make_unique<Crowdbtree>(config);
        core                          = TreeCoreAccess::get(*tree);
        ASSERT_TRUE(tree->put("first", "value").ok());
        const auto     account   = core->binding()->account();
        const auto     snapshot  = account->snapshot();
        constexpr auto index     = static_cast<size_t>(detail::BudgetClass::kHandoff);
        const auto     available = detail::ResourceBudget::capacity(account, detail::BudgetClass::kHandoff) -
                                   snapshot.allocated[index] - snapshot.reserved[index];
        blocker = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, available);
        ASSERT_TRUE(blocker);
        const auto initial_waits = wait_count();
        tree->get_async("first", [&](Status read, GetView value) {
            EXPECT_TRUE(read.ok()) << read.to_string();
            EXPECT_TRUE(value.found());
            written = tree->put("second", "next");
            done.store(true, std::memory_order_release);
            done.notify_all();
        });
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
        ASSERT_TRUE(written.ok()) << written.to_string();
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (wait_count() == initial_waits && std::chrono::steady_clock::now() < until) {
            std::this_thread::yield();
        }
        ASSERT_GT(wait_count(), initial_waits);
    }

    [[nodiscard]] uint64_t wait_count() const
    {
        return core->publisher().work_metrics()->counters.snapshot()[static_cast<size_t>(
            TreeWorkCause::Allocation)][static_cast<size_t>(TreeWorkCounter::AdmissionWaits)];
    }

    Config                                        config;
    std::unique_ptr<Crowdbtree>                   tree;
    TreeCore                                     *core = nullptr;
    std::optional<detail::ResourceBudget::Credit> blocker;
    std::atomic<bool>                             done{false};
    Status                                        written;
};

TEST_F(SourceRotationWait, BudgetReleaseResumesFiniteHandoffWithoutAnotherWrite)
{
    blocker.reset();
    // Scheduler-tail drain requires operation tickets to have finished first.
    // Observe automatic publication without submitting another write or flush.
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (core->capture()->coverage != 2 && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    ASSERT_EQ(core->capture()->coverage, 2U);
    core->binding()->drain();
    EXPECT_EQ(tree->get_view("second").value().to_string(), "next");
    ASSERT_TRUE(tree->put("third", "continued").ok());
}

TEST_F(SourceRotationWait, CloseCancelsOnlyTheUnacceptedSoftBudgetWait)
{
    core->close_and_drain();
    EXPECT_TRUE(core->closed());
    EXPECT_EQ(tree->contiguous_slot(), 2U);
    blocker.reset();
}

TEST_F(SourceRotationWait, CloseReleasesAHostWriterWaitingForTheSoftPause)
{
    core->binding()->drain();
    const auto        baseline = core->operation_lifetime().outstanding();
    std::atomic<bool> written_done{false};
    Status            status;
    std::thread       writer([&] {
        status = tree->put("third", "accepted ingress");
        written_done.store(true, std::memory_order_release);
    });
    const auto        until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (core->operation_lifetime().outstanding() <= baseline && !written_done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_GT(core->operation_lifetime().outstanding(), baseline);
    EXPECT_FALSE(written_done.load(std::memory_order_acquire));
    core->close_and_drain();
    writer.join();
    EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
    EXPECT_EQ(tree->contiguous_slot(), 2U);
    blocker.reset();
}

TEST(SourceRotationProgress, RetainedReclaimCannotStrandAnAlreadyAppliedHostWrite)
{
    Config config;
    config.memtable_flush_entries = 1;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 2;
    Crowdbtree  tree(config);
    auto *const core = TreeCoreAccess::get(tree);
    ASSERT_TRUE(tree.put("first", "value").ok());
    core->binding()->drain();
    const auto account   = core->binding()->account();
    const auto available = detail::ResourceBudget::available(account, detail::BudgetClass::kReclaim);
    auto       reserved  = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, available);
    if (!reserved) {
        FAIL() << "Reclaim credit unavailable";
    }
    auto retained = reserved->allocate(available);
    ASSERT_TRUE(retained);
    std::atomic<bool> done{false};
    Status            status;
    std::thread       writer([&] {
        status = tree.put("second", "accepted");
        done.store(true, std::memory_order_release);
    });
    const auto        until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    const bool returned_with_retained_owner = done.load(std::memory_order_acquire);
    retained.reset();
    reserved.reset();
    writer.join();
    EXPECT_TRUE(returned_with_retained_owner);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.get_view("second").value().to_string(), "accepted");
}
} // namespace
} // namespace crowdb::tree
