// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_maintenance_internal.h"

#include <gtest/gtest.h>

#include <thread>

namespace crowdb::tree
{
namespace
{
class MaintenanceReportCAbi : public testing::Test
{
  protected:
    ct_runtime *runtime = nullptr;
    ct_tree    *tree    = nullptr;

    void SetUp() override
    {
        ct_runtime_options options{};
        ct_runtime_options_default(&options);
        options.workers = 1;
        ASSERT_EQ(ct_runtime_create(&options, &runtime), 0);
        ct_options config{};
        config.backend       = CT_BACKEND_MEM_BLOCK;
        ct_page_store *store = nullptr;
        ASSERT_EQ(ct_page_store_open_mem(4096, &store), 0);
        std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> owned_store(store, &ct_page_store_free);
        config.page_store       = store;
        config.block_size       = 65536;
        config.frame_bytes      = 4096;
        config.iu_size          = 4096;
        config.max_inline_value = 64;
        config.sync_mode        = CT_SYNC_SKIP;
        ASSERT_EQ(ct_open_with_runtime(&config, runtime, nullptr, &tree), 0);
    }

    void TearDown() override
    {
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    TreeCore &core()
    {
        return *TreeCoreAccess::get(*c_api_tree(tree));
    }

    void put(uint64_t slot, uint8_t key)
    {
        const std::string value(8192, static_cast<char>(key));
        ASSERT_EQ(ct_apply_put(tree, slot, &key, 1, reinterpret_cast<const uint8_t *>(value.data()), value.size()), 0);
    }
};

TEST_F(MaintenanceReportCAbi, FlushReportRetainsFrontiersAndRejectsWrongPollWithoutConsuming)
{
    put(1, 'a');
    put(2, 'b');
    ct_future *future = nullptr;
    ASSERT_EQ(ct_flush_report_async(tree, &future), 0);
    ASSERT_NE(future, nullptr);
    int32_t done = 9;
    EXPECT_EQ(ct_future_poll(future, &done, nullptr, nullptr, nullptr), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(done, 9);
    ct_flush_result_v1 report{};
    report.struct_size = sizeof(report);
    report.version     = 2;
    EXPECT_EQ(ct_future_poll_flush_report(future, &done, &report), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(report.version, 2U);
    report.version = 1;
    core().binding()->lane()->drain();
    ASSERT_EQ(ct_future_poll_flush_report(future, &done, &report), 0);
    ASSERT_EQ(done, 1);
    EXPECT_EQ(report.terminal_status, 0);
    EXPECT_EQ(report.handoff_status, 0);
    EXPECT_EQ(report.maintenance_status, 0);
    EXPECT_EQ(report.handoff_completed, 1);
    EXPECT_EQ(report.handoff_advanced, 1);
    EXPECT_EQ(report.floor, 0U);
    EXPECT_EQ(report.frontier, 2U);
    EXPECT_EQ(report.published_coverage, 2U);
}

TEST_F(MaintenanceReportCAbi, WritebackAndDurableCheckpointKeepDistinctReports)
{
    put(1, 'a');
    ct_future *future = nullptr;
    ASSERT_EQ(ct_writeback_async(tree, &future), 0);
    ct_checkpoint_result_v1 report{};
    report.struct_size = sizeof(report);
    report.version     = 1;
    int32_t done       = 0;
    while (done == 0) {
        ASSERT_EQ(ct_future_poll_checkpoint_report(future, &done, &report), 0);
        if (done == 0) {
            std::this_thread::yield();
        }
    }
    ASSERT_EQ(report.terminal_status, 0);
    EXPECT_EQ(report.captured_coverage, 1U);
    EXPECT_EQ(report.writeback_completed, 1);
    EXPECT_EQ(report.durable_committed, 0);
    EXPECT_EQ(report.durable_coverage, 0U);
    EXPECT_EQ(ct_last_applied_slot(tree), 1U);
    EXPECT_EQ(ct_durable_coverage(tree), 0U);
    EXPECT_GT(report.written_frames, report.primary_frames);
    EXPECT_GT(report.rebased_pages, 0U);
    ASSERT_EQ(ct_checkpoint_report_async(tree, &future), 0);
    done = 0;
    while (done == 0) {
        ASSERT_EQ(ct_future_poll_checkpoint_report(future, &done, &report), 0);
        if (done == 0) {
            std::this_thread::yield();
        }
    }
    ASSERT_EQ(report.terminal_status, 0);
    EXPECT_EQ(report.durable_committed, 1);
    EXPECT_EQ(ct_durable_coverage(tree), 1U);
    EXPECT_EQ(report.durable_coverage, 1U);
    EXPECT_GT(report.snapshot_sequence, 0U);
}

TEST_F(MaintenanceReportCAbi, AbandonedPendingFutureKeepsItsConnectedOperationUntilTreeClose)
{
    struct Gate
    {
        std::atomic<bool> entered{false};
        std::atomic<bool> release{false};
    } gate;

    put(1, 'a');
    ASSERT_TRUE(core()
                    .binding()
                    ->cpu()
                    ->try_submit(
                        core().binding()->lane(),
                        [](void *context) noexcept {
                            auto &wait = *static_cast<Gate *>(context);
                            wait.entered.store(true, std::memory_order_release);
                            wait.entered.notify_one();
                            while (!wait.release.load(std::memory_order_acquire)) {
                                wait.release.wait(false);
                            }
                        },
                        &gate)
                    .ok());
    while (!gate.entered.load(std::memory_order_acquire)) {
        gate.entered.wait(false);
    }
    ct_future *future  = nullptr;
    const auto started = ct_flush_report_async(tree, &future);
    if (started != 0) {
        gate.release.store(true, std::memory_order_release);
        gate.release.notify_one();
        FAIL() << "flush start rejected: " << started;
    }
    ct_future_free(future);
    std::jthread close([this] { ct_close(tree); });
    gate.release.store(true, std::memory_order_release);
    gate.release.notify_one();
    close.join();
    tree = nullptr;
}

TEST(MaintenanceReportMapping, MaintenanceFailurePreservesCompletedHandoff)
{
    auto future  = make_ct_future({});
    future->kind = ct_future_impl::Kind::kFlushReport;
    FlushResult source;
    source.batch_id           = 5;
    source.floor              = 10;
    source.frontier           = 20;
    source.published_coverage = 20;
    source.maintenance_status = Status::io_error();
    source.deferred_pages     = 7;
    source.phase              = FlushPhase::kComplete;
    source.handoff_advanced   = true;
    c_api_detail::store_report(*future, source, Status::Ok());
    future->done.store(true, std::memory_order_release);
    auto              *handle = reinterpret_cast<ct_future *>(make_ct_future_handle(std::move(future)));
    ct_flush_result_v1 report{};
    report.struct_size = sizeof(report);
    report.version     = 1;
    int32_t done       = 0;
    ASSERT_EQ(ct_future_poll_flush_report(handle, &done, &report), 0);
    EXPECT_EQ(done, 1);
    EXPECT_EQ(report.terminal_status, static_cast<int32_t>(Code::kIoError));
    EXPECT_EQ(report.handoff_status, 0);
    EXPECT_EQ(report.handoff_completed, 1);
    EXPECT_EQ(report.handoff_advanced, 1);
    EXPECT_EQ(report.frontier, 20U);
    EXPECT_EQ(report.published_coverage, 20U);
    EXPECT_EQ(report.deferred_pages, 7U);
}
} // namespace
} // namespace crowdb::tree
