// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <thread>

namespace crowdb::tree
{
namespace
{
Batch put_batch(std::string key)
{
    Batch batch;
    batch.ops.push_back({.key = std::move(key), .kind = OpKind::kPut, .value = "value"});
    return batch;
}

TEST(TreeSourceControls, EntryThresholdAndFiniteCountHandoffAllowRepeatedBatches)
{
    Config config;
    config.memtable_flush_entries = 2;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 3;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("a", "value").ok());
    EXPECT_EQ(tree.frozen_table_count(), 0U);
    ASSERT_TRUE(tree.put("b", "value").ok());
    EXPECT_EQ(tree.frozen_table_count(), 1U);
    for (size_t i = 0; i < 30; ++i) {
        const auto status = tree.put("key" + std::to_string(i), "value");
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_LT(tree.frozen_table_count(), config.max_memtable_count);
    }
    EXPECT_GT(TreeCoreAccess::get(tree)->capture()->coverage, 0U);
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.frozen_table_count(), 0U);
    for (size_t i = 0; i < 30; ++i) {
        EXPECT_TRUE(tree.get("key" + std::to_string(i), nullptr, nullptr));
    }
    ASSERT_TRUE(tree.put("after-maintenance", "value").ok());
    EXPECT_TRUE(tree.get("after-maintenance", nullptr, nullptr));
}

TEST(TreeSourceControls, ByteThresholdAndGenerationReplacementKeepIndependentControls)
{
    Config config;
    config.memtable_flush_bytes   = 1;
    config.memtable_flush_entries = UINT32_MAX;
    config.max_memtable_count     = 4;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("before", "value").ok());
    EXPECT_EQ(tree.frozen_table_count(), 1U);
    ASSERT_TRUE(tree.clear().ok());
    EXPECT_EQ(tree.frozen_table_count(), 0U);
    EXPECT_FALSE(tree.get("before", nullptr, nullptr));
    ASSERT_TRUE(tree.put("after", "value").ok());
    EXPECT_EQ(tree.frozen_table_count(), 1U);
    EXPECT_TRUE(tree.get("after", nullptr, nullptr));
}

TEST(TreeSourceControls, MissingPrefixKeepsGapAdmissionAndResumesFiniteDrain)
{
    Config config;
    config.memtable_flush_entries = 1;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 2;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.apply(2, put_batch("two")).ok());
    ASSERT_TRUE(tree.apply(3, put_batch("three")).ok());
    EXPECT_EQ(TreeCoreAccess::get(tree)->capture()->coverage, 0U);
    EXPECT_TRUE(tree.get("two", nullptr, nullptr));
    EXPECT_TRUE(tree.get("three", nullptr, nullptr));
    const auto status = tree.apply(1, put_batch("one"));
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(TreeCoreAccess::get(tree)->capture()->coverage, 3U);
    EXPECT_TRUE(tree.get("one", nullptr, nullptr));
    ASSERT_TRUE(tree.apply(4, put_batch("four")).ok());
}

TEST(TreeSourceControls, SingleWorkerReadCallbackCanWriteAcrossTheSoftThreshold)
{
    Config             config;
    TreeRuntimeOptions runtime;
    runtime.workers         = 1;
    runtime.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime, &config.runtime).ok());
    config.memtable_flush_entries = 2;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 4;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("seed", "value").ok());
    std::atomic<bool> done{false};
    Status            written;
    tree.get_async("seed", [&](Status status, GetView value) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(value.found());
        EXPECT_TRUE(detail::FlushRuntime::on_worker_thread());
        written = tree.put("callback", "value");
        done.store(true, std::memory_order_release);
        done.notify_all();
    });
    while (!done.load(std::memory_order_acquire)) {
        done.wait(false, std::memory_order_acquire);
    }
    ASSERT_TRUE(written.ok()) << written.to_string();
    TreeCoreAccess::get(tree)->binding()->drain();
    EXPECT_EQ(tree.frozen_table_count(), 1U);
    EXPECT_TRUE(tree.get("callback", nullptr, nullptr));
    EXPECT_FALSE(detail::FlushRuntime::on_worker_thread());

    struct BackendWrite
    {
        Crowdbtree       &tree;
        std::atomic<bool> done{false};
        Status            status;

        static void run(void *context) noexcept
        {
            auto &call = *static_cast<BackendWrite *>(context);
            EXPECT_TRUE(detail::FlushRuntime::on_worker_thread());
            call.status = call.tree.put("backend-one", "value");
            if (call.status.ok()) {
                call.status = call.tree.put("backend-two", "value");
            }
            call.done.store(true, std::memory_order_release);
            call.done.notify_all();
        }
    } call{.tree = tree, .done = false, .status = {}};

    const auto &binding = TreeCoreAccess::get(tree)->binding();
    ASSERT_TRUE(binding->backend_cpu()->try_submit(binding->backend_lane(), &BackendWrite::run, &call).ok());
    while (!call.done.load(std::memory_order_acquire)) {
        call.done.wait(false, std::memory_order_acquire);
    }
    ASSERT_TRUE(call.status.ok()) << call.status.to_string();
    binding->drain();
    EXPECT_TRUE(tree.get("backend-two", nullptr, nullptr));
}

TEST(TreeSourceControls, RejectedPublicationLeavesTheCommittedWriteInItsOriginalSource)
{
    Config config;
    config.memtable_flush_entries = 1;
    Crowdbtree tree(config);
    TreeCoreAccess::get(tree)->publisher().close();
    ASSERT_TRUE(tree.put("first", "value").ok());
    EXPECT_EQ(tree.frozen_table_count(), 0U);
    ASSERT_TRUE(tree.put("second", "value").ok());
    EXPECT_TRUE(tree.get("first", nullptr, nullptr));
    EXPECT_TRUE(tree.get("second", nullptr, nullptr));
}

TEST(TreeSourceControls, CloseDrainsRotationQueuedByTheLastAcceptedCallback)
{
    Config             config;
    TreeRuntimeOptions runtime;
    runtime.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime, &config.runtime).ok());
    config.memtable_flush_entries = 2;
    auto tree                     = std::make_unique<Crowdbtree>(config);
    ASSERT_TRUE(tree->put("seed", "value").ok());
    std::atomic<bool> ready{false};
    std::atomic<bool> release{false};
    Status            written;
    tree->get_async("seed", [&](Status status, GetView value) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(value.found());
        written = tree->put("last", "value");
        ready.store(true, std::memory_order_release);
        ready.notify_all();
        while (!release.load(std::memory_order_acquire)) {
            release.wait(false, std::memory_order_acquire);
        }
    });
    while (!ready.load(std::memory_order_acquire)) {
        ready.wait(false, std::memory_order_acquire);
    }
    ASSERT_TRUE(written.ok()) << written.to_string();
    std::thread close([&] { tree.reset(); });
    release.store(true, std::memory_order_release);
    release.notify_all();
    close.join();
    EXPECT_FALSE(tree);
}
} // namespace
} // namespace crowdb::tree
