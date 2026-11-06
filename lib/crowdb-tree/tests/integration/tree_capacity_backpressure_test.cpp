// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "common/test_tree_core_chunk.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/operation_wait.h"

#include <chrono>
#include <future>

namespace crowdb::tree
{
namespace
{
struct TestCapacityTree
{
    std::shared_ptr<test_write::TestCheckpointStore> store = std::make_shared<test_write::TestCheckpointStore>();
    Config                                           config;
    std::shared_ptr<TreeCore>                        core;

    explicit TestCapacityTree(size_t entries = 1)
    {
        TreeRuntimeOptions host;
        host.workers = 1;
        EXPECT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        store->device                               = std::make_shared<MemPageStore>(4096);
        config.page_store                           = store->device.get();
        config.async_page_store                     = store.get();
        config.backend_owner                        = store;
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.leaf_split_bytes                     = 3000;
        config.memtable_flush_entries               = entries;
        config.memtable_flush_bytes                 = SIZE_MAX;
        config.max_memtable_count                   = 2;
        config.runtime_binding.limits.io_operations = 1;
        EXPECT_TRUE(TreeCore::open(config, &core).ok());
    }

    [[nodiscard]] Status put(uint64_t slot) const
    {
        return core->apply(slot, Batch{.ops = {{.key   = std::to_string(slot),
                                                .kind  = OpKind::kPut,
                                                .value = std::string(20000, static_cast<char>('a' + slot))}}});
    }
};
} // namespace

TEST(TreeCapacityBackpressure, OrdinaryWritesWaitForPersistentCompletionOnOneWorker)
{
    TestCapacityTree test;
    ASSERT_TRUE(test.put(1).ok());
    EXPECT_EQ(test.core->durable_coverage(), 0U);
    test.store->defer_at = test.store->events.size() + 1;
    auto second          = std::async(std::launch::async, [&] { return test.put(2); });
    test.store->wait_deferred();
    auto third = std::async(std::launch::async, [&] { return test.put(3); });
    EXPECT_EQ(third.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    EXPECT_EQ(test.core->contiguous_slot(), 2U);
    EXPECT_EQ(test.core->durable_coverage(), 0U);
    test.store->finish();
    const auto second_status = second.get();
    const auto third_status  = third.get();
    ASSERT_TRUE(second_status.ok()) << second_status.to_string();
    ASSERT_TRUE(third_status.ok()) << third_status.to_string();
    EXPECT_GE(test.core->durable_coverage(), 2U);
    EXPECT_EQ(test.core->contiguous_slot(), 3U);
    for (uint64_t slot = 1; slot <= 3; ++slot) {
        GetView value;
        ASSERT_TRUE(test.core->get(std::to_string(slot), &value).ok());
        EXPECT_EQ(value.value().to_string(), std::string(20000, static_cast<char>('a' + slot)));
    }
}

TEST(TreeCapacityBackpressure, WriterBytesPauseBeforeTheMemTableRotationThreshold)
{
    TestCapacityTree test(1000);
    test.core.reset();
    test.config.runtime_binding.limits.writer_bytes = 1024 * 1024;
    ASSERT_TRUE(TreeCore::open(test.config, &test.core).ok());
    test.store->defer_at = test.store->events.size() + 1;
    auto writer          = std::async(std::launch::async, [&] {
        for (uint64_t slot = 1; slot <= 128; ++slot) {
            auto status = test.put(slot);
            if (!status.ok()) {
                return status;
            }
        }
        return Status::Ok();
    });
    test.store->wait_deferred();
    EXPECT_EQ(writer.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    EXPECT_LT(test.core->contiguous_slot(), 128U);
    test.store->finish();
    const auto status = writer.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.core->contiguous_slot(), 128U);
    EXPECT_GT(test.core->durable_coverage(), 0U);
    GetView value;
    ASSERT_TRUE(test.core->get("128", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(20000, static_cast<char>('a' + 128)));
}

TEST(TreeCapacityBackpressure, FailedPersistentFlushKeepsCoverageAndExplicitRepairReopensAdmission)
{
    TestCapacityTree test;
    ASSERT_TRUE(test.put(1).ok());
    test.store->defer_at = test.store->events.size() + 1;
    test.store->fail_at  = test.store->defer_at;
    auto second          = std::async(std::launch::async, [&] { return test.put(2); });
    test.store->wait_deferred();
    test.store->finish();
    ASSERT_TRUE(second.get().ok());
    EXPECT_EQ(test.core->contiguous_slot(), 2U);
    EXPECT_EQ(test.core->durable_coverage(), 0U);
    GetView held;
    ASSERT_TRUE(test.core->get("1", &held).ok());
    EXPECT_EQ(held.value().to_string(), std::string(20000, 'b'));
    test.store->fail_at = 0;
    auto third          = std::async(std::launch::async, [&] { return test.put(3); });
    EXPECT_EQ(third.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    const auto repaired = test.core->checkpoint();
    ASSERT_TRUE(repaired.ok()) << repaired.to_string();
    EXPECT_EQ(test.core->durable_coverage(), 2U);
    const auto third_status = third.get();
    ASSERT_TRUE(third_status.ok()) << third_status.to_string();
    EXPECT_EQ(held.value().to_string(), std::string(20000, 'b'));
}

TEST(TreeCapacityBackpressure, ExplicitCheckpointJoiningCapacityFlushBindsItsOwnCurrentToken)
{
    TestCapacityTree test;
    ASSERT_TRUE(test.put(1).ok());
    test.store->defer_at = test.store->events.size() + 1;
    auto second          = std::async(std::launch::async, [&] { return test.put(2); });
    test.store->wait_deferred();
    auto checkpoint = std::async(std::launch::async, [&] { return test.core->checkpoint(); });
    EXPECT_EQ(checkpoint.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    test.store->finish();
    const auto written   = second.get();
    const auto persisted = checkpoint.get();
    ASSERT_TRUE(written.ok()) << written.to_string();
    ASSERT_TRUE(persisted.ok()) << persisted.to_string();
    EXPECT_EQ(test.core->durable_coverage(), 2U);
    ASSERT_TRUE(test.put(3).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    EXPECT_EQ(test.core->durable_coverage(), 3U);
}

TEST(TreeCapacityBackpressure, AutomaticChunkPersistenceKeepsThePreviouslyPairedWalOffset)
{
    test_chunk::TestCoreChunk test;
    test.config.memtable_flush_entries = 1;
    test.config.memtable_flush_bytes   = SIZE_MAX;
    test.config.max_memtable_count     = 2;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "first").ok());
    test.store->request_wal_offset(4096);
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
    test.store->request_wal_offset(8192);
    ASSERT_TRUE(test.put(2, "second").ok());
    ASSERT_TRUE(test.put(3, "third").ok());
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 3U);
    EXPECT_EQ(test.store->durable_wal_offset(), 4096U);
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.store->durable_wal_offset(), 8192U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), "third");
    EXPECT_EQ(test.tree->durable_snapshot_last_applied_slot(), 3U);
}

TEST(TreeCapacityBackpressure, AsyncFlushJoiningAnOlderCheckpointStillCoversItsEntryPrefix)
{
    TestCapacityTree test(1000);
    ASSERT_TRUE(test.put(1).ok());
    test.store->defer_at = test.store->events.size() + 1;
    auto checkpoint      = std::async(std::launch::async, [&] { return test.core->checkpoint(); });
    test.store->wait_deferred();
    ASSERT_TRUE(test.put(2).ok());
    std::promise<Status> completed;
    auto                 done = completed.get_future();
    test.core->flush_async([&](Status status) { completed.set_value(std::move(status)); });
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    test.store->finish();
    const auto checkpoint_status = checkpoint.get();
    const auto flushed           = done.get();
    ASSERT_TRUE(checkpoint_status.ok()) << checkpoint_status.to_string();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    EXPECT_EQ(test.core->capture()->coverage, 2U);
    EXPECT_EQ(test.core->durable_coverage(), 1U);
}

TEST(TreeCapacityBackpressure, RetainedReaderKeepsTheActualWriterAllocationChargedAfterPersistence)
{
    TestCapacityTree test(1000);
    ASSERT_TRUE(test.put(1).ok());
    GetView held;
    ASSERT_TRUE(test.core->get("1", &held).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    test.core->binding()->drain();
    test.core->publisher().reclaim();
    const auto category = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);
    const auto pinned   = test.core->binding()->account()->snapshot().allocated[category];
    EXPECT_GE(pinned, 20000U);
    EXPECT_EQ(held.value().to_string(), std::string(20000, 'b'));
    held = {};
    test.core->publisher().reclaim();
    test.core->binding()->drain();
    const auto released = test.core->binding()->account()->snapshot().allocated[category];
    EXPECT_LT(released, pinned);
}

TEST(TreeCapacityBackpressure, AnArmedWriteFinishesAcrossCloseWhileCapacityFlushWaitsForIo)
{
    TestCapacityTree test;
    ASSERT_TRUE(test.put(1).ok());
    test.store->defer_at = test.store->events.size() + 1;
    auto second          = std::async(std::launch::async, [&] { return test.put(2); });
    test.store->wait_deferred();
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(test.core
                    ->prepare_apply(
                        1, [](size_t) { return CoreWriteInput{.key = Slice("accepted"), .value = Slice("retained")}; },
                        detail::BudgetClass::kAdmittedWriter, 0, &operation)
                    .ok());
    ASSERT_TRUE(operation->arm().ok());
    auto       closed = std::async(std::launch::async, [&] { test.core->close_and_drain(); });
    const auto until  = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!test.core->closed() && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(test.core->closed());
    std::promise<Status> completed;
    auto                 done = completed.get_future();
    operation->start(
        3,
        [](void *raw, detail::CallbackSignal, Status status) noexcept {
            static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
        },
        &completed);
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    test.store->finish();
    const auto second_status = second.get();
    const auto accepted      = done.get();
    const auto drained       = closed.wait_for(std::chrono::seconds(5));
    if (drained != std::future_status::ready) {
        operation.reset();
    }
    closed.get();
    EXPECT_EQ(drained, std::future_status::ready);
    operation.reset();
    ASSERT_TRUE(second_status.ok()) << second_status.to_string();
    ASSERT_TRUE(accepted.ok()) << accepted.to_string();
    EXPECT_EQ(test.core->contiguous_slot(), 3U);
    EXPECT_GE(test.core->durable_coverage(), 2U);
}

TEST(TreeCapacityBackpressure, PersistentFlushPublishesAllChangesFromEarlierOrdinaryFlushes)
{
    Config             config;
    TreeRuntimeOptions host;
    host.limits = {.handoff_bytes    = 1024ULL << 20,
                   .routing_bytes    = 2048ULL << 20,
                   .reclaim_bytes    = 1024ULL << 20,
                   .writer_bytes     = 2048ULL << 20,
                   .gap_repair_bytes = 512ULL << 20};
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.limits = {.handoff_bytes    = 1024ULL << 20,
                                     .routing_bytes    = 1024ULL << 20,
                                     .reclaim_bytes    = 512ULL << 20,
                                     .writer_bytes     = 2048ULL << 20,
                                     .gap_repair_bytes = 256ULL << 20};
    auto store                    = std::make_shared<MemPageStore>(4096);
    config.page_store             = store.get();
    config.backend_owner          = store;
    config.frame_bytes            = 4096;
    config.leaf_split_bytes       = 3000;
    config.max_inline_value       = 512;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 8;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    for (uint64_t slot = 1; slot <= 28000; ++slot) {
        const auto applied = core->apply(slot, Batch{.ops = {{.key   = std::to_string(100000 + slot),
                                                              .kind  = OpKind::kPut,
                                                              .value = std::string(500, 'v')}}});
        ASSERT_TRUE(applied.ok()) << slot << ": " << applied.to_string();
        if (slot % 4000 == 0) {
            if (slot != 4000 && slot != 24000 && slot != 28000) {
                const auto flushed = core->flush();
                ASSERT_TRUE(flushed.ok()) << slot << ": " << flushed.to_string();
                continue;
            }
            std::shared_ptr<CheckpointOperation> operation;
            const auto prepared = core->prepare_checkpoint(checkpoint_detail::defaults(*core), &operation);
            ASSERT_TRUE(prepared.ok()) << slot << ": " << prepared.to_string();
            const auto status = detail::wait_snapshot_operation(operation);
            ASSERT_TRUE(status.ok()) << slot << ": " << operation->result().status.to_string();
            EXPECT_EQ(core->durable_coverage(), slot);
        }
    }
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    EXPECT_EQ(core->capture()->mapping.copy_bounds().max_inner_bytes, 0U);
    GetView previous;
    ASSERT_TRUE(core->get(std::to_string(100000 + ((28064 * 7919) % 28000) + 1), &previous).ok());
    std::vector<bool> changed(28000);
    for (uint64_t slot = 28001; slot <= 28064; ++slot) {
        const auto id = (slot * 7919) % 28000;
        changed[id]   = true;
        ASSERT_TRUE(core->apply(slot, Batch{.ops = {{.key   = std::to_string(100001 + id),
                                                     .kind  = OpKind::kPut,
                                                     .value = std::string(500, 'w')}}})
                        .ok());
    }
    ASSERT_TRUE(core->flush().ok());
    ASSERT_TRUE(core->checkpoint().ok());
    EXPECT_EQ(core->durable_coverage(), 28064U);
    EXPECT_EQ(previous.value().to_string(), std::string(500, 'v'));
    previous = {};
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    EXPECT_EQ(core->durable_coverage(), 28064U);
    std::vector<std::future<Status>> readers;
    readers.reserve(16);
    for (uint64_t lane = 0; lane < 16; ++lane) {
        readers.push_back(std::async(std::launch::async, [&, lane] {
            for (uint64_t id = lane; id < 28000; id += 16) {
                GetView value;
                auto    status = core->get(std::to_string(100001 + id), &value);
                if (!status.ok()) {
                    return status;
                }
                if (value.value().to_string() != std::string(500, changed[id] ? 'w' : 'v')) {
                    return Status::corruption("persisted value differs after cold reopen");
                }
            }
            return Status::Ok();
        }));
    }
    for (auto &reader : readers) {
        const auto status = reader.get();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
}
} // namespace crowdb::tree
