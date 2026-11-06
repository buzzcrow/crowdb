// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_core_read_internal.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace crowdb::tree
{
namespace
{
class TreeCoreRead : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
        Config config;
        config.runtime = runtime;
        ASSERT_TRUE(TreeRuntimeBinding::create(runtime, config.runtime_binding, &binding).ok());
        auto status = TreeCore::create(config, binding, {}, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void use_single_io()
    {
        core->close_and_drain();
        core.reset();
        binding.reset();
        Config config;
        config.runtime                              = runtime;
        config.runtime_binding.limits.io_operations = 1;
        ASSERT_TRUE(TreeRuntimeBinding::create(runtime, config.runtime_binding, &binding).ok());
        ASSERT_TRUE(TreeCore::create(config, binding, {}, &core).ok());
    }

    std::shared_ptr<TreeRuntime>        runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::shared_ptr<TreeCore>           core;
};

TEST_F(TreeCoreRead, DefaultAdmissionGetsAndRetainsImmutableValueAfterClose)
{
    Batch batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "original"}}};
    ASSERT_TRUE(core->apply(1, batch).ok());
    GetView value;
    auto    status = core->get("key", &value);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), "original");
    batch.ops[0].value = "replacement";
    ASSERT_TRUE(core->apply(2, batch).ok());
    core->close_and_drain();
    core.reset();
    binding.reset();
    EXPECT_EQ(value.value().to_string(), "original");
    EXPECT_EQ(value.slot(), 1U);
    EXPECT_GT(runtime->snapshot().allocated.writer_bytes, 0U);
    value = {};
    // Late value owners enqueue paid reference cleanup on the host queue.
    runtime->drain_cpu();
    EXPECT_EQ(runtime->snapshot().allocated.writer_bytes, 0U);
}

TEST(TreeCoreReadProgress, SynchronousResidentReadDoesNotDependOnAnOccupiedCpuWorker)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1}, &runtime).ok());
    Config config;
    config.runtime = runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, config.runtime_binding, &binding).ok());
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::create(config, binding, {}, &core).ok());
    ASSERT_TRUE(core->apply(1, {.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());

    struct Gate
    {
        std::atomic<bool> entered{false};
        std::atomic<bool> released{false};
    } gate;

    ASSERT_TRUE(core->binding()
                    ->cpu()
                    ->try_submit(
                        core->binding()->lane(),
                        [](void *context) noexcept {
                            auto &pending = *static_cast<Gate *>(context);
                            pending.entered.store(true, std::memory_order_release);
                            pending.entered.notify_all();
                            pending.released.wait(false, std::memory_order_acquire);
                        },
                        &gate)
                    .ok());
    gate.entered.wait(false, std::memory_order_acquire);
    Status            asynchronous_status;
    GetView           asynchronous_value;
    std::atomic<bool> asynchronous_done{false};
    core->get_async("key", [&](Status outcome, GetView value) {
        asynchronous_status = std::move(outcome);
        asynchronous_value  = std::move(value);
        asynchronous_done.store(true, std::memory_order_release);
    });
    EXPECT_FALSE(asynchronous_done.load(std::memory_order_acquire));
    Status                  status;
    Status                  scan_status;
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    GetView                 result;
    std::atomic<bool>       done{false};
    std::thread             reader([&] {
        status      = core->get("key", &result);
        scan_status = core->scan({}, &entries, &truncated);
        done.store(true, std::memory_order_release);
    });
    const auto              until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    const bool completed_inline = done.load(std::memory_order_acquire);
    gate.released.store(true, std::memory_order_release);
    gate.released.notify_all();
    reader.join();
    core->binding()->drain();
    EXPECT_TRUE(completed_inline);
    EXPECT_TRUE(asynchronous_done.load(std::memory_order_acquire));
    EXPECT_TRUE(asynchronous_status.ok()) << asynchronous_status.to_string();
    EXPECT_TRUE(asynchronous_value.found());
    EXPECT_TRUE(scan_status.ok()) << scan_status.to_string();
    EXPECT_FALSE(truncated);
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries.front().key, "key");
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.value().to_string(), "value");
}

TEST_F(TreeCoreRead, ScanDefaultsAdmitAndRespectEmptyCursorAndDirection)
{
    Batch batch{
        .ops = {{.key = "", .kind = OpKind::kPut, .value = "empty"},
                {.key = "a", .kind = OpKind::kPut, .value = "one"},
                {.key = "b", .kind = OpKind::kPut, .value = "two"}}
    };
    ASSERT_TRUE(core->apply(1, batch).ok());
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    CoreScanOptions         options{};
    options.has_start_bound = true;
    auto status             = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 2U);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(entries[1].key, "b");
    options.reverse         = true;
    options.start_inclusive = true;
    status                  = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].key, "");
}

TEST_F(TreeCoreRead, KeysOnlyTombstonesAndStrictBudgetUseTheSameCapturedRead)
{
    Batch batch{
        .ops = {{.key = "a", .kind = OpKind::kPut, .value = std::string(8192, 'x')},
                {.key = "b", .kind = OpKind::kDelete, .value = ""}}
    };
    ASSERT_TRUE(core->apply(1, batch).ok());
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    CoreScanOptions         options{};
    options.byte_budget        = 2;
    options.keys_only          = true;
    options.include_tombstones = true;
    options.strict_byte_budget = true;
    auto status                = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 2U);
    EXPECT_TRUE(entries[0].value.empty());
    EXPECT_TRUE(entries[1].tombstone);
    options.keys_only          = false;
    options.include_tombstones = false;
    status                     = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(entries.empty());
    EXPECT_TRUE(truncated);
    options.strict_byte_budget = false;
    status                     = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].value.size(), 8192U);
    // The trailing tombstone is excluded, so the bounded probe proves the end.
    EXPECT_FALSE(truncated);
    ASSERT_TRUE(core->apply(2, Batch{.ops = {{.key = "c", .kind = OpKind::kPut, .value = "next"}}}).ok());
    status = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(entries[0].value.size(), 8192U);
    EXPECT_TRUE(truncated);
}

TEST_F(TreeCoreRead, PackedResultKeepsCapacityChargedAfterTreeClose)
{
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ScanPackedBuf packed;
    bool          truncated = false;
    size_t        count     = 0;
    auto          status    = core->scan({}, nullptr, &truncated, &packed, &count);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(count, 1U);
    const auto size = packed.size();
    core->close_and_drain();
    core.reset();
    binding.reset();
    const auto held = runtime->snapshot().allocated.routing_bytes;
    EXPECT_GT(held, size);
    packed = {};
    EXPECT_LT(runtime->snapshot().allocated.routing_bytes, held);
}

TEST(TreeCoreReadProgress, SmallResidentReadsUseActualMemoryInsteadOfWorstCaseWindows)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({.workers = 4}, &runtime).ok());
    Config config;
    config.runtime                              = runtime;
    config.runtime_binding.limits.routing_bytes = 4 * 1024 * 1024;
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, config.runtime_binding, &binding).ok());
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::create(config, binding, {}, &core).ok());
    ASSERT_TRUE(core->apply(1, {.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    GetView value;
    auto    status = core->get("key", &value);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(value.value().to_string(), "value");
    std::atomic<size_t> remaining{32};
    std::atomic<size_t> failures{0};
    for (size_t index = 0; index < 32; ++index) {
        core->get_async("key", [&](Status outcome, GetView result) {
            if (!outcome.ok() || !result.found() || result.value().to_string() != "value") {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
            remaining.fetch_sub(1, std::memory_order_release);
            remaining.notify_all();
        });
    }
    while (const auto pending = remaining.load(std::memory_order_acquire)) {
        remaining.wait(pending, std::memory_order_acquire);
    }
    EXPECT_EQ(failures.load(), 0U);
    ScanPackedBuf packed;
    bool          truncated = false;
    size_t        count     = 0;
    status                  = core->scan({}, nullptr, &truncated, &packed, &count);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 1U);
    EXPECT_FALSE(truncated);
    EXPECT_LT(packed.capacity(), 1024U);
    EXPECT_LT(runtime->snapshot().allocated.routing_bytes, 4 * 1024 * 1024U);
}

TEST_F(TreeCoreRead, TerminalGetReturnsIoCreditBeforeCallingConsumer)
{
    use_single_io();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    std::atomic<bool> done{false};
    Status            first_status;
    Status            second_status;
    bool              first_found  = false;
    bool              second_found = false;
    core->get_async("key", [&](Status status, GetView value) {
        first_status = std::move(status);
        first_found  = value.found();
        core->get_async("key", [&](Status next_status, GetView next_value) {
            second_status = std::move(next_status);
            second_found  = next_value.found();
            done.store(true, std::memory_order_release);
            done.notify_one();
        });
    });
    while (!done.load(std::memory_order_acquire)) {
        done.wait(false, std::memory_order_acquire);
    }
    EXPECT_TRUE(first_status.ok()) << first_status.to_string();
    EXPECT_TRUE(first_found);
    EXPECT_TRUE(second_status.ok()) << second_status.to_string();
    EXPECT_TRUE(second_found);
}

TEST_F(TreeCoreRead, TerminalScanReturnsIoCreditBeforeCallingConsumer)
{
    use_single_io();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    std::atomic<bool> done{false};
    Status            first_status;
    Status            second_status;
    size_t            first_bytes  = 0;
    size_t            second_bytes = 0;
    core->scan_async({}, [&](Status status, ScanPackedBuf packed, bool) {
        first_status = std::move(status);
        first_bytes  = packed.size();
        core->scan_async({}, [&](Status next_status, ScanPackedBuf next_packed, bool) {
            second_status = std::move(next_status);
            second_bytes  = next_packed.size();
            done.store(true, std::memory_order_release);
            done.notify_one();
        });
    });
    while (!done.load(std::memory_order_acquire)) {
        done.wait(false, std::memory_order_acquire);
    }
    EXPECT_TRUE(first_status.ok()) << first_status.to_string();
    EXPECT_GT(first_bytes, 0U);
    EXPECT_TRUE(second_status.ok()) << second_status.to_string();
    EXPECT_EQ(second_bytes, first_bytes);
}
} // namespace
} // namespace crowdb::tree
