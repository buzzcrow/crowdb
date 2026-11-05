// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <latch>

namespace crowdb::tree
{
namespace
{
TEST(TreeSourceWindow, LargeConfiguredCountMakesContinuousProgressWithinTheReadWindow)
{
    Config config;
    config.memtable_flush_entries = 32;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 1024;
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    for (size_t index = 0; index < 4096; ++index) {
        const auto status = tree.put("key-" + std::to_string(index), "value");
        ASSERT_TRUE(status.ok()) << index << ": " << status.to_string();
        ASSERT_LE(core->capture()->sources->tables.size(), core->read_limits().max_memtables - 1);
    }
    EXPECT_EQ(core->config().max_memtable_count, 1024U);
    EXPECT_GT(core->capture()->coverage, 0U);
    auto status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (size_t index = 0; index < 4096; ++index) {
        GetView value;
        status = core->get("key-" + std::to_string(index), &value);
        ASSERT_TRUE(status.ok()) << index << ": " << status.to_string();
        ASSERT_EQ(value.value().to_string(), "value");
    }
    ASSERT_TRUE(tree.put("after-drain", "next").ok());
}

TEST(TreeSourceWindow, FiveThousandSequentialVersionsFlushWithDefaultQuotas)
{
    Crowdbtree tree(Config{});
    auto      *core = TreeCoreAccess::get(tree);
    for (uint64_t slot = 1; slot <= 5000; ++slot) {
        Batch batch;
        batch.ops.push_back({.key = "key", .kind = OpKind::kPut, .value = std::to_string(slot)});
        const auto status = tree.apply(slot, batch);
        ASSERT_TRUE(status.ok()) << "slot " << slot << ": " << status.to_string();
        // Source-window coverage requires each previous write's physical
        // retirement to finish before admitting the next bounded allocation.
        core->binding()->account()->drain_retirements();
    }
    const auto status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree.contiguous_slot(), 5000U);
    EXPECT_EQ(tree.get_view("key").value().to_string(), "5000");
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(TreeSourceWindow, WriterCreditReturnsOnlyAfterPhysicalRetirement)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.limits.writer_bytes = 2 * 1024 * 1024;
    Crowdbtree tree(config);
    auto      *core    = TreeCoreAccess::get(tree);
    const auto binding = core->binding();
    binding->drain();

    struct Gate
    {
        std::latch entered{1}, release{1};
    } gate;

    auto unblock = [binding](Gate *gate) {
        gate->release.count_down();
        binding->drain();
    };
    std::unique_ptr<Gate, decltype(unblock)> cleanup(&gate, unblock);
    ASSERT_TRUE(binding->cpu()
                    ->try_submit(
                        binding->lane(),
                        [](void *context) noexcept {
                            auto &gate = *static_cast<Gate *>(context);
                            gate.entered.count_down();
                            gate.release.wait();
                        },
                        &gate)
                    .ok());
    gate.entered.wait();
    uint64_t rejected = 0;
    for (uint64_t slot = 1; slot <= 128; ++slot) {
        Batch      batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = std::to_string(slot)}}};
        const auto status = tree.apply(slot, batch);
        if (!status.ok()) {
            EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
            rejected = slot;
            break;
        }
    }
    ASSERT_GT(rejected, 1U);
    EXPECT_EQ(tree.contiguous_slot(), rejected - 1);
    EXPECT_GT(binding->account()->retirements_pending_for_tests(), 0U);
    constexpr auto category = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);
    const auto     held     = binding->account()->snapshot().allocated[category];
    EXPECT_GT(held, config.runtime_binding.limits.writer_bytes / 2);
    cleanup.reset();
    binding->account()->drain_retirements();
    EXPECT_LT(binding->account()->snapshot().allocated[category], held / 2);
    EXPECT_EQ(tree.get_view("key").value().to_string(), std::to_string(rejected - 1));
    const Batch next{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "continued"}}};
    ASSERT_TRUE(tree.apply(rejected, next).ok());
    EXPECT_EQ(tree.contiguous_slot(), rejected);
    EXPECT_EQ(tree.get_view("key").value().to_string(), "continued");
}
#endif

TEST(TreeSourceWindow, CapturedPrefixLeavesRoomForChildReplayAndSuccessor)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_entries = 1;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 1024;
    Crowdbtree source(config);
    for (size_t index = 0; index < 61; ++index) {
        ASSERT_TRUE(source.put("key-" + std::to_string(index), "value").ok());
    }
    ASSERT_EQ(TreeCoreAccess::get(source)->capture()->sources->tables.size(), 62U);
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    auto     status     = source.begin_split_memtable_view(&generation, &frontier);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(frontier, 61U);
    EXPECT_EQ(TreeCoreAccess::get(source)->capture()->sources->tables.size(), 63U);
    Crowdbtree child(config);
    status = child.install_split_memtable_overlay(source, frontier);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto *core = TreeCoreAccess::get(child);
    ASSERT_EQ(core->capture()->sources->tables.size(), 63U);
    std::atomic<bool> done{false};
    Status            written;
    child.get_async("key-0", [&](Status read, GetView value) {
        EXPECT_TRUE(read.ok()) << read.to_string();
        EXPECT_TRUE(value.found());
        written = child.put("replayed", "child");
        done.store(true, std::memory_order_release);
        done.notify_one();
    });
    while (!done.load(std::memory_order_acquire)) {
        done.wait(false, std::memory_order_acquire);
    }
    ASSERT_TRUE(written.ok()) << written.to_string();
    core->binding()->drain();
    EXPECT_LE(core->capture()->sources->tables.size(), core->read_limits().max_memtables);
    status = source.publish_split_memtable_view(generation, frontier, child, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(child.clear_split_memtable_overlay(source).ok());
    ASSERT_TRUE(source.release_split_memtable_view(generation).ok());
    status = child.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(child.contiguous_slot(), 62U);
    for (size_t index = 0; index < 61; ++index) {
        GetView value;
        status = core->get("key-" + std::to_string(index), &value);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(value.value().to_string(), "value");
    }
    EXPECT_EQ(child.get_view("replayed").value().to_string(), "child");
}
} // namespace
} // namespace crowdb::tree
