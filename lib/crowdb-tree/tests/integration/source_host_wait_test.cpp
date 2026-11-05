// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_controller_test.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_split_internal.h"
#include "btree/tree_core_split_wait.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <barrier>
#include <chrono>
#include <thread>

namespace crowdb::tree
{
namespace
{

TEST(SourceHostWait, SplitCaptureWaitsForThePreparingController)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_bytes   = SIZE_MAX;
    config.memtable_flush_entries = UINT32_MAX;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("key", "value").ok());
    auto *core = TreeCoreAccess::get(tree);
    core->binding()->drain();

    struct Gate
    {
        std::atomic<bool> held{false};
        std::atomic<bool> waiting{false};
        std::atomic<bool> release{false};
    } gate;

    auto hold = [](void *context, bool acquired) noexcept {
        auto &state = *static_cast<Gate *>(context);
        if (!acquired) {
            return;
        }
        state.held.store(true, std::memory_order_release);
        state.held.notify_one();
        while (!state.release.load(std::memory_order_acquire)) {
            state.release.wait(false);
        }
    };
    auto observe = [](void *context, bool acquired) noexcept {
        if (!acquired) {
            auto &state = *static_cast<Gate *>(context);
            state.waiting.store(true, std::memory_order_release);
            state.waiting.notify_one();
        }
    };
    Status                          prepared;
    std::shared_ptr<FlushOperation> operation;
    std::thread                     holder([&] {
        set_flush_prepare_hook_for_tests(hold, &gate);
        prepared = core->prepare_flush(&operation);
        set_flush_prepare_hook_for_tests(nullptr, nullptr);
    });
    while (!gate.held.load(std::memory_order_acquire)) {
        gate.held.wait(false);
    }
    Status      captured;
    uint64_t    session  = 0;
    uint64_t    frontier = 0;
    std::thread capture([&] {
        set_flush_prepare_hook_for_tests(observe, &gate);
        captured = tree.begin_split_memtable_view(&session, &frontier);
        set_flush_prepare_hook_for_tests(nullptr, nullptr);
    });
    while (!gate.waiting.load(std::memory_order_acquire)) {
        gate.waiting.wait(false);
    }
    gate.release.store(true, std::memory_order_release);
    gate.release.notify_one();
    holder.join();
    capture.join();
    ASSERT_TRUE(prepared.ok()) << prepared.to_string();
    ASSERT_TRUE(captured.ok()) << captured.to_string();
    EXPECT_EQ(frontier, 1U);
    ASSERT_TRUE(tree.release_split_memtable_view(session).ok());
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_EQ(tree.get_view("key").value().to_string(), "value");
}

TEST(SourceHostWait, SplitCaptureReturnsQuotaFailureWithoutWaiting)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.limits.reclaim_bytes = 4096;
    config.memtable_flush_bytes                 = SIZE_MAX;
    config.memtable_flush_entries               = UINT32_MAX;
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("key", "value").ok());
    uint64_t   session  = 71;
    uint64_t   frontier = 72;
    const auto status   = tree.begin_split_memtable_view(&session, &frontier);
    EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
    EXPECT_EQ(session, 71U);
    EXPECT_EQ(frontier, 72U);
    EXPECT_EQ(tree.get_view("key").value().to_string(), "value");
}

TEST(SourceHostWait, SplitCaptureRejectsWorkerWaitBeforeCreatingASession)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("key", "value").ok());
    auto *core = TreeCoreAccess::get(tree);

    struct Request
    {
        Crowdbtree       *tree;
        uint64_t          session  = 71;
        uint64_t          frontier = 72;
        Status            status;
        std::atomic<bool> done{false};
    } request{.tree = &tree, .status = Status::Ok()};

    auto capture = [](void *context) noexcept {
        auto &state  = *static_cast<Request *>(context);
        state.status = state.tree->begin_split_memtable_view(&state.session, &state.frontier);
        state.done.store(true, std::memory_order_release);
        state.done.notify_one();
    };
    ASSERT_TRUE(core->binding()->cpu()->try_submit(core->binding()->lane(), capture, &request).ok());
    while (!request.done.load(std::memory_order_acquire)) {
        request.done.wait(false);
    }
    EXPECT_EQ(request.status.code(), Code::kUnavailable);
    EXPECT_EQ(request.session, 71U);
    EXPECT_EQ(request.frontier, 72U);
    ASSERT_TRUE(tree.begin_split_memtable_view(&request.session, &request.frontier).ok());
    EXPECT_EQ(request.frontier, 1U);
    EXPECT_TRUE(tree.release_split_memtable_view(request.session).ok());
}

TEST(SourceHostWait, PreparedFlushPreservesInputsAndResumesAnOrdinaryWriter)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_bytes   = SIZE_MAX;
    config.memtable_flush_entries = UINT32_MAX;
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_TRUE(tree.put("first", "before").ok());
    core->binding()->drain();
    std::shared_ptr<FlushOperation> prepared;
    ASSERT_TRUE(core->prepare_flush(&prepared).ok());
    const auto        baseline = core->operation_lifetime().outstanding();
    std::atomic<bool> done{false};
    Status            written;
    std::thread       writer([&] {
        written = tree.put("second", "unmoved");
        done.store(true, std::memory_order_release);
    });
    const auto        until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (core->operation_lifetime().outstanding() <= baseline && !done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_GT(core->operation_lifetime().outstanding(), baseline);
    EXPECT_FALSE(done.load(std::memory_order_acquire));
    const auto flushed = prepared->wait();
    writer.join();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    ASSERT_TRUE(written.ok()) << written.to_string();
    EXPECT_EQ(tree.contiguous_slot(), 2U);
    EXPECT_EQ(tree.get_view("second").value().to_string(), "unmoved");
}

TEST(SourceHostWait, SplitCaptureJoinsAnAlreadyRunningFlush)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.put("key", "value").ok());
    auto                           *core = TreeCoreAccess::get(tree);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(core->prepare_flush(&operation).ok());

    struct Gate
    {
        std::atomic<bool> entered{false};
        std::atomic<bool> release{false};
        FlushOperation   *operation = nullptr;
        Status            worker_wait;
    } gate;

    gate.operation = operation.get();

    auto block = [](void *raw) noexcept {
        auto       &state = *static_cast<Gate *>(raw);
        FlushResult ignored;
        state.worker_wait = state.operation->wait_capture(&ignored);
        state.entered.store(true, std::memory_order_release);
        state.entered.notify_one();
        while (!state.release.load(std::memory_order_acquire)) {
            state.release.wait(false);
        }
    };
    ASSERT_TRUE(core->binding()->cpu()->try_submit(core->binding()->lane(), block, &gate).ok());
    while (!gate.entered.load(std::memory_order_acquire)) {
        gate.entered.wait(false);
    }
    std::atomic<bool> flushed{false};
    Status            flush_status;
    tree.flush_async([&](Status status) {
        flush_status = std::move(status);
        flushed.store(true, std::memory_order_release);
        flushed.notify_one();
    });
    FlushResult duplicate_report;
    const auto  duplicate = detail::wait_split_report(operation->capture_sender(), &duplicate_report);
    EXPECT_EQ(duplicate.code(), Code::kUnavailable);
    EXPECT_EQ(gate.worker_wait.code(), Code::kUnavailable);
    std::atomic<bool> capture_entered{false};
    Status            captured;
    uint64_t          generation = 0;
    uint64_t          frontier   = 0;
    std::thread       capture([&] {
        capture_entered.store(true, std::memory_order_release);
        capture_entered.notify_one();
        captured = tree.begin_split_memtable_view(&generation, &frontier);
    });
    while (!capture_entered.load(std::memory_order_acquire)) {
        capture_entered.wait(false);
    }
    gate.release.store(true, std::memory_order_release);
    gate.release.notify_one();
    capture.join();
    while (!flushed.load(std::memory_order_acquire)) {
        flushed.wait(false);
    }
    ASSERT_TRUE(flush_status.ok()) << flush_status.to_string();
    ASSERT_TRUE(captured.ok()) << captured.to_string();
    EXPECT_EQ(frontier, 1U);
    ASSERT_TRUE(tree.release_split_memtable_view(generation).ok());
    EXPECT_EQ(tree.get_view("key").value().to_string(), "value");
    FlushResult initial;
    ASSERT_TRUE(operation->wait_capture(&initial).ok());
    const auto fixed = operation->frozen_sources();
    ASSERT_TRUE(fixed.base);
    EXPECT_FALSE(fixed.catalog);
    EXPECT_EQ(fixed.base->coverage, 1U);
    ASSERT_TRUE(tree.put("later", "second").ok());
    ASSERT_TRUE(tree.flush().ok());
    FlushResult again;
    ASSERT_TRUE(operation->wait_capture(&again).ok());
    EXPECT_EQ(again.frontier, 1U);
    EXPECT_EQ(operation->frozen_sources().base, fixed.base);
    EXPECT_EQ(core->capture()->coverage, 2U);
}

TEST(SourceHostWait, NewSplitChoosesItsBaselineAfterInterveningNormalHandoffs)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_bytes   = SIZE_MAX;
    config.memtable_flush_entries = UINT32_MAX;
    Crowdbtree source(config);
    auto      *core = TreeCoreAccess::get(source);
    ASSERT_TRUE(source.put("first", "one").ok());
    std::shared_ptr<FlushOperation> old;
    ASSERT_TRUE(core->prepare_flush(&old).ok());
    FlushResult first;
    ASSERT_TRUE(old->wait_capture(&first).ok());
    EXPECT_EQ(first.frontier, 1U);
    ASSERT_TRUE(source.put("second", "two").ok());
    Status interleaved;
    size_t handoffs = 0;
    set_between_split_captures_for_tests([&] {
        for (const auto &key : {"third", "fourth"}) {
            interleaved = source.put(key, key);
            if (!interleaved.ok()) {
                return;
            }
            interleaved = source.flush();
            if (!interleaved.ok()) {
                return;
            }
            ++handoffs;
        }
    });
    uint64_t   session  = 0;
    uint64_t   frontier = 0;
    const auto captured = source.begin_split_memtable_view(&session, &frontier);
    set_between_split_captures_for_tests({});
    ASSERT_TRUE(interleaved.ok()) << interleaved.to_string();
    ASSERT_TRUE(captured.ok()) << captured.to_string();
    EXPECT_EQ(handoffs, 2U);
    EXPECT_EQ(frontier, 4U);
    Crowdbtree child(config);
    const auto published = source.publish_split_memtable_view(session, frontier, child, config.key_range);
    ASSERT_TRUE(published.ok()) << published.to_string();
    uint64_t slot = 1;
    for (const auto &[key, expected] : {
             std::pair{"first",  "one"   },
             std::pair{"second", "two"   },
             std::pair{"third",  "third" },
             std::pair{"fourth", "fourth"}
    }) {
        auto value = child.get_view(key);
        EXPECT_EQ(value.slot(), slot++);
        EXPECT_EQ(value.value().to_string(), expected);
    }
    EXPECT_EQ(child.contiguous_slot(), 4U);
    ASSERT_TRUE(source.release_split_memtable_view(session).ok());
}

TEST(SourceHostWait, ExistingSplitPrefixSurvivesLaterNormalHandoffs)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_bytes   = SIZE_MAX;
    config.memtable_flush_entries = UINT32_MAX;
    Crowdbtree source(config);
    Crowdbtree child(config);
    ASSERT_TRUE(source.put("key", "inherited").ok());
    ASSERT_TRUE(child.install_split_memtable_overlay(source, 1).ok());
    ASSERT_TRUE(child.put("key", "child").ok());
    ASSERT_TRUE(source.put("key", "parent-newer").ok());
    ASSERT_TRUE(source.flush().ok());
    ASSERT_TRUE(source.put("later", "outside-prefix").ok());
    ASSERT_TRUE(source.flush().ok());
    Crowdbtree explicit_target(config);
    auto       status = explicit_target.install_split_memtable_overlay(source, 3);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(explicit_target.contiguous_slot(), 0U);
    EXPECT_FALSE(explicit_target.get_view("key").found());
    uint64_t session  = 0;
    uint64_t frontier = 0;
    status            = source.begin_split_memtable_view(&session, &frontier);
    ASSERT_TRUE(status.ok()) << status.to_string();
    Crowdbtree fresh(config);
    status = source.publish_split_memtable_view(session, frontier, fresh, config.key_range);
    ASSERT_TRUE(status.ok()) << "frontier=" << frontier << " " << status.to_string();
    EXPECT_EQ(frontier, 1U);
    EXPECT_EQ(fresh.get_view("key").slot(), 1U);
    EXPECT_EQ(fresh.get_view("key").value().to_string(), "inherited");
    EXPECT_FALSE(fresh.get_view("later").found());
    ASSERT_TRUE(fresh.put("key", "parent-newer").ok());
    ASSERT_TRUE(fresh.put("later", "outside-prefix").ok());
    EXPECT_EQ(fresh.get_view("key").slot(), 2U);
    EXPECT_EQ(fresh.get_view("key").value().to_string(), "parent-newer");
    EXPECT_EQ(fresh.get_view("later").slot(), 3U);
    EXPECT_EQ(fresh.get_view("later").value().to_string(), "outside-prefix");
    // The originally installed child still publishes its fixed cutover while
    // retaining its independently accepted later write.
    status = source.publish_split_memtable_view(session, 1, child, config.key_range);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(child.clear_split_memtable_overlay(source).ok());
    EXPECT_EQ(child.get_view("key").slot(), 2U);
    EXPECT_EQ(child.get_view("key").value().to_string(), "child");
    EXPECT_FALSE(child.get_view("later").found());
    ASSERT_TRUE(source.release_split_memtable_view(session).ok());
}

TEST(SourceHostWait, ConcurrentFlushCallersJoinFiniteWorkWithoutFailingAdmission)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.memtable_flush_bytes   = SIZE_MAX;
    config.memtable_flush_entries = UINT32_MAX;
    Crowdbtree tree(config);
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(tree.put(std::to_string(i), "value").ok());
    }
    std::barrier               start(4);
    std::array<Status, 4>      results;
    std::array<std::thread, 4> workers;
    for (size_t i = 0; i < workers.size(); ++i) {
        workers[i] = std::thread([&, i] {
            start.arrive_and_wait();
            for (size_t step = 0; step < 16; ++step) {
                results[i] = tree.flush();
                if (!results[i].ok()) {
                    return;
                }
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    for (const auto &result : results) {
        EXPECT_TRUE(result.ok()) << result.to_string();
    }
    EXPECT_EQ(TreeCoreAccess::get(tree)->capture()->coverage, 100U);
}
} // namespace
} // namespace crowdb::tree
