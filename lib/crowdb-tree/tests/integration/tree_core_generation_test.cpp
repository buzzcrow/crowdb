// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/tree.h"

#include <gtest/gtest.h>

#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
class TreeCoreGeneration : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = 2;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes       = 4096;
        config.leaf_split_bytes  = 3000;
        config.buffer_pool_bytes = 4 * 1024 * 1024;
        tree                     = std::make_unique<Crowdbtree>(config);
        core                     = TreeCoreAccess::get(*tree);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeCoreGeneration, ClearResetsSourcesSlotsAndGcWithoutInvalidatingBorrowedValues)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    ASSERT_TRUE(tree->flush().ok());
    auto borrowed = tree->get_view("old");
    auto original = core->capture();
    tree->set_gc_watermark(100, 100);
    ASSERT_EQ(tree->gc_watermark(), 100U);
    ASSERT_TRUE(tree->apply(3, {}).ok());
    ASSERT_TRUE(tree->clear().ok());
    const auto empty = core->capture();
    EXPECT_EQ(empty->generation, original->generation + 1);
    EXPECT_GT(empty->commit_seq, original->commit_seq);
    EXPECT_EQ(empty->coverage, 0U);
    EXPECT_EQ(empty->gc_floor, 0U);
    EXPECT_EQ(tree->gc_watermark(), 0U);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    EXPECT_EQ(core->max_seen_slot(), 0U);
    EXPECT_EQ(core->memtable_count(), 0U);
    EXPECT_EQ(empty->sources->floor, 0U);
    EXPECT_EQ(empty->sources->tables.size(), 1U);
    EXPECT_FALSE(tree->get_view("old").found());
    EXPECT_EQ(borrowed.value().to_string(), "retained");
    EXPECT_EQ(original->coverage, 1U);
    ASSERT_TRUE(tree->put("new", "value").ok());
    EXPECT_EQ(tree->get_view("new").slot(), 1U);
    ASSERT_TRUE(tree->apply(2, {}).ok());
    EXPECT_EQ(tree->contiguous_slot(), 2U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(core->capture()->coverage, 2U);
}

TEST_F(TreeCoreGeneration, ReplacedControllerReusesPauseCreditAndReleasesItAtClose)
{
    const auto account = core->binding()->account();
    ASSERT_TRUE(tree->put("old", "old-generation").ok());
    ASSERT_TRUE(tree->clear().ok());
    for (size_t round = 0; round < 3; ++round) {
        ASSERT_TRUE(tree->put("key", std::to_string(round)).ok());
        const auto status = tree->flush();
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(core->capture()->coverage, round + 1);
        EXPECT_EQ(tree->get_view("key").value().to_string(), std::to_string(round));
    }
    tree.reset();
    account->drain_retirements();
    const auto closed = account->snapshot();
    for (size_t category = 0; category < detail::ResourceBudget::kClasses; ++category) {
        EXPECT_EQ(closed.allocated[category], 0U) << category;
        EXPECT_EQ(closed.reserved[category], 0U) << category;
    }
}

TEST_F(TreeCoreGeneration, RejectedPreparationPreservesOriginalGenerationAndFutureWrites)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    const auto original = core->capture();
    auto       pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kReclaim,
                                                              config.runtime_binding.limits.reclaim_bytes);
    ASSERT_TRUE(pressure);
    EXPECT_EQ(tree->clear().code(), Code::kResourceExhausted);
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->get_view("old").value().to_string(), "value");
    pressure.reset();
    ASSERT_TRUE(tree->put("next", "value").ok());
    EXPECT_EQ(tree->get_view("next").slot(), 2U);
    ASSERT_TRUE(tree->clear().ok());
    EXPECT_EQ(core->capture()->generation, original->generation + 1);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST_F(TreeCoreGeneration, ReplacementWaitsForAnAlreadyAdmittedWriterBeforeResettingItsFrontier)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    auto                  table = core->capture()->sources->tables.back();
    std::binary_semaphore entered(0);
    std::binary_semaphore resume(0);

    struct Pause
    {
        std::binary_semaphore &entered;
        std::binary_semaphore &resume;
    } pause{.entered = entered, .resume = resume};

    table->set_hook_for_tests(&pause, [](void *argument, ConcurrentSkipList::PausePoint point, Slice) {
        if (point == ConcurrentSkipList::PausePoint::kBeforePrepare) {
            auto &pause = *static_cast<Pause *>(argument);
            pause.entered.release();
            pause.resume.acquire();
        }
    });
    Status            write;
    Status            cleared;
    std::atomic<bool> installed{false};
    core->set_before_generation_install_for_tests([&] { installed.store(true, std::memory_order_release); });
    std::jthread writer([&] { write = tree->put("accepted", "old-generation"); });
    entered.acquire();
    std::jthread replacement([&] { cleared = tree->clear(); });
    // The writer remains an ingress participant, so publication cannot yet
    // reach the generation install hook.
    EXPECT_FALSE(installed.load(std::memory_order_acquire));
    resume.release();
    writer.join();
    replacement.join();
    ASSERT_TRUE(write.ok()) << write.to_string();
    ASSERT_TRUE(cleared.ok()) << cleared.to_string();
    EXPECT_TRUE(installed.load(std::memory_order_acquire));
    EXPECT_FALSE(tree->get_view("accepted").found());
    ASSERT_TRUE(tree->put("new", "value").ok());
    EXPECT_EQ(tree->get_view("new").slot(), 1U);
}

TEST_F(TreeCoreGeneration, CloseWinsWhileReplacementIsDrainingAndAdmissionNeverReopens)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    const auto            original = core->capture();
    std::binary_semaphore before_install(0);
    std::binary_semaphore resume(0);
    std::atomic<bool>     reached_install{false};
    core->set_before_generation_install_for_tests([&] {
        reached_install.store(true, std::memory_order_release);
        before_install.release();
        resume.acquire();
    });
    Status       cleared;
    std::jthread replacement([&] {
        cleared = tree->clear();
        if (!reached_install.load(std::memory_order_acquire)) {
            before_install.release();
        }
    });
    before_install.acquire();
    if (!reached_install.load(std::memory_order_acquire)) {
        replacement.join();
        ASSERT_TRUE(cleared.ok()) << cleared.to_string();
        FAIL() << "replacement unexpectedly skipped its install boundary";
    }
    std::atomic<bool> closed{false};
    std::jthread      closer([&] {
        core->close_and_drain();
        closed.store(true, std::memory_order_release);
    });
    while (!core->closed()) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(closed.load(std::memory_order_acquire));
    EXPECT_EQ(tree->put("rejected", "value").code(), Code::kUnavailable);
    EXPECT_EQ(tree->clear().code(), Code::kUnavailable);
    resume.release();
    replacement.join();
    closer.join();
    EXPECT_EQ(cleared.code(), Code::kUnavailable);
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->put("still-closed", "value").code(), Code::kUnavailable);
}
#endif
} // namespace
} // namespace crowdb::tree
