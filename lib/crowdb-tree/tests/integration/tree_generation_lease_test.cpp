// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_generation_lease.h"

#include <gtest/gtest.h>

#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
class PreparedGenerationLease : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions options;
        options.workers = 1;
        ASSERT_TRUE(TreeRuntime::create(options, &config.runtime).ok());
        tree = std::make_unique<Crowdbtree>(config);
        core = TreeCoreAccess::get(*tree);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(PreparedGenerationLease, CancelReopensTheUnchangedSourcesAndRoot)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    ASSERT_TRUE(tree->flush().ok());
    const auto                           original = core->capture();
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    ASSERT_TRUE(lease->acquire().ok());
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->put("blocked", "value").code(), Code::kUnavailable);
    lease->cancel();
    lease.reset();
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->get_view("old").value().to_string(), "retained");
    ASSERT_TRUE(tree->put("next", "accepted").ok());
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("next").value().to_string(), "accepted");
}

TEST_F(PreparedGenerationLease, CommitDoesNotWaitForCpuOrReadmitStorage)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    auto                                 borrowed   = tree->get_view("old");
    const auto                           generation = core->capture()->generation;
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    ASSERT_TRUE(lease->acquire().ok());

    struct BlockedWorker
    {
        std::binary_semaphore entered{0};
        std::binary_semaphore release{0};
    } blocked;

    ASSERT_TRUE(core->binding()
                    ->cpu()
                    ->try_submit(
                        core->binding()->lane(),
                        [](void *context) noexcept {
                            auto &worker = *static_cast<BlockedWorker *>(context);
                            worker.entered.release();
                            worker.release.acquire();
                        },
                        &blocked)
                    .ok());
    blocked.entered.acquire();
    config.runtime->stop_admission();
    // A new caller thread has no existing publication Epoch participant. The
    // prepared commit must not attempt a capture or register one here.
    Status       installed;
    std::jthread caller([&] { installed = lease->commit(); });
    caller.join();
    blocked.release.release();
    ASSERT_TRUE(installed.ok()) << installed.to_string();
    EXPECT_EQ(core->capture()->generation, generation + 1);
    EXPECT_EQ(core->capture()->coverage, 0U);
    EXPECT_EQ(core->contiguous_slot(), 0U);
    EXPECT_EQ(borrowed.value().to_string(), "retained");
}

TEST_F(PreparedGenerationLease, CancelBeforeAcquireDoesNotFenceTheTree)
{
    const auto                           original = core->capture();
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    lease.reset();
    EXPECT_EQ(core->capture(), original);
    ASSERT_TRUE(tree->put("key", "value").ok());
    EXPECT_EQ(tree->get_view("key").slot(), 1U);
}
} // namespace
} // namespace crowdb::tree
