// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "snapshot/operation_wait.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
uint64_t checkpoint_jobs(size_t entries)
{
    MemPageStore store(1);
    Config       config;
    config.page_store                            = &store;
    config.frame_bytes                           = 65536;
    config.memtable_flush_bytes                  = 64 * 1024 * 1024;
    config.execution.maintenance_max_visits      = 1;
    config.execution.maintenance_visits_per_step = 1;
    config.execution.maintenance_max_candidates  = 1;
    config.execution.maintenance_max_steps       = 1;
    config.runtime_binding.limits.handoff_bytes  = 128 * 1024 * 1024;
    config.runtime_binding.limits.reclaim_bytes  = 128 * 1024 * 1024;
    TreeRuntimeOptions host;
    host.workers = 1;
    auto status  = TreeRuntime::create(host, &config.runtime);
    EXPECT_TRUE(status.ok()) << status.to_string();
    Crowdbtree        tree(config);
    const std::string value(16384, 'v');
    for (uint64_t i = 1; i <= entries; ++i) {
        status = tree.apply(
            i, Batch{.ops = {{.key = "key" + std::to_string(10000 + i), .kind = OpKind::kPut, .value = value}}});
        EXPECT_TRUE(status.ok()) << status.to_string();
    }
    status = tree.flush();
    EXPECT_TRUE(status.ok()) << status.to_string();
    auto *core = TreeCoreAccess::get(tree);
    core->binding()->account()->drain_retirements();
    core->binding()->lane()->drain();
    EXPECT_EQ(core->capture()->stats.split_candidate_pages, 0U);
    EXPECT_GT(core->capture()->stats.leaf_pages, 1U);
    const auto                           before = config.runtime->snapshot().cpu.completed;
    std::shared_ptr<CheckpointOperation> operation;
    status = core->prepare_checkpoint(checkpoint_detail::defaults(*core), &operation);
    EXPECT_TRUE(status.ok()) << status.to_string();
    if (!status.ok()) {
        return 0;
    }
    status = detail::wait_snapshot_operation(operation);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(operation->result().durable_coverage, entries);
    EXPECT_EQ(operation->result().structural_windows, 0U);
    operation.reset();
    core->binding()->account()->drain_retirements();
    core->binding()->lane()->drain();
    const auto jobs = config.runtime->snapshot().cpu.completed - before;
    for (uint64_t i = 1; i <= entries; ++i) {
        const auto result = tree.get_view("key" + std::to_string(10000 + i));
        EXPECT_TRUE(result.found());
        EXPECT_EQ(result.value().to_string(), value);
    }
    return jobs;
}
} // namespace

TEST(CheckpointSplitProgress, DoublingFlushOutputKeepsCheckpointWorkBelowQuadraticGrowth)
{
    const auto small = checkpoint_jobs(512);
    const auto large = checkpoint_jobs(1024);
    ASSERT_GT(small, 0U);
    EXPECT_LT(large, 3 * small) << "checkpoint jobs: 512 keys=" << small << ", 1024 keys=" << large;
}
} // namespace crowdb::tree
