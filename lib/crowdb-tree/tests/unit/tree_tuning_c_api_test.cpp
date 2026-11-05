// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/c_api.h"
#include "runtime/tree_runtime_c_api.h"
#include "runtime/tree_tuning_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeTuningCAbi, ExplicitValuesReachTheEngineAndKeyAdmission)
{
    ct_tree_tuning tuning{};
    ct_tree_tuning_default(&tuning);
    tuning.max_key_size                = 4;
    tuning.source_max_entries          = 128;
    tuning.source_max_key_bytes        = 64 * 1024;
    tuning.source_max_gap_entries      = 32;
    tuning.source_max_gap_key_bytes    = 16 * 1024;
    tuning.flush_parallelism           = 2;
    tuning.run_max_entries             = 64;
    tuning.run_max_key_bytes           = 64 * 1024;
    tuning.run_key_block_bytes         = 512;
    tuning.run_max_bytes               = 512 * 1024;
    tuning.install_run_window          = 16;
    tuning.maintenance_visits_per_step = 8;
    tuning.maintenance_max_visits      = 128;
    tuning.maintenance_max_candidates  = 16;
    tuning.maintenance_max_steps       = 128;
    tuning.maintenance_metadata_bytes  = 512 * 1024;
    tuning.cache_max_pages             = 512;
    ct_options options{};
    options.tuning = &tuning;
    ct_tree *tree  = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    auto       *core = TreeCoreAccess::get(*c_api_tree(tree));
    const auto &e    = core->config().execution;
    EXPECT_EQ(e.source_max_entries, 128U);
    EXPECT_EQ(e.source_max_key_bytes, 64U * 1024);
    EXPECT_EQ(e.source_max_gap_entries, 32U);
    EXPECT_EQ(e.source_max_gap_key_bytes, 16U * 1024);
    EXPECT_EQ(e.flush_parallelism, 2U);
    EXPECT_EQ(e.run_max_entries, 64U);
    EXPECT_EQ(e.run_max_key_bytes, 64U * 1024);
    EXPECT_EQ(e.run_key_block_bytes, 512U);
    EXPECT_EQ(e.run_max_bytes, 512U * 1024);
    EXPECT_EQ(e.install_run_window, 16U);
    EXPECT_EQ(e.maintenance_visits_per_step, 8U);
    EXPECT_EQ(e.maintenance_max_visits, 128U);
    EXPECT_EQ(e.maintenance_max_candidates, 16U);
    EXPECT_EQ(e.maintenance_max_steps, 128U);
    EXPECT_EQ(e.maintenance_metadata_bytes, 512U * 1024);
    EXPECT_EQ(e.cache_max_pages, 512U);
    const uint8_t key[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t value = 'v';
    EXPECT_EQ(ct_apply_put(tree, 1, key, sizeof(key), &value, 1), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_apply_put(tree, 1, key, 4, &value, 1), 0);
    ct_close(tree);
}

TEST(TreeTuningCAbi, InvalidWindowsAndStructSizeFailBeforeOpening)
{
    ct_tree_tuning tuning{};
    ct_tree_tuning_default(&tuning);
    ct_options options{};
    options.tuning = &tuning;
    ct_tree *tree  = nullptr;
    --tuning.struct_size;
    EXPECT_EQ(ct_open(&options, &tree), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(tree, nullptr);
    ++tuning.struct_size;
    tuning.run_key_block_bytes = tuning.run_max_key_bytes + 1;
    EXPECT_EQ(ct_open(&options, &tree), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(tree, nullptr);
    ct_tree_tuning_default(&tuning);
    tuning.maintenance_max_candidates = 0;
    EXPECT_EQ(ct_open(&options, &tree), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(tree, nullptr);
}

TEST(TreeRuntimeCAbi, BackendPoolSettingsCrossTheAbi)
{
    ct_runtime_options options{};
    ct_runtime_options_default(&options);
    options.backend_workers     = 3;
    options.backend_queue_limit = 80;
    ct_runtime *runtime         = nullptr;
    ASSERT_EQ(ct_runtime_create(&options, &runtime), 0);
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.backend_queue_limit = 40;
    Config config;
    ASSERT_TRUE(configure_tree_runtime(runtime, &binding, &config).ok());
    EXPECT_EQ(config.runtime->options().backend_workers, 3U);
    EXPECT_EQ(config.runtime->options().backend_queue_limit, 80U);
    EXPECT_EQ(config.runtime_binding.backend_queue_limit, 40U);
    ct_runtime_free(runtime);
}
} // namespace crowdb::tree
