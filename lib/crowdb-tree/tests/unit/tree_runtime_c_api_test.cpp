// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/c_api.h"
#include "runtime/tree_runtime_binding.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

#include <vector>

namespace crowdb::tree
{
namespace
{
TEST(TreeRuntimeCAbi, NullRuntimeUsesTheProcessHostAndEnforcesWriterCredit)
{
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.limits.writer_bytes = 1024 * 1024;
    Config first;
    Config second;
    ASSERT_TRUE(configure_tree_runtime(nullptr, &binding, &first).ok());
    ASSERT_TRUE(configure_tree_runtime(nullptr, nullptr, &second).ok());
    ASSERT_EQ(first.runtime, second.runtime);
    EXPECT_EQ(first.runtime_binding.limits.writer_bytes, 1024U * 1024U);

    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, nullptr, &binding, &tree), 0);
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    EXPECT_EQ(core->config().runtime, first.runtime);
    EXPECT_EQ(core->binding()->options().limits.writer_bytes, 1024U * 1024U);
    const std::vector<uint8_t> value(2 * 1024 * 1024);
    EXPECT_EQ(ct_apply_put(tree, 1, reinterpret_cast<const uint8_t *>("key"), 3, value.data(), value.size()),
              static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(core->contiguous_slot(), 0U);
    ct_close(tree);
}

TEST(TreeRuntimeCAbi, NullRuntimeDoesNotBypassUnfundedOpenOrRebuildBinding)
{
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.lifecycle_bytes = 1;
    ct_options options{};
    ct_tree   *tree = nullptr;
    EXPECT_EQ(ct_open_with_runtime(&options, nullptr, &binding, &tree), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(tree, nullptr);

    ct_tree *source = nullptr;
    ASSERT_EQ(ct_open(&options, &source), 0);
    ct_page_store *store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &store), 0);
    options.page_store = store;
    EXPECT_EQ(ct_rebuild_range_with_runtime(source, &options, nullptr, &binding, &tree, nullptr),
              static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(tree, nullptr);
    ct_page_store_free(store);
    ct_close(source);
}

TEST(TreeRuntimeCAbi, NullRuntimeKeepsTheDefaultReloadableMemoryBackend)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, nullptr, nullptr, &tree), 0);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    uint64_t generation = 0;
    uint64_t coverage   = 0;
    EXPECT_EQ(ct_snapshot_info(tree, &generation, &coverage), 0);
    EXPECT_GT(generation, 0U);
    EXPECT_EQ(coverage, 1U);
    ct_close(tree);
}

TEST(TreeRuntimeCAbi, VersionedOptionsRejectWrongSizeAndUnfundedStorage)
{
    ct_runtime_options options{};
    ct_runtime_options_default(&options);
    EXPECT_EQ(options.struct_size, sizeof(options));
    ct_runtime *runtime = nullptr;
    --options.struct_size;
    EXPECT_EQ(ct_runtime_create(&options, &runtime), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(runtime, nullptr);
    ++options.struct_size;
    options.lifecycle_bytes = 1;
    EXPECT_EQ(ct_runtime_create(&options, &runtime), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(runtime, nullptr);
    ct_runtime_free(nullptr);
}

TEST(TreeRuntimeCAbi, FreeHandleRetainsIndependentConfigAndBindingOwners)
{
    ct_runtime *handle = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &handle), 0);
    Config config;
    ASSERT_TRUE(configure_tree_runtime(handle, nullptr, &config).ok());
    auto                                owner = config.runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(owner, config.runtime_binding, &binding).ok());
    const auto before = owner->snapshot().allocated.routing_bytes;
    ct_runtime_free(handle);
    EXPECT_FALSE(owner->stopped());
    EXPECT_LT(owner->snapshot().allocated.routing_bytes, before);
    config.runtime.reset();
    owner.reset();
    EXPECT_TRUE(binding->cpu()->try_submit(binding->lane(), [](void *) noexcept {}, nullptr).ok());
    binding->cpu()->drain();
}

TEST(TreeRuntimeCAbi, OpenRuntimeUsesOwnedCoreAndRejectsInvalidBinding)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ASSERT_NE(tree, nullptr);
    uint64_t checkpoint = 0;
    EXPECT_EQ(ct_snapshot(tree, &checkpoint), static_cast<int32_t>(Code::kUnavailable));
    ct_close(tree);
    tree = nullptr;
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.cpu_queue_limit = 0;
    EXPECT_EQ(ct_open_with_runtime(&options, runtime, &binding, &tree), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(tree, nullptr);
    ct_runtime_stop_admission(runtime);
    Config config;
    EXPECT_EQ(configure_tree_runtime(runtime, nullptr, &config).code(), Code::kUnavailable);
    ct_runtime_drain_cpu(runtime);
    ct_runtime_free(runtime);
}
} // namespace
} // namespace crowdb::tree
