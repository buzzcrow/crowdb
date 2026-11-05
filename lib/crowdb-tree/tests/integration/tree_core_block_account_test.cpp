// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "snapshot/commit_anchor.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace crowdb::tree
{
namespace
{
constexpr size_t kRouting  = static_cast<size_t>(detail::BudgetClass::kRouting);
constexpr size_t kMetadata = static_cast<size_t>(AllocationKind::kMetadata);

TEST(TreeCoreBlockAccount, ConstructorRecoversAndTransfersExistingBackendWithMonotonicAutoSlots)
{
    tree_test::TempDir              directory("core_block_budget_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 4, 5, 65536, 1, &device).ok());
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 2;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.page_store  = device.get();
    config.frame_bytes = 4096;
    std::shared_ptr<detail::ResourceBudget::Account> first;
    {
        Crowdbtree tree(config);
        first = TreeCoreAccess::get(tree)->binding()->account();
        ASSERT_TRUE(tree.apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "first"}}}).ok());
        ASSERT_TRUE(tree.snapshot().ok());
    }
    EXPECT_GT(first->snapshot().allocated[kRouting], 0U);
    {
        Crowdbtree reopened(config);
        EXPECT_EQ(first->snapshot().allocated[kRouting], 0U);
        EXPECT_EQ(first->snapshot().physical[kMetadata], 0U);
        EXPECT_EQ(reopened.contiguous_slot(), 1U);
        GetView old;
        ASSERT_TRUE(TreeCoreAccess::get(reopened)->get("a", &old).ok());
        ASSERT_TRUE(old.found());
        EXPECT_EQ(old.value().to_string(), "first");
        EXPECT_EQ(old.slot(), 1U);
        ASSERT_TRUE(reopened.put("a", "second").ok());
        EXPECT_EQ(reopened.contiguous_slot(), 2U);
        ASSERT_TRUE(reopened.snapshot().ok());
    }
    {
        Crowdbtree again(config);
        EXPECT_EQ(again.contiguous_slot(), 2U);
        ASSERT_TRUE(again.put("a", "third").ok());
        GetView value;
        ASSERT_TRUE(TreeCoreAccess::get(again)->get("a", &value).ok());
        EXPECT_EQ(value.slot(), 3U);
        EXPECT_EQ(value.value().to_string(), "third");
    }
}

TEST(TreeCoreBlockAccount, FailedAdoptionReleasesDomainAndPrivateCreateCannotBypassIt)
{
    tree_test::TempDir              directory("core_block_fail_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 4, 5, 65536, 1, &device).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store  = device.get();
    config.frame_bytes = 4096;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(config.runtime, config.runtime_binding, &binding).ok());
    std::shared_ptr<TreeCore> bypass;
    EXPECT_EQ(TreeCore::create(config, binding, {}, &bypass).code(), Code::kInvalidArgument);
    EXPECT_FALSE(bypass);
    binding.reset();
    const auto baseline                         = config.runtime->snapshot();
    const auto original_limit                   = config.runtime_binding.limits.routing_bytes;
    config.runtime_binding.limits.routing_bytes = config.runtime_binding.lifecycle_bytes + 1;
    bool rejected                               = false;
    try {
        Crowdbtree candidate(config);
    }
    catch (const std::runtime_error &) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(config.runtime->snapshot().allocated.routing_bytes, baseline.allocated.routing_bytes);
    EXPECT_EQ(config.runtime->snapshot().reserved.routing_bytes, baseline.reserved.routing_bytes);
    ASSERT_TRUE(device->try_claim_runtime_domain());
    device->release_runtime_domain();
    config.runtime_binding.limits.routing_bytes = original_limit;
    {
        Crowdbtree accepted(config);
        ASSERT_TRUE(accepted.put("a", "value").ok());
        ASSERT_TRUE(accepted.snapshot().ok());
    }
    ASSERT_TRUE(device->try_claim_runtime_domain());
    device->release_runtime_domain();
}

TEST(TreeCoreBlockAccount, FailedRecoveryRetainsRealBackendChargeAndReleasesDomainForRetry)
{
    tree_test::TempDir              directory("core_block_recovery_fail_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 4, 5, 65536, 1, &device).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store         = device.get();
    config.frame_bytes        = 4096;
    const auto before_backend = config.runtime->snapshot().allocated.routing_bytes;
    {
        Crowdbtree original(config);
        ASSERT_TRUE(original.put("a", "retained").ok());
        ASSERT_TRUE(original.snapshot().ok());
    }
    std::vector<snapshot::CommitAnchor> anchors;
    ASSERT_TRUE(snapshot::read_valid_anchors(*device, device->iu_size(), &anchors).ok());
    ASSERT_EQ(anchors.size(), 1U);
    uint8_t original = 0;
    ASSERT_TRUE(device->read_at(anchors[0].segdir_addr, &original, 1).ok());
    const uint8_t corrupted = original ^ 1;
    ASSERT_TRUE(device->write_at(anchors[0].segdir_addr, &corrupted, 1).ok());
    std::shared_ptr<TreeCore> rejected;
    const auto                status = TreeCore::open(config, &rejected);
    EXPECT_FALSE(status.ok()) << status.to_string();
    EXPECT_FALSE(rejected);
    EXPECT_GT(config.runtime->snapshot().allocated.routing_bytes, before_backend);
    ASSERT_TRUE(device->try_claim_runtime_domain());
    device->release_runtime_domain();
    ASSERT_TRUE(device->write_at(anchors[0].segdir_addr, &original, 1).ok());
    {
        Crowdbtree retry(config);
        GetView    found;
        ASSERT_TRUE(TreeCoreAccess::get(retry)->get("a", &found).ok());
        ASSERT_TRUE(found.found());
        EXPECT_EQ(found.value().to_string(), "retained");
    }
    device.reset();
    EXPECT_EQ(config.runtime->snapshot().allocated.routing_bytes, before_backend);
}
} // namespace
} // namespace crowdb::tree
