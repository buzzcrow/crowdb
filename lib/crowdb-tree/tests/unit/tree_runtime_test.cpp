// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/btree/tree.h"
#include "runtime/tree_runtime_binding.h"
#include "runtime/tree_runtime_internal.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
using detail::BudgetClass;
using detail::ResourceBudget;

TEST(TreeRuntime, BindingsShareCpuAndHostBudgetButKeepIndependentQuotas)
{
    TreeRuntimeOptions options;
    options.workers             = 1;
    options.limits.writer_bytes = 100;
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create(options, &runtime).ok());
    const auto baseline = runtime->snapshot();
    EXPECT_GT(baseline.allocated.routing_bytes, 0U);
    EXPECT_EQ(baseline.reserved.routing_bytes + baseline.allocated.routing_bytes, options.lifecycle_bytes);

    TreeBindingOptions binding;
    binding.limits.writer_bytes = 80;
    std::shared_ptr<TreeRuntimeBinding> first;
    std::shared_ptr<TreeRuntimeBinding> second;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, binding, &first).ok());
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, binding, &second).ok());
    EXPECT_EQ(first->cpu(), second->cpu());
    EXPECT_NE(first->lane(), second->lane());
    EXPECT_NE(first->account(), second->account());
    EXPECT_EQ(first->lifecycle_budget()->domain(), second->lifecycle_budget()->domain());
    EXPECT_EQ(runtime->snapshot().bindings, 2U);

    auto a = ResourceBudget::try_reserve(first->account(), BudgetClass::kAdmittedWriter, 60);
    ASSERT_TRUE(a);
    EXPECT_FALSE(ResourceBudget::try_reserve(first->account(), BudgetClass::kAdmittedWriter, 21));
    EXPECT_FALSE(ResourceBudget::try_reserve(second->account(), BudgetClass::kAdmittedWriter, 41));
    auto b = ResourceBudget::try_reserve(second->account(), BudgetClass::kAdmittedWriter, 40);
    ASSERT_TRUE(b);
    EXPECT_EQ(runtime->snapshot().reserved.writer_bytes, 100U);
    a.reset();
    b.reset();
    first.reset();
    second.reset();
    const auto after = runtime->snapshot();
    EXPECT_EQ(after.bindings, 0U);
    EXPECT_EQ(after.reserved.writer_bytes, 0U);
    EXPECT_EQ(after.allocated.routing_bytes, baseline.allocated.routing_bytes);
    EXPECT_EQ(after.reserved.routing_bytes, baseline.reserved.routing_bytes);
}

TEST(TreeRuntime, WeakMetricsControlDoesNotRetainItsTreeAccount)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    const auto                          baseline = runtime->snapshot();
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    auto                                           account      = binding->account();
    auto                                           metrics      = binding->work_metrics();
    std::weak_ptr<detail::ResourceBudget::Account> weak_account = account;
    binding.reset();
    EXPECT_GT(account->snapshot().allocated[1], 0U);
    metrics.reset();
    EXPECT_EQ(account->snapshot().allocated[1], 0U);
    EXPECT_EQ(account->snapshot().reserved[1], 0U);
    EXPECT_EQ(account->snapshot().physical[static_cast<size_t>(AllocationKind::kMetadata)], 0U);
    account.reset();
    EXPECT_TRUE(weak_account.expired());
    weak_account.reset();
    EXPECT_EQ(runtime->snapshot().allocated.routing_bytes, baseline.allocated.routing_bytes);
    EXPECT_EQ(runtime->snapshot().reserved.routing_bytes, baseline.reserved.routing_bytes);
}

TEST(TreeRuntime, LateMetricsReleaseReturnsHostLifecycleAfterExecutorIsGone)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    std::weak_ptr<AllocationBudget>     lifecycle = TreeRuntimeAccess::lifecycle(*runtime);
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    auto metrics = binding->work_metrics();
    binding.reset();
    runtime.reset();
    EXPECT_FALSE(lifecycle.expired());
    metrics.reset();
    EXPECT_TRUE(lifecycle.expired());
}

TEST(TreeRuntime, StoppingAdmissionKeepsAcceptedCpuContinuationsUsable)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    auto credit = ResourceBudget::try_reserve(binding->account(), BudgetClass::kHandoff, 1024);
    ASSERT_TRUE(credit);
    runtime->stop_admission();
    runtime->stop_admission();
    EXPECT_TRUE(runtime->snapshot().stopped);
    EXPECT_FALSE(ResourceBudget::try_reserve(binding->account(), BudgetClass::kHandoff, 1));
    std::shared_ptr<TreeRuntimeBinding> rejected;
    EXPECT_EQ(TreeRuntimeBinding::create(runtime, {}, &rejected).code(), Code::kUnavailable);
    std::atomic<size_t> completed{0};
    const auto          run = [](void *value) noexcept { static_cast<std::atomic<size_t> *>(value)->fetch_add(1); };
    ASSERT_TRUE(binding->cpu()->try_submit(binding->lane(), run, &completed).ok());
    binding->cpu()->drain();
    EXPECT_EQ(completed.load(), 1U);
    EXPECT_EQ(runtime->snapshot().reserved.handoff_bytes, 1024U);
    credit.reset();
    EXPECT_EQ(runtime->snapshot().reserved.handoff_bytes, 0U);
    runtime->drain_cpu();
    EXPECT_EQ(binding->cpu()->try_submit(binding->lane(), run, &completed).code(), Code::kUnavailable);
}

TEST(TreeRuntime, BindingRetainsHostAfterCallerDropsItsReference)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    std::weak_ptr<TreeRuntime>          weak = runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    runtime.reset();
    EXPECT_FALSE(weak.expired());
    std::atomic<size_t> completed{0};
    ASSERT_TRUE(binding->cpu()
                    ->try_submit(
                        binding->lane(),
                        [](void *value) noexcept { static_cast<std::atomic<size_t> *>(value)->fetch_add(1); },
                        &completed)
                    .ok());
    binding->cpu()->drain();
    EXPECT_EQ(completed.load(), 1U);
    binding.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(TreeRuntime, InvalidAndUnfundedRuntimeStorageAreRejectedBeforePoolConstruction)
{
    std::shared_ptr<TreeRuntime> runtime;
    TreeRuntimeOptions           options;
    options.workers = 0;
    EXPECT_EQ(TreeRuntime::create(options, &runtime).code(), Code::kInvalidArgument);
    options.workers = std::numeric_limits<size_t>::max();
    EXPECT_EQ(TreeRuntime::create(options, &runtime).code(), Code::kInvalidArgument);
    options.workers         = 1;
    options.lifecycle_bytes = 256;
    EXPECT_EQ(TreeRuntime::create(options, &runtime).code(), Code::kResourceExhausted);
    EXPECT_FALSE(runtime);
}

TEST(TreeRuntime, FailedBindingCreationReturnsAccountAndReservationStorage)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    const auto                          baseline = runtime->snapshot();
    std::shared_ptr<TreeRuntimeBinding> binding;
    TreeBindingOptions                  options;
    options.lifecycle_bytes = 128;
    EXPECT_EQ(TreeRuntimeBinding::create(runtime, options, &binding).code(), Code::kResourceExhausted);
    options                     = {};
    options.limits.writer_bytes = runtime->options().limits.writer_bytes + 1;
    EXPECT_EQ(TreeRuntimeBinding::create(runtime, options, &binding).code(), Code::kInvalidArgument);
    const auto after = runtime->snapshot();
    EXPECT_FALSE(binding);
    EXPECT_EQ(after.bindings, 0U);
    EXPECT_EQ(after.allocated.routing_bytes, baseline.allocated.routing_bytes);
    EXPECT_EQ(after.reserved.routing_bytes, baseline.reserved.routing_bytes);
}

TEST(TreeRuntime, RuntimeCppConstructionOwnsAnIndependentImmutableNamespace)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    EXPECT_EQ(tree.last_applied_slot(), 0U);
    EXPECT_EQ(tree.leaf_count_atomic(), 1U);
    EXPECT_EQ(tree.tree_state_stats().resources.resident_frames, 0U);
    EXPECT_EQ(tree.tree_state_stats().logical.leaf_pages, 1U);
    auto  output   = std::make_unique<Crowdbtree>();
    auto *original = output.get();
    EXPECT_EQ(Crowdbtree::open(config, &output).code(), Code::kInvalidArgument);
    EXPECT_EQ(output.get(), original);
}
} // namespace
} // namespace crowdb::tree
