// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/runtime_c_api.h"
#include "runtime/tree_runtime_binding.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct WaitReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::atomic<bool> *stopped;

    void set_value() const noexcept
    {
        ADD_FAILURE() << "fully occupied budget granted a waiter";
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
    }

    void set_stopped() const noexcept
    {
        stopped->store(true);
    }
};

TEST(RuntimeSnapshot, SeparatesExecutorDurationsAndIncludesCancelledBudgetWaits)
{
    TreeRuntimeOptions options;
    options.limits.handoff_bytes = 64;
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create(options, &runtime).ok());
    TreeBindingOptions tree;
    tree.limits.handoff_bytes = 64;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, tree, &binding).ok());
    const auto          before = runtime->snapshot();
    std::atomic<size_t> completed{0};
    const auto          run = [](void *context) noexcept { static_cast<std::atomic<size_t> *>(context)->fetch_add(1); };
    ASSERT_TRUE(binding->cpu()->try_submit(binding->lane(), run, &completed).ok());
    ASSERT_TRUE(binding->backend_cpu()->try_submit(binding->backend_lane(), run, &completed).ok());
    binding->drain();
    EXPECT_EQ(completed.load(), 2U);
    const auto after = runtime->snapshot();
    EXPECT_GT(after.cpu.completed, before.cpu.completed);
    EXPECT_GT(after.backend.completed, before.backend.completed);
    EXPECT_GT(after.cpu.execute_ns, before.cpu.execute_ns);
    EXPECT_GT(after.backend.execute_ns, before.backend.execute_ns);
    EXPECT_GT(after.cpu.queue_ns, before.cpu.queue_ns);
    EXPECT_GT(after.backend.queue_ns, before.backend.queue_ns);
    EXPECT_EQ(after.queued, 0U);
    EXPECT_EQ(after.inflight, 0U);

    auto occupied = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kHandoff, 64);
    ASSERT_TRUE(occupied);
    detail::ResourceBudget::Request request(binding->account(), detail::BudgetClass::kHandoff, 1);
    std::atomic<bool>               stopped{false};
    auto                            operation = stdexec::connect(request.sender(), WaitReceiver{&stopped});
    stdexec::start(operation);
    EXPECT_EQ(runtime->snapshot().budget_waiters, 1U);
    runtime->stop_admission();
    EXPECT_TRUE(stopped.load());
    const auto terminal = runtime->snapshot();
    EXPECT_EQ(terminal.budget_waiters, 0U);
    EXPECT_EQ(terminal.budget_cancelled, before.budget_cancelled + 1);
    EXPECT_GT(terminal.budget_wait_ns, before.budget_wait_ns);
    EXPECT_GT(terminal.budget_grants, before.budget_grants);
    EXPECT_EQ(runtime->snapshot().budget_wait_ns, terminal.budget_wait_ns);
}

TEST(RuntimeSnapshot, CAbiRejectsUnknownLayoutsWithoutChangingOutput)
{
    ct_runtime *raw = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw, &ct_runtime_free);
    ct_runtime_snapshot_v1                                  sample{};
    sample.struct_size = sizeof(sample);
    sample.version     = 2;
    sample.bindings    = 123;
    EXPECT_EQ(ct_runtime_snapshot(runtime.get(), &sample), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(sample.bindings, 123U);
    sample.version = 1;
    --sample.struct_size;
    EXPECT_EQ(ct_runtime_snapshot(runtime.get(), &sample), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(sample.bindings, 123U);
    sample.struct_size = sizeof(sample);
    ASSERT_EQ(ct_runtime_snapshot(runtime.get(), &sample), 0);
    EXPECT_EQ(sample.bindings, 0U);
    EXPECT_GT(sample.allocated.routing_bytes, 0U);
    const auto allocated = sample.allocated.routing_bytes;
    ASSERT_EQ(ct_runtime_snapshot(runtime.get(), &sample), 0);
    EXPECT_EQ(sample.allocated.routing_bytes, allocated);
    ct_runtime_stop_admission(runtime.get());
    ASSERT_EQ(ct_runtime_snapshot(runtime.get(), &sample), 0);
    EXPECT_EQ(sample.stopped, 1U);
}
} // namespace
} // namespace crowdb::tree
