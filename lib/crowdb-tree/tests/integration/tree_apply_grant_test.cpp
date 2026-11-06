// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class TreeApplyGrant : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.runtime_binding.limits.gap_repair_bytes = 16ULL * 1024 * 1024;
        tree                                           = std::make_unique<Crowdbtree>(config);
        core                                           = TreeCoreAccess::get(*tree);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeApplyGrant, CompleteGapBoundIncludesBothCopiedInputAndFutureCell)
{
    // A single 8 MiB value fits the read limit and one source's payload,
    // but two owned copies plus descriptors cannot fit the 16 MiB gap budget.
    const std::string                   value(8 * 1024 * 1024, 'v');
    const auto                          before = core->binding()->account()->snapshot();
    std::shared_ptr<TreeApplyOperation> operation;
    const auto                          status = core->prepare_apply(
        1, [&](size_t) { return CoreWriteInput{.key = Slice("key"), .value = Slice(value)}; },
        detail::BudgetClass::kAdmittedWriter, 0, &operation);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    const auto after = core->binding()->account()->snapshot();
    EXPECT_EQ(after.reserved, before.reserved);
    EXPECT_EQ(after.allocated, before.allocated);
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeApplyGrant, RetainedCallerControlsArePartOfTheSameCompleteBound)
{
    std::shared_ptr<TreeApplyOperation> operation;
    EXPECT_EQ(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter,
                                  config.runtime_binding.limits.gap_repair_bytes, &operation)
                  .code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    EXPECT_TRUE(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter, 256, &operation).ok());
    EXPECT_TRUE(operation);
}

TEST_F(TreeApplyGrant, PermanentOrdinaryLimitDoesNotBecomeAnUnfinishableWait)
{
    Config smaller                              = config;
    smaller.runtime_binding.limits.writer_bytes = 1024 * 1024;
    Crowdbtree                          local(smaller);
    const std::string                   value(600 * 1024, 'v');
    std::shared_ptr<TreeApplyOperation> operation;
    EXPECT_EQ(TreeCoreAccess::get(local)
                  ->prepare_apply(
                      1, [&](size_t) { return CoreWriteInput{.key = Slice("key"), .value = Slice(value)}; },
                      detail::BudgetClass::kAdmittedWriter, 0, &operation)
                  .code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(operation);
}

TEST_F(TreeApplyGrant, DuplicateKeysKeepLastCellButChargeAllRetainedInputBytes)
{
    const std::array<std::string, 3>    values{"first", "second", "last"};
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(
        core->prepare_apply(
                values.size(),
                [&](size_t index) { return CoreWriteInput{.key = Slice("same"), .value = Slice(values[index])}; },
                detail::BudgetClass::kAdmittedWriter, 0, &operation)
            .ok());
    ASSERT_TRUE(operation->arm().ok());

    struct Completion
    {
        std::atomic<bool> done{false};
        Status            status;
    } completion;

    operation->start(
        1,
        [](void *raw, detail::CallbackSignal, Status status) noexcept {
            auto &value  = *static_cast<Completion *>(raw);
            value.status = std::move(status);
            value.done.store(true, std::memory_order_release);
            value.done.notify_all();
        },
        &completion);
    while (!completion.done.load(std::memory_order_acquire)) {
        completion.done.wait(false, std::memory_order_acquire);
    }
    EXPECT_TRUE(completion.status.ok());
    EXPECT_EQ(tree->get_view("same").value().to_string(), "last");
}
} // namespace
} // namespace crowdb::tree
