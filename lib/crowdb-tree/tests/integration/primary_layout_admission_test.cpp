// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/admitted_batch.h"
#include "btree/tree_apply_operation.h"
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class PrimaryLayoutAdmission : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.frame_bytes      = 4096;
        config.max_key_size     = 4096;
        config.max_inline_value = 4096;
        tree                    = std::make_unique<Crowdbtree>(config);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
};

TEST_F(PrimaryLayoutAdmission, RejectedBatchLeavesEveryInputAndFrontierUnchanged)
{
    Batch invalid{
        .ops = {{.key = "first", .kind = OpKind::kPut, .value = "valid"},
                {.key = std::string(4097, 'k'), .kind = OpKind::kPut, .value = "value"}}
    };
    EXPECT_EQ(tree->apply(1, invalid).code(), Code::kInvalidArgument);
    EXPECT_FALSE(tree->get_view("first").found());
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    EXPECT_EQ(tree->put(std::string(4097, 'k'), "value").code(), Code::kInvalidArgument);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    ASSERT_TRUE(tree->put("accepted", "value").ok());
    EXPECT_EQ(tree->contiguous_slot(), 1U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("accepted").value().to_string(), "value");
}

TEST_F(PrimaryLayoutAdmission, EncodedAndExternalInputsUseTheSameDeclaredKeyLimit)
{
    const std::string                   key(4097, 'k');
    std::vector<Crowdbtree::encoded_op> encoded;
    encoded.push_back({.key = key, .cell = encode_cell_buf(1, OpKind::kPut, std::string(4096, 'v'))});
    EXPECT_EQ(tree->apply_encoded(1, std::move(encoded)).code(), Code::kInvalidArgument);
    std::vector<Crowdbtree::external_op> external;
    auto                                 value = buffer::alloc(4096);
    external.push_back({.key = key, .flags = 0, .value = std::move(value)});
    EXPECT_EQ(tree->apply_external(1, std::move(external)).code(), Code::kInvalidArgument);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    EXPECT_FALSE(tree->get_view("encoded").found());
    EXPECT_FALSE(tree->get_view("external").found());
}

TEST_F(PrimaryLayoutAdmission, PreproposalAndReplayRejectBeforeChargingOrClaimingSource)
{
    auto             *core = TreeCoreAccess::get(*tree);
    const std::string value(4096, 'v');
    const std::string key(4097, 'k');
    const auto        input = [&](size_t) { return CoreWriteInput{.key = key, .value = value}; };
    core->binding()->drain();
    const auto baseline = core->binding()->account()->snapshot();
    for (const auto category : {detail::BudgetClass::kAdmittedWriter, detail::BudgetClass::kGapRepair}) {
        std::shared_ptr<TreeApplyOperation> operation;
        EXPECT_EQ(core->prepare_apply(1, input, category, 0, &operation).code(), Code::kInvalidArgument);
        EXPECT_FALSE(operation);
        SourceWriteOptions options{.account = core->binding()->account()};
        options.max_key_size = config.max_key_size;
        std::shared_ptr<AdmittedBatch> batch;
        EXPECT_EQ(AdmittedBatch::create(config, options, 20'000, 1, input, category, &batch).code(),
                  Code::kInvalidArgument);
        EXPECT_FALSE(batch);
    }
    core->binding()->drain();
    const auto after = core->binding()->account()->snapshot();
    EXPECT_EQ(after.allocated, baseline.allocated);
    EXPECT_EQ(after.reserved, baseline.reserved);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    EXPECT_EQ(core->capture()->sources->tables.front()->count(), 0U);
}

TEST_F(PrimaryLayoutAdmission, OverflowAndTombstoneNeedOnlyTheirActualPrimaryCells)
{
    const std::string large(20'000, 'v');
    ASSERT_TRUE(tree->put("overflow", large).ok());
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("overflow").value().to_string(), large);
    Batch erase{.ops = {{.key = "overflow", .kind = OpKind::kDelete, .value = std::string(4096, 'x')}}};
    ASSERT_TRUE(tree->apply(2, erase).ok());
    EXPECT_FALSE(tree->get_view("overflow").found());
}
} // namespace
} // namespace crowdb::tree
