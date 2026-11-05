// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/admitted_batch.h"

#include <gtest/gtest.h>

#include <array>

namespace crowdb::tree
{
namespace
{
constexpr auto kWriter      = detail::BudgetClass::kAdmittedWriter;
constexpr auto kWriterIndex = static_cast<size_t>(kWriter);

struct TestAdmittedInputs
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(4 * 1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    SourceWriteOptions                               options{
        .account = account, .max_entries = 4, .max_key_bytes = 1024, .max_gap_entries = 2, .max_gap_key_bytes = 512};
    Config config;

    Status admit(const Batch &batch, std::shared_ptr<AdmittedBatch> *out) const
    {
        return AdmittedBatch::create(
            config, options, 1024 * 1024, batch.ops.size(),
            [&](size_t index) {
                const auto &op = batch.ops[index];
                return CoreWriteInput{
                    .key = Slice(op.key), .value = Slice(op.value), .flags = static_cast<uint8_t>(op.kind)};
            },
            kWriter, out);
    }

    [[nodiscard]] size_t usage() const
    {
        const auto snapshot = budget.snapshot();
        return snapshot.allocated[kWriterIndex] + snapshot.reserved[kWriterIndex];
    }
};

TEST(AdmittedBatch, OwnsInputsAndLastDuplicateBeforeConsensusWithoutSourceEntry)
{
    TestAdmittedInputs             fixture;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, {}, &frontier).ok());
    std::shared_ptr<AdmittedBatch> admitted;
    {
        Batch input;
        input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = "old"});
        input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = "chosen"});
        input.ops.push_back({.key = "z", .kind = OpKind::kPut, .value = "tail"});
        ASSERT_TRUE(fixture.admit(input, &admitted).ok());
    }
    EXPECT_EQ(sources.catalog()->active->count(), 0U);
    EXPECT_EQ(admitted->size(), 2U);
    auto blocker = detail::ResourceBudget::try_reserve(fixture.account, kWriter,
                                                       TestAdmittedInputs::limits()[kWriterIndex] - fixture.usage());
    ASSERT_TRUE(blocker);
    ASSERT_TRUE(admitted->try_commit(sources, *frontier, 1).ok());
    EXPECT_TRUE(admitted->consumed());
    EXPECT_EQ(frontier->contiguous().load(), 1U);
    const auto  table = sources.catalog()->active;
    auto        guard = table->read_guard();
    const auto *cell  = table->find("k");
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(CellView(cell->cell.slice()).value(), Slice("chosen"));
    EXPECT_EQ(admitted->try_commit(sources, *frontier, 2).code(), Code::kInvalidArgument);
}

TEST(AdmittedBatch, BusySourcePreservesInputsAndReservationUntilSuccessorIsPublished)
{
    TestAdmittedInputs fixture;
    fixture.options.max_entries = 1;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, {}, &frontier).ok());
    Batch input;
    input.ops.push_back({.key = "a", .kind = OpKind::kPut, .value = "one"});
    std::shared_ptr<AdmittedBatch> first;
    ASSERT_TRUE(fixture.admit(input, &first).ok());
    ASSERT_TRUE(first->try_commit(sources, *frontier, 1).ok());
    std::shared_ptr<AdmittedBatch> second;
    ASSERT_TRUE(fixture.admit(input, &second).ok());
    EXPECT_EQ(second->try_commit(sources, *frontier, 2).code(), Code::kUnavailable);
    EXPECT_FALSE(second->consumed());
    auto reserved = detail::ResourceBudget::try_reserve(fixture.account, detail::BudgetClass::kHandoff, 128 * 1024);
    if (!reserved) {
        FAIL() << "handoff credit unavailable";
    }
    auto                           credit = detail::AllocationReservation::create(std::move(reserved.value()));
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(sources.try_capture(frontier->contiguous(), credit, &capture).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    ASSERT_TRUE(sources.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(second->try_commit(sources, *frontier, 2).ok());
    EXPECT_EQ(frontier->contiguous().load(), 2U);
}

TEST(AdmittedBatch, ChosenGapKeepsItsOrdinaryMemoryReservation)
{
    TestAdmittedInputs fixture;
    fixture.options.max_entries = 1;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, {}, &frontier).ok());
    Batch input;
    input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = "value"});
    std::shared_ptr<AdmittedBatch> gap;
    std::shared_ptr<AdmittedBatch> ahead;
    ASSERT_TRUE(fixture.admit(input, &gap).ok());
    ASSERT_TRUE(fixture.admit(input, &ahead).ok());
    ASSERT_TRUE(ahead->try_commit(sources, *frontier, 2).ok());
    EXPECT_EQ(frontier->contiguous().load(), 0U);
    ASSERT_TRUE(gap->try_commit(sources, *frontier, 1).ok());
    EXPECT_EQ(frontier->contiguous().load(), 2U);
    const auto category = static_cast<size_t>(detail::BudgetClass::kGapRepair);
    EXPECT_EQ(fixture.budget.snapshot().reserved[category] + fixture.budget.snapshot().allocated[category], 0U);
}

TEST(AdmittedBatch, HigherGapCannotConsumeTheLastContiguousSourceAllowance)
{
    TestAdmittedInputs fixture;
    fixture.options.max_entries = fixture.options.max_gap_entries = 1;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, {}, &frontier).ok());
    Batch input;
    input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = "value"});
    std::array<std::shared_ptr<AdmittedBatch>, 3> admitted;
    for (auto &batch : admitted) {
        ASSERT_TRUE(fixture.admit(input, &batch).ok());
    }
    // The newest slot uses the ordinary source allowance. A later execution
    // of slot 2 must leave the independent gap allowance for missing slot 1.
    ASSERT_TRUE(admitted[2]->try_commit(sources, *frontier, 3).ok());
    EXPECT_EQ(admitted[1]->try_commit(sources, *frontier, 2).code(), Code::kUnavailable);
    EXPECT_FALSE(admitted[1]->consumed());
    EXPECT_EQ(sources.catalog()->active_budget->resource_usage()[2].used, 0U);
    ASSERT_TRUE(admitted[0]->try_commit(sources, *frontier, 1).ok());
    EXPECT_EQ(frontier->contiguous().load(), 1U);
    EXPECT_EQ(sources.catalog()->active_budget->resource_usage()[2].used, 1U);

    auto reserved = detail::ResourceBudget::try_reserve(fixture.account, detail::BudgetClass::kHandoff, 128 * 1024);
    if (!reserved) {
        FAIL() << "handoff credit unavailable";
    }
    auto                           credit = detail::AllocationReservation::create(std::move(reserved.value()));
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(sources.try_capture(frontier->contiguous(), credit, &capture).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()));
    ASSERT_TRUE(sources.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(admitted[1]->try_commit(sources, *frontier, 2).ok());
    EXPECT_EQ(frontier->contiguous().load(), 3U);
}

TEST(AdmittedBatch, HigherGapsUseFreeOrdinaryCapacityWithoutTakingTheProgressAllowance)
{
    TestAdmittedInputs fixture;
    fixture.options.max_gap_entries = 1;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 8, {}, &frontier).ok());
    Batch input;
    input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = "value"});
    std::array<std::shared_ptr<AdmittedBatch>, 3> admitted;
    for (auto &batch : admitted) {
        ASSERT_TRUE(fixture.admit(input, &batch).ok());
    }
    ASSERT_TRUE(admitted[2]->try_commit(sources, *frontier, 3).ok());
    ASSERT_TRUE(admitted[1]->try_commit(sources, *frontier, 2).ok());
    auto usage = sources.catalog()->active_budget->resource_usage();
    EXPECT_EQ(usage[0].used, 2U);
    EXPECT_EQ(usage[2].used, 0U);
    ASSERT_TRUE(admitted[0]->try_commit(sources, *frontier, 1).ok());
    EXPECT_EQ(frontier->contiguous().load(), 3U);
    usage = sources.catalog()->active_budget->resource_usage();
    EXPECT_EQ(usage[0].used, 2U);
    EXPECT_EQ(usage[2].used, 1U);
}

TEST(AdmittedBatch, CancellationAndInvalidInputsReturnAllPreproposalCredit)
{
    TestAdmittedInputs fixture;
    const auto         baseline = fixture.usage();
    {
        Batch input;
        input.ops.push_back({.key = "k", .kind = OpKind::kPut, .value = std::string(4096, 'v')});
        std::shared_ptr<AdmittedBatch> admitted;
        ASSERT_TRUE(fixture.admit(input, &admitted).ok());
        EXPECT_GT(fixture.usage(), baseline);
    }
    EXPECT_EQ(fixture.usage(), baseline);
    Batch invalid;
    invalid.ops.push_back(
        {.key = std::string(fixture.options.max_key_size + 1, 'k'), .kind = OpKind::kPut, .value = "v"});
    std::shared_ptr<AdmittedBatch> rejected;
    EXPECT_EQ(fixture.admit(invalid, &rejected).code(), Code::kInvalidArgument);
    EXPECT_FALSE(rejected);
    EXPECT_EQ(fixture.usage(), baseline);
}

TEST(AdmittedBatch, NoOpRespectsTheBoundedFrontierAndDoesNotSkipGaps)
{
    TestAdmittedInputs             fixture;
    SourceAdmission                sources(0, 1, fixture.options);
    std::shared_ptr<ApplyFrontier> frontier;
    ASSERT_TRUE(ApplyFrontier::create(0, 2, {}, &frontier).ok());
    std::shared_ptr<AdmittedBatch> noop;
    ASSERT_TRUE(fixture.admit({}, &noop).ok());
    EXPECT_EQ(noop->try_commit(sources, *frontier, 3).code(), Code::kResourceExhausted);
    EXPECT_FALSE(noop->consumed());
    ASSERT_TRUE(noop->try_commit(sources, *frontier, 2).ok());
    EXPECT_EQ(frontier->contiguous().load(), 0U);
    EXPECT_EQ(sources.catalog()->active->count(), 0U);
}
} // namespace
} // namespace crowdb::tree
