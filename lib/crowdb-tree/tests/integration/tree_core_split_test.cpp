// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"
#include "local_checkpoint_test_support.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
class TreeCoreSplit : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions options;
        options.workers = 1;
        ASSERT_TRUE(TreeRuntime::create(options, &config.runtime).ok());
        config.frame_bytes       = 4096;
        config.leaf_split_bytes  = 3000;
        config.buffer_pool_bytes = 4 * 1024 * 1024;
        parent                   = std::make_unique<Crowdbtree>(config);
    }

    std::unique_ptr<Crowdbtree> child(KeyRange range = KeyRange::unbounded())
    {
        auto options      = config;
        options.key_range = std::move(range);
        return std::make_unique<Crowdbtree>(options);
    }

    static Status put(Crowdbtree &tree, uint64_t slot, std::string key, std::string value)
    {
        return tree.apply(slot,
                          Batch{.ops = {{.key = std::move(key), .kind = OpKind::kPut, .value = std::move(value)}}});
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> parent;
};

TEST_F(TreeCoreSplit, ContinuingParentKeepsChildPrefixAndIndependentFutureWrites)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 1, "x", "inherited").ok());
    ASSERT_TRUE(put(*parent, 2, "a", "apple").ok());
    auto status = destination->install_split_memtable_overlay(*parent, 2);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(destination->last_applied_slot(), 0U);
    EXPECT_EQ(destination->contiguous_slot(), 2U);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "inherited");
    EXPECT_FALSE(destination->flush().ok());
    EXPECT_FALSE(destination->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(put(*destination, 3, "x", "child").ok());
    ASSERT_TRUE(put(*parent, 3, "a", "banana").ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    status              = parent->begin_split_memtable_view(&generation, &frontier);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(frontier, 3U);
    status = parent->publish_split_memtable_view(generation, 2, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(destination->last_applied_slot(), 2U);
    EXPECT_EQ(destination->contiguous_slot(), 3U);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "child");
    EXPECT_EQ(destination->get_view("a").value().to_string(), "apple");
    ASSERT_TRUE(destination->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(destination->flush().ok());
    EXPECT_EQ(destination->last_applied_slot(), 3U);
    ASSERT_TRUE(parent->flush().ok());
    EXPECT_EQ(parent->get_view("a").value().to_string(), "banana");
}

TEST_F(TreeCoreSplit, TwoChildrenPublishOnlyTheirHalfOpenRanges)
{
    const auto left_range  = KeyRange::bounded({}, "m");
    const auto right_range = KeyRange::bounded("m", {});
    auto       left        = child(left_range);
    auto       right       = child(right_range);
    ASSERT_TRUE(put(*parent, 1, "a", "left").ok());
    ASSERT_TRUE(put(*parent, 2, "m", "right").ok());
    ASSERT_TRUE(left->install_split_memtable_overlay(*parent, 2).ok());
    ASSERT_TRUE(right->install_split_memtable_overlay(*parent, 2).ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    EXPECT_EQ(frontier, 2U);
    auto status = parent->publish_split_memtable_view(generation, 2, *left, left_range);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = parent->publish_split_memtable_view(generation, 2, *right, right_range);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(left->get_view("a").value().to_string(), "left");
    EXPECT_FALSE(left->get_view("m").found());
    EXPECT_EQ(right->get_view("m").value().to_string(), "right");
    EXPECT_FALSE(right->get_view("a").found());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(parent->flush().ok());
}

TEST_F(TreeCoreSplit, InheritedVersionsAndReverseScanSurviveSourceDestruction)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 4, "x", "future-parent").ok());
    ASSERT_TRUE(put(*parent, 1, "x", "inherited").ok());
    ASSERT_TRUE(put(*parent, 2, "a", "first").ok());
    ASSERT_TRUE(destination->install_split_memtable_overlay(*parent, 2).ok());
    parent.reset();
    EXPECT_EQ(destination->get_view("x").value().to_string(), "inherited");
    for (bool reverse : {false, true}) {
        std::vector<scan_entry> entries;
        bool                    truncated = false;
        const auto              status =
            TreeCoreAccess::get(*destination)
                ->scan({.prefix = {}, .start = {}, .end = {}, .begin = {}, .limit = 8, .reverse = reverse}, &entries,
                       &truncated);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(entries.size(), 2U);
        EXPECT_FALSE(truncated);
        EXPECT_EQ(entries[reverse ? 0 : 1].key, "x");
        EXPECT_EQ(entries[reverse ? 0 : 1].slot, 1U);
    }
}

TEST_F(TreeCoreSplit, InvalidSessionAndRangeLeaveDestinationUntouched)
{
    auto destination = child(KeyRange::bounded("a", "z"));
    ASSERT_TRUE(put(*parent, 1, "x", "value").ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    EXPECT_FALSE(
        parent->publish_split_memtable_view(generation + 1, 1, *destination, KeyRange::bounded("a", "z")).ok());
    EXPECT_FALSE(parent->publish_split_memtable_view(generation, 1, *destination, KeyRange::bounded("a", "m")).ok());
    EXPECT_EQ(destination->contiguous_slot(), 0U);
    EXPECT_FALSE(TreeCoreAccess::get(*destination)->capture()->sources->inheritance);
    EXPECT_FALSE(parent->release_split_memtable_view(generation + 1).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
}

TEST_F(TreeCoreSplit, ExistingDestinationCannotCreditAMissingParentPrefix)
{
    auto destination = child();
    ASSERT_TRUE(put(*destination, 1, "own", "destination").ok());
    ASSERT_TRUE(destination->flush().ok());
    const auto before = TreeCoreAccess::get(*destination)->capture();
    ASSERT_TRUE(put(*parent, 1, "old", "committed-parent").ok());
    ASSERT_TRUE(put(*parent, 2, "other", "second-parent").ok());
    ASSERT_TRUE(parent->flush().ok());
    ASSERT_TRUE(put(*parent, 3, "new", "source-only").ok());
    const auto status = destination->install_split_memtable_overlay(*parent, 3);
    EXPECT_EQ(status.code(), Code::kInvalidArgument) << status.to_string();
    EXPECT_EQ(TreeCoreAccess::get(*destination)->capture(), before);
    EXPECT_EQ(destination->contiguous_slot(), 1U);
    EXPECT_EQ(destination->get_view("own").value().to_string(), "destination");
    EXPECT_FALSE(destination->get_view("old").found());
}

TEST_F(TreeCoreSplit, EmptyChildrenInheritTheFixedL1AndCapturedL0Prefix)
{
    ASSERT_TRUE(put(*parent, 1, "a", "base-left").ok());
    ASSERT_TRUE(put(*parent, 2, "z", "base-right").ok());
    ASSERT_TRUE(parent->flush().ok());
    ASSERT_TRUE(put(*parent, 3, "a", "captured-left").ok());
    ASSERT_TRUE(put(*parent, 4, "n", "captured-right").ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    ASSERT_EQ(frontier, 4U);
    ASSERT_TRUE(put(*parent, 5, "z", "future-parent").ok());
    ASSERT_TRUE(parent->flush().ok());
    ASSERT_TRUE(parent->flush().ok());
    EXPECT_EQ(parent->get_view("z").value().to_string(), "future-parent");
    const auto left_range  = KeyRange::bounded({}, "m");
    const auto right_range = KeyRange::bounded("m", {});
    auto       left        = child(left_range);
    auto       right       = child(right_range);
    auto       status      = parent->publish_split_memtable_view(generation, frontier, *left, left_range);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = parent->publish_split_memtable_view(generation, frontier, *right, right_range);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(left->get_view("a").value().to_string(), "captured-left");
    EXPECT_FALSE(left->get_view("z").found());
    EXPECT_EQ(right->get_view("z").value().to_string(), "base-right");
    EXPECT_EQ(right->get_view("n").value().to_string(), "captured-right");
    EXPECT_FALSE(right->get_view("a").found());
    EXPECT_EQ(left->last_applied_slot(), frontier);
    EXPECT_EQ(right->contiguous_slot(), frontier);
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    parent.reset();
    ASSERT_TRUE(put(*right, 5, "z", "child").ok());
    status = right->flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(right->get_view("z").value().to_string(), "child");
    EXPECT_EQ(left->get_view("a").value().to_string(), "captured-left");
}

TEST_F(TreeCoreSplit, FullyFlushedPrefixInstallsIntoAnEmptyChildWithoutAnL0Interval)
{
    ASSERT_TRUE(put(*parent, 1, "x", "base").ok());
    ASSERT_TRUE(parent->flush().ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    auto destination = child();
    auto status      = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(destination->get_view("x").value().to_string(), "base");
    EXPECT_EQ(destination->last_applied_slot(), frontier);
    EXPECT_FALSE(TreeCoreAccess::get(*destination)->capture()->sources->inheritance);
    const auto installed = TreeCoreAccess::get(*destination)->capture();
    ASSERT_TRUE(parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded()).ok());
    EXPECT_EQ(TreeCoreAccess::get(*destination)->capture(), installed);
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
}

TEST_F(TreeCoreSplit, MultiWindowBaselineCopiesEveryRecordAtTheFixedCoverage)
{
    constexpr uint64_t count = 513;
    for (uint64_t slot = 1; slot <= count; ++slot) {
        ASSERT_TRUE(put(*parent, slot, "key-" + std::to_string(slot), std::string(80, 'v')).ok());
    }
    ASSERT_TRUE(parent->flush().ok());
    EXPECT_GT(TreeCoreAccess::get(*parent)->capture()->stats.page_count(), 1U);
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    auto       destination = child();
    const auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    parent.reset();
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    ASSERT_TRUE(destination->scan({}, {}, {}, 0, 0, false, 0, &entries, &truncated).ok());
    ASSERT_EQ(entries.size(), count);
    EXPECT_FALSE(truncated);
    for (const auto &entry : entries) {
        EXPECT_EQ(entry.key, "key-" + std::to_string(entry.slot));
        EXPECT_EQ(entry.value, std::string(80, 'v'));
    }
    EXPECT_EQ(destination->last_applied_slot(), count);
}

TEST_F(TreeCoreSplit, BaselineQuotaFailureLeavesTheDestinationUnchangedAndCanRetry)
{
    // Leave host headroom for the retained parent while filling the child quota.
    config.runtime_binding.limits.reclaim_bytes = 64 * 1024 * 1024;
    ASSERT_TRUE(put(*parent, 1, "x", "base").ok());
    ASSERT_TRUE(parent->flush().ok());
    ASSERT_TRUE(put(*parent, 2, "y", "captured").ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    for (const auto category : {detail::BudgetClass::kReclaim, detail::BudgetClass::kHandoff}) {
        auto       destination = child();
        auto      *core        = TreeCoreAccess::get(*destination);
        const auto before      = core->capture();
        const auto account     = core->binding()->account();
        const auto sample      = account->snapshot();
        const auto index       = static_cast<size_t>(category);
        const auto used        = sample.allocated[index] + sample.reserved[index];
        const auto limit = category == detail::BudgetClass::kReclaim ? core->binding()->options().limits.reclaim_bytes
                                                                     : core->binding()->options().limits.handoff_bytes;
        auto       held  = detail::ResourceBudget::try_reserve(account, category, limit - used);
        ASSERT_TRUE(held.has_value());
        auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
        EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
        EXPECT_EQ(core->capture(), before);
        EXPECT_EQ(destination->contiguous_slot(), 0U);
        EXPECT_FALSE(destination->get_view("x").found());
        EXPECT_FALSE(destination->get_view("y").found());
        held.reset();
        status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(destination->get_view("x").value().to_string(), "base");
        EXPECT_EQ(destination->get_view("y").value().to_string(), "captured");
    }
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
}

TEST_F(TreeCoreSplit, ColdOverflowBaselineBecomesIndependentOfTheParentBackend)
{
    auto device              = std::make_shared<MemPageStore>(4096);
    auto backend             = std::make_shared<test_write::TestCheckpointStore>();
    backend->device          = device;
    auto options             = config;
    options.page_store       = device.get();
    options.async_page_store = backend.get();
    options.backend_owner    = backend;
    options.max_inline_value = 64;
    parent.reset();
    ASSERT_TRUE(Crowdbtree::open(options, &parent).ok());
    const std::string value(12000, 'v');
    ASSERT_TRUE(put(*parent, 1, "x", value).ok());
    ASSERT_TRUE(parent->snapshot().ok());
    // Checkpoint rebase already publishes a reloadable cold base.
    {
        const auto view = TreeCoreAccess::get(*parent)->capture();
        const auto leaf = view->mapping.get(view->root_page_id)->leaf;
        ASSERT_EQ(leaf->bases.size(), 1U);
        EXPECT_FALSE(leaf->bases.front().resident);
        ASSERT_TRUE(leaf->bases.front().backing);
        EXPECT_TRUE(leaf->bases.front().backing->reloadable());
    }
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    auto       destination = child();
    const auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    parent.reset();
    std::weak_ptr<test_write::TestCheckpointStore> old_backend = backend;
    options.backend_owner.reset();
    backend.reset();
    device.reset();
    EXPECT_TRUE(old_backend.expired());
    EXPECT_EQ(destination->get_view("x").value().to_string(), value);
    EXPECT_EQ(destination->last_applied_slot(), 1U);
}

TEST_F(TreeCoreSplit, CompletedPrefixCanBeRetriedWithoutDuplicateHandoff)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 1, "x", "value").ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto sequence = TreeCoreAccess::get(*destination)->capture()->commit_seq;
    ASSERT_TRUE(parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded()).ok());
    EXPECT_EQ(TreeCoreAccess::get(*destination)->capture()->commit_seq, sequence);
    ASSERT_TRUE(destination->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
}

TEST_F(TreeCoreSplit, PartialCoverageFailureRetriesTheSameBatchAndKeepsChildWrites)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 1, "x", "inherited").ok());
    ASSERT_TRUE(destination->install_split_memtable_overlay(*parent, 1).ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    auto  &publisher    = TreeCoreAccess::get(*destination)->publisher();
    size_t publications = 0;
    publisher.set_before_root_for_tests([&] {
        if (++publications == 3) {
            throw std::bad_alloc();
        }
    });
    const auto failed = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    publisher.set_before_root_for_tests({});
    EXPECT_FALSE(failed.ok());
    EXPECT_EQ(publications, 3U);
    EXPECT_EQ(destination->last_applied_slot(), 0U);
    EXPECT_TRUE(TreeCoreAccess::get(*destination)->capture()->pending_handoff);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "inherited");
    ASSERT_TRUE(put(*destination, 2, "x", "child").ok());
    const auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(destination->last_applied_slot(), 1U);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "child");
    ASSERT_TRUE(destination->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(destination->flush().ok());
}

TEST_F(TreeCoreSplit, UnwrittenEmptyPrefixNeedsNoInheritedSourceOrCoverageMutation)
{
    auto     destination = child();
    uint64_t generation  = 0;
    uint64_t frontier    = 123;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    EXPECT_EQ(frontier, 0U);
    const auto sequence = TreeCoreAccess::get(*destination)->capture()->commit_seq;
    const auto status = parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(TreeCoreAccess::get(*destination)->capture()->commit_seq, sequence);
    EXPECT_FALSE(TreeCoreAccess::get(*destination)->capture()->sources->inheritance);
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(put(*destination, 1, "x", "first").ok());
}

TEST_F(TreeCoreSplit, ChildWriterCanCompleteWhileInheritedCoverageIsPublishing)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 1, "x", "inherited").ok());
    ASSERT_TRUE(destination->install_split_memtable_overlay(*parent, 1).ok());
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    std::promise<void> coverage;
    std::promise<void> resume;
    auto               boundary     = coverage.get_future();
    auto               resumed      = resume.get_future();
    auto              &publisher    = TreeCoreAccess::get(*destination)->publisher();
    size_t             publications = 0;
    publisher.set_before_root_for_tests([&] {
        if (++publications == 3) {
            coverage.set_value();
            resumed.wait();
        }
    });
    auto published = std::async(std::launch::async, [&] {
        return parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    });
    boundary.wait();
    const auto written = put(*destination, 2, "x", "concurrent-child");
    resume.set_value();
    const auto status = published.get();
    publisher.set_before_root_for_tests({});
    ASSERT_TRUE(written.ok()) << written.to_string();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(destination->last_applied_slot(), 1U);
    EXPECT_EQ(destination->contiguous_slot(), 2U);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "concurrent-child");
    ASSERT_TRUE(destination->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(destination->flush().ok());
}

TEST_F(TreeCoreSplit, ParentPartialHandoffRemainsCapturedUntilItsNormalRetry)
{
    auto destination = child();
    ASSERT_TRUE(put(*parent, 1, "x", "inherited").ok());
    auto  &publisher    = TreeCoreAccess::get(*parent)->publisher();
    size_t publications = 0;
    publisher.set_before_root_for_tests([&] {
        if (++publications == 5) {
            throw std::bad_alloc();
        }
    });
    const auto failed = parent->flush();
    publisher.set_before_root_for_tests({});
    ASSERT_FALSE(failed.ok());
    const auto repairing_metrics = publisher.work_metrics()->totals();
    EXPECT_EQ(repairing_metrics[static_cast<size_t>(TreeStage::FlushRepair)].inflight, 1U);
    EXPECT_EQ(repairing_metrics[static_cast<size_t>(TreeStage::FlushRepair)].count, 0U);
    ASSERT_TRUE(TreeCoreAccess::get(*parent)->capture()->pending_handoff);
    const auto sequence  = TreeCoreAccess::get(*parent)->capture()->commit_seq;
    const auto installed = destination->install_split_memtable_overlay(*parent, 1);
    ASSERT_TRUE(installed.ok()) << installed.to_string();
    EXPECT_EQ(TreeCoreAccess::get(*parent)->capture()->commit_seq, sequence);
    EXPECT_EQ(parent->last_applied_slot(), 0U);
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(parent->begin_split_memtable_view(&generation, &frontier).ok());
    const auto published =
        parent->publish_split_memtable_view(generation, frontier, *destination, KeyRange::unbounded());
    ASSERT_TRUE(published.ok()) << published.to_string();
    ASSERT_TRUE(parent->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(parent->flush().ok());
    EXPECT_EQ(parent->last_applied_slot(), 1U);
    EXPECT_EQ(destination->get_view("x").value().to_string(), "inherited");
    const auto repaired_metrics = publisher.work_metrics()->totals();
    EXPECT_EQ(repaired_metrics[static_cast<size_t>(TreeStage::FlushRepair)].inflight, 0U);
    EXPECT_EQ(repaired_metrics[static_cast<size_t>(TreeStage::FlushRepair)].count, 1U);
}
} // namespace
} // namespace crowdb::tree
