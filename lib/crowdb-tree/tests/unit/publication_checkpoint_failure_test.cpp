// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <future>
#include <memory_resource>

namespace crowdb::tree
{
namespace
{
class CheckpointResource : public std::pmr::memory_resource
{
  public:
    size_t                allocations        = 0;
    size_t                deallocations      = 0;
    size_t                fail_at            = 0;
    size_t                largest_allocation = 0;
    std::function<void()> before_allocate;

  private:
    void *do_allocate(size_t bytes, size_t alignment) override
    {
        largest_allocation = std::max(largest_allocation, bytes);
        if (before_allocate) {
            before_allocate();
        }
        if (++allocations == fail_at) {
            throw std::bad_alloc();
        }
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }

    void do_deallocate(void *pointer, size_t bytes, size_t alignment) override
    {
        ++deallocations;
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }

    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }
};

std::shared_ptr<InstallRecord> pending_record(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = sequence});
}

std::shared_ptr<PublishedPage> pending_leaf()
{
    auto page  = std::make_shared<PublishedPage>();
    page->leaf = std::make_shared<PageState>();
    return page;
}

PublicationResult pending_publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> completed;
    auto                            result = completed.get_future();
    request->on_done                       = [&](PublicationResult value) { completed.set_value(std::move(value)); };
    if (!publisher.try_submit(request)) {
        ADD_FAILURE() << "checkpoint test publication rejected";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

PublicationResult pending_checkpoint(PublicationCoordinator &publisher, CheckpointAction action, uint64_t token)
{
    PublicationTransaction transaction;
    transaction.record     = pending_record(token);
    transaction.checkpoint = {.action = action, .token = token};
    if (action == CheckpointAction::Capture) {
        transaction.expected_coverage = publisher.capture()->coverage;
    }
    return pending_publish(publisher, std::move(transaction));
}

TEST(PublicationCheckpointFailure, EveryMultiPageAllocationFailureRollsBackMembershipsAndDeletion)
{
    size_t failures = 0;
    for (size_t offset = 1; offset <= 8; ++offset) {
        CheckpointResource     resource;
        PublicationCoordinator publisher(8, 1, {}, &resource);
        auto                   original = pending_leaf();
        ASSERT_TRUE(pending_publish(publisher, {.record = pending_record(0),
                                                .pages  = {{.page_id = 0, .expected = {}, .replacement = original}},
                                                .root_page_id = 0})
                        .status.ok());
        ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
        const auto before = publisher.capture();
        auto       record = pending_record(2);
        resource.fail_at  = resource.allocations + offset;
        auto result =
            pending_publish(publisher, {
                                           .record = record,
                                           .pages  = {{.page_id = 0, .expected = original, .replacement = {}},
                                                      {.page_id = 1, .expected = {}, .replacement = pending_leaf()},
                                                      {.page_id = 2, .expected = {}, .replacement = pending_leaf()}},
                                           .root_page_id = 1
        });
        resource.fail_at = 0;
        if (!result.status.ok()) {
            ++failures;
            EXPECT_EQ(result.status.code(), Code::kResourceExhausted);
            EXPECT_EQ(publisher.capture(), before);
            EXPECT_FALSE(record->committed.load());
            EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
        }
        ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
        EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, result.status.ok() ? 2U : 0U);
        EXPECT_EQ(publisher.capture()->stats.checkpoint_mapping_pending, result.status.ok());
    }
    EXPECT_GE(failures, 4U);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(PublicationCheckpointFailure, FinalRootAllocationFailureRollsBackAlreadyPreparedLedger)
{
    detail::ResourceBudget::Limits limits{};
    const auto                     category = static_cast<size_t>(detail::BudgetClass::kHandoff);
    limits[category]                        = 1024 * 1024;
    detail::ResourceBudget budget(limits, 4);
    {
        CheckpointResource     resource;
        PublicationCoordinator publisher(8, 1, {}, &resource);
        auto                   original = pending_leaf();
        ASSERT_TRUE(pending_publish(publisher, {.record = pending_record(0),
                                                .pages  = {{.page_id = 0, .expected = {}, .replacement = original}},
                                                .root_page_id = 0})
                        .status.ok());
        ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
        auto credit = detail::ResourceBudget::try_reserve(budget.make_account(limits), detail::BudgetClass::kHandoff,
                                                          limits[category]);
        if (!credit) {
            FAIL() << "checkpoint metadata reservation was rejected";
        }
        auto reservation         = std::make_shared<detail::AllocationReservation>(std::move(*credit));
        bool reached_pending     = false;
        bool reached_root        = false;
        resource.before_allocate = [&] { reached_pending = true; };
        publisher.set_before_root_for_tests([&] {
            reached_root = true;
            reservation->seal();
        });
        const auto before = publisher.capture();
        auto       record = pending_record(2);
        const auto result =
            pending_publish(publisher, {.record   = record,
                                        .pages    = {{.page_id = 1, .expected = {}, .replacement = pending_leaf()}},
                                        .metadata = reservation});
        publisher.set_before_root_for_tests({});
        resource.before_allocate = {};
        EXPECT_TRUE(reached_pending);
        EXPECT_TRUE(reached_root);
        EXPECT_EQ(result.status.code(), Code::kResourceExhausted);
        EXPECT_EQ(publisher.capture(), before);
        EXPECT_FALSE(record->committed.load());
        // Node-based membership has no retained hash capacity. Rollback frees
        // every allocation made by the failed delta, including both sets.
        EXPECT_EQ(budget.snapshot().allocated[category], 0U);
        EXPECT_EQ(budget.snapshot().reserved[category], 0U);
        ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
        EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
        EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    }
    EXPECT_EQ(budget.snapshot().allocated[category], 0U);
}
#endif

TEST(PublicationCheckpointFailure, CapturedSetNodesAreReleasedOnlyByExplicitReclaim)
{
    CheckpointResource     resource;
    PublicationCoordinator publisher(8, 1, {}, &resource);
    ASSERT_TRUE(pending_publish(publisher, {.record = pending_record(0),
                                            .pages  = {{.page_id = 0, .expected = {}, .replacement = pending_leaf()}},
                                            .root_page_id = 0})
                    .status.ok());
    ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    const auto frees = resource.deallocations;
    ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Capture, 2).status.ok());
    EXPECT_EQ(resource.deallocations, frees);
    publisher.reclaim();
    EXPECT_GT(resource.deallocations, frees);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
}

TEST(PublicationCheckpointFailure, CapturePreparationFailureDoesNotConsumeItsToken)
{
    CheckpointResource     resource;
    PublicationCoordinator publisher(8, 1, {}, &resource);
    const auto             before = publisher.capture();
    resource.fail_at              = resource.allocations + 1;
    EXPECT_EQ(pending_checkpoint(publisher, CheckpointAction::Capture, 1).status.code(), Code::kResourceExhausted);
    EXPECT_EQ(publisher.capture(), before);
    resource.fail_at = 0;
    ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    EXPECT_TRUE(publisher.capture()->checkpoint_inflight);
    ASSERT_TRUE(pending_checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
}

TEST(PublicationCheckpointFailure, OneChangedPageNeverAllocatesTreeSizedMembershipCapacity)
{
    CheckpointResource       resource;
    CheckpointPendingMetrics pending(&resource);
    for (uint64_t pid = 0; pid < 4096; ++pid) {
        const auto before = resource.allocations;
        pending.change_page(pid);
        EXPECT_EQ(resource.allocations - before, 2U);
    }
    EXPECT_LE(resource.largest_allocation, 256U);
    ASSERT_TRUE(pending.capture(1));
    for (uint64_t pid = 4096; pid < 8192; ++pid) {
        const auto before = resource.allocations;
        pending.change_page(pid);
        EXPECT_EQ(resource.allocations - before, 3U);
    }
    EXPECT_LE(resource.largest_allocation, 256U);
    ASSERT_TRUE(pending.finish(1, true));
    TreeLogicalStats stats;
    pending.copy_to(stats);
    EXPECT_EQ(stats.checkpoint_pending_pages, 4096U);
}
} // namespace
} // namespace crowdb::tree
