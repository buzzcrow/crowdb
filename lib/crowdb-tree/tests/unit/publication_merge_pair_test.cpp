// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"
#include "btree/logical_page.h"
#include "btree/maintenance_operation.h"
#include "btree/published_state.h"
#include "btree/tree_state_reconcile.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
class PublicationMergePair : public ::testing::Test
{
  protected:
    PublicationCoordinator publisher{8, 1};
    uint64_t               sequence = 0;

    PublicationResult publish(PublicationTransaction transaction)
    {
        transaction.record = std::make_shared<InstallRecord>(
            InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = ++sequence});
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = std::move(transaction);
        std::promise<PublicationResult> done;
        auto                            result = done.get_future();
        request->on_done                       = [&](PublicationResult value) { done.set_value(std::move(value)); };
        if (!publisher.try_submit(request)) {
            ADD_FAILURE() << "merge test publication rejected";
            return {.status = Status::unavailable()};
        }
        return result.get();
    }

    void SetUp() override
    {
        PublicationTransaction transaction;
        for (uint64_t pid = 1; pid <= 3; ++pid) {
            auto logical              = std::make_shared<PageState>();
            logical->content_id       = pid;
            logical->route_generation = 1;
            logical->right_sibling    = pid == 3 ? UINT64_MAX : pid + 1;
            if (pid > 1) {
                logical->range.lower = OwnedKey(pid == 2 ? "m" : "t");
            }
            if (pid < 3) {
                logical->range.upper = OwnedKey(pid == 1 ? "m" : "t");
            }
            auto page                     = std::make_shared<PublishedPage>();
            page->leaf                    = std::move(logical);
            page->metrics.page_size_known = true;
            page->metrics.page_size_bytes = kFrameHeaderSize + kFrameTrailerSize;
            page->metrics.kv_known        = true;
            transaction.pages.push_back({.page_id = pid, .expected = {}, .replacement = page});
        }
        auto inner         = std::make_shared<InnerPageState>();
        inner->children    = {1, 2, 3};
        inner->separators  = {OwnedKey("m"), OwnedKey("t")};
        auto page          = std::make_shared<PublishedPage>();
        page->inner        = std::move(inner);
        page->level        = 1;
        page->metrics.leaf = false;
        transaction.pages.push_back({.page_id = 10, .expected = {}, .replacement = page});
        transaction.root_page_id = 10;
        ASSERT_TRUE(publish(std::move(transaction)).status.ok());
    }

    PublicationTransaction classification(uint64_t left, bool ready = true)
    {
        const auto             view = publisher.capture();
        const auto             a    = view->mapping.get(left);
        const auto             b    = view->mapping.get(left + 1);
        PublicationTransaction transaction;
        transaction.merge_pair = MergePairPublication{
            .left            = left,
            .expected_left   = a,
            .expected_right  = b,
            .expected_parent = view->mapping.get(10),
            .candidate       = MergePairProof{.left          = left,
                                              .right         = left + 1,
                                              .parent        = 10,
                                              .left_content  = a->leaf->content_id,
                                              .right_content = b->leaf->content_id,
                                              .left_route    = a->leaf->route_generation,
                                              .right_route   = b->leaf->route_generation,
                                              .ready         = ready}
        };
        return transaction;
    }

    void reconcile(const std::shared_ptr<const PublishedTreeState> &view)
    {
        TreeStateReconciliation audit(view);
        for (size_t steps = 0; steps < 1024 && !audit.complete(); ++steps) {
            ASSERT_TRUE(audit.step(1).ok());
        }
        EXPECT_TRUE(audit.complete());
    }
};

TEST_F(PublicationMergePair, AdjacentPairsPublishUniqueMembershipAndPreserveOldReaders)
{
    const auto old     = publisher.capture();
    const auto pending = old->stats.checkpoint_pending_pages;
    ASSERT_TRUE(publish(classification(1)).status.ok());
    ASSERT_TRUE(publish(classification(2, false)).status.ok());
    const auto view = publisher.capture();
    EXPECT_EQ(view->stats.merge_candidate_pages, 3U);
    EXPECT_EQ(view->stats.merge_candidate_pairs, 2U);
    EXPECT_EQ(view->stats.merge_ready_pairs, 1U);
    EXPECT_EQ(view->stats.checkpoint_pending_pages, pending);
    EXPECT_EQ(view->mapping.copy_bounds().merge_candidate_pages, 3U);
    EXPECT_EQ(old->stats.merge_candidate_pages, 0U);
    for (uint64_t pid = 1; pid <= 3; ++pid) {
        EXPECT_TRUE(view->mapping.get(pid)->metrics.merge_candidate);
        EXPECT_FALSE(old->mapping.get(pid)->metrics.merge_candidate);
        EXPECT_EQ(view->mapping.get(pid)->merge_outgoing.has_value(), pid < 3);
        EXPECT_FALSE(old->mapping.get(pid)->merge_outgoing);
    }
    reconcile(old);
    reconcile(view);
}

TEST_F(PublicationMergePair, ReadinessAndRepresentationChangesPreserveCandidateAge)
{
    ASSERT_TRUE(publish(classification(1, false)).status.ok());
    const auto before = publisher.capture();
    ASSERT_GT(before->merge_candidate_since_ns, 0U);
    auto page = std::make_shared<PublishedPage>(*before->mapping.get(1));
    ++page->revision;
    page->metrics.merge_candidate = false; // Recomputed logical summary omits pair membership.
    ASSERT_TRUE(publish({.pages = {{1, before->mapping.get(1), page}}}).status.ok());
    EXPECT_TRUE(publisher.capture()->mapping.get(1)->metrics.merge_candidate);
    ASSERT_TRUE(publish(classification(1, true)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->merge_candidate_since_ns, before->merge_candidate_since_ns);
    EXPECT_EQ(after->stats.merge_ready_pairs, 1U);
    const auto proof = after->mapping.get(1)->merge_outgoing;
    if (!proof) {
        FAIL() << "published outgoing proof missing";
    }
    EXPECT_TRUE(proof->ready);
    reconcile(after);
}

TEST_F(PublicationMergePair, ChangedMiddlePageInvalidatesBothPairsAtomically)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    ASSERT_TRUE(publish(classification(2)).status.ok());
    const auto before  = publisher.capture();
    auto       page    = std::make_shared<PublishedPage>(*before->mapping.get(2));
    auto       logical = std::make_shared<PageState>(*page->leaf);
    ++logical->content_id;
    page->leaf = std::move(logical);
    ASSERT_TRUE(publish({.pages = {{2, before->mapping.get(2), page}}}).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.merge_candidate_pairs, 0U);
    EXPECT_EQ(after->stats.merge_candidate_pages, 0U);
    EXPECT_EQ(after->merge_candidate_since_ns, 0U);
    EXPECT_EQ(before->stats.merge_candidate_pages, 3U);
    EXPECT_FALSE(after->mapping.get(1)->merge_outgoing);
    EXPECT_FALSE(after->mapping.get(2)->merge_outgoing);
    reconcile(after);
}

TEST_F(PublicationMergePair, SplitPriorityInvalidatesPairWithoutChangingLogicalVersion)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    const auto before             = publisher.capture();
    auto       page               = std::make_shared<PublishedPage>(*before->mapping.get(2));
    page->metrics.split_candidate = true;
    ASSERT_TRUE(publish({.pages = {{2, before->mapping.get(2), page}}}).status.ok());
    EXPECT_EQ(publisher.capture()->stats.merge_candidate_pairs, 0U);
    EXPECT_EQ(publish(classification(1)).status.code(), Code::kUnavailable);
    reconcile(publisher.capture());
}

TEST_F(PublicationMergePair, SiblingInvalidationDoesNotRejectAlreadyPreparedPage)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    const auto before        = publisher.capture();
    auto       left          = std::make_shared<PublishedPage>(*before->mapping.get(1));
    auto       right         = std::make_shared<PublishedPage>(*before->mapping.get(2));
    auto       left_logical  = std::make_shared<PageState>(*left->leaf);
    auto       right_logical = std::make_shared<PageState>(*right->leaf);
    ++left_logical->content_id;
    ++right_logical->content_id;
    left->leaf  = std::move(left_logical);
    right->leaf = std::move(right_logical);
    ASSERT_TRUE(publish({.pages = {{1, before->mapping.get(1), left}}}).status.ok());
    ASSERT_NE(publisher.capture()->mapping.get(2), before->mapping.get(2));
    const auto result = publish({.pages = {{.page_id = 2, .expected = before->mapping.get(2), .replacement = right}}});
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    const auto after = publisher.capture();
    EXPECT_EQ(after->mapping.get(2)->leaf, right->leaf);
    EXPECT_EQ(after->stats.merge_candidate_pages, 0U);
    EXPECT_EQ(after->stats.merge_candidate_pairs, 0U);
    reconcile(before);
    reconcile(after);
}

TEST_F(PublicationMergePair, ClassificationDoesNotRejectPreparedPageButRepresentationChangeDoes)
{
    const auto before = publisher.capture();
    auto       page   = std::make_shared<PublishedPage>(*before->mapping.get(2));
    ++page->revision;
    ASSERT_TRUE(publish(classification(1)).status.ok());
    const auto result = publish({.pages = {{.page_id = 2, .expected = before->mapping.get(2), .replacement = page}}});
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.merge_candidate_pages, 2U);
    EXPECT_EQ(after->stats.merge_candidate_pairs, 1U);
    EXPECT_TRUE(after->mapping.get(2)->metrics.merge_candidate);
    EXPECT_EQ(publish({.pages = {{2, before->mapping.get(2), page}}}).status.code(), Code::kUnavailable);
    reconcile(after);
}

TEST_F(PublicationMergePair, WindowedSiblingFlushKeepsCoverageAndStatisticsConsistent)
{
    auto table = std::make_shared<MemTable>();
    for (const auto *key : {"a", "b", "n", "o"}) {
        ASSERT_TRUE(table->upsert(key, 2, encode_cell_buf(2, OpKind::kPut, key)));
    }
    table->close();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(std::move(table));
    ASSERT_TRUE(publish({.expected_sources = publisher.capture()->sources, .sources = sources}).status.ok());
    ASSERT_TRUE(publish(classification(1)).status.ok());
    detail::FlushRuntime runtime(2, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    FlushBatchOptions    options;
    options.install_run_window     = 1;
    options.run_limits.max_entries = 1;
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 20, 2, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
    const auto after = publisher.capture();
    EXPECT_EQ(after->coverage, 2U);
    EXPECT_EQ(after->stats.overlay_entries, 4U);
    EXPECT_EQ(after->stats.merge_candidate_pairs, 0U);
    EXPECT_EQ(after->stats.merge_candidate_pages, 0U);
    EXPECT_TRUE(after->sources->tables.empty());
    for (const auto *key : {"a", "b", "n", "o"}) {
        const auto pid   = key[0] < 'm' ? 1 : 2;
        const auto value = after->mapping.get(pid)->leaf->lookup(key);
        ASSERT_EQ(value.state, LogicalLookupState::kValue);
        EXPECT_EQ(value.cell.value.to_string(), key);
    }
    reconcile(after);
}

TEST_F(PublicationMergePair, ParentRouteChangeInvalidatesOnlyItsDirectPairProofs)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    ASSERT_TRUE(publish(classification(2)).status.ok());
    const auto before    = publisher.capture();
    auto       page      = std::make_shared<PublishedPage>(*before->mapping.get(10));
    auto       inner     = std::make_shared<InnerPageState>(*page->inner);
    inner->separators[0] = OwnedKey("n");
    page->inner          = std::move(inner);
    ASSERT_TRUE(publish({.pages = {{10, before->mapping.get(10), page}}}).status.ok());
    EXPECT_EQ(publisher.capture()->stats.merge_candidate_pages, 0U);
    reconcile(publisher.capture());
}

TEST_F(PublicationMergePair, BoundedMaintenanceClassifiesAdjacentPairsBeforeStructuralWork)
{
    detail::ResourceBudget::Limits limits{};
    limits.fill(16 * 1024 * 1024);
    detail::ResourceBudget          budget(limits, 4);
    auto                            account = budget.make_account(limits);
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PageMaintenanceOperationOptions options;
    options.max_steps                             = 2;
    options.publication.pages.merge_trigger_bytes = 256;
    options.publication.pages.merge_target_bytes  = 512;
    options.acquire                               = [&](auto *out) {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, 4 * 1024 * 1024);
        if (!credit) {
            return Status::resource_exhausted();
        }
        *out = std::make_shared<detail::AllocationReservation>(std::move(*credit));
        return Status::Ok();
    };
    auto                 maintenance = make_page_maintenance(publisher, runtime, lane, options);
    std::promise<Status> done;
    auto                 terminal = done.get_future();
    maintenance(publisher.capture(), [&](Status status, uint64_t) { done.set_value(std::move(status)); });
    const auto status = terminal.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    runtime.drain();
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.leaf_pages, 3U);
    EXPECT_EQ(after->stats.merge_candidate_pages, 3U);
    EXPECT_EQ(after->stats.merge_candidate_pairs, 2U);
    EXPECT_EQ(after->stats.merge_ready_pairs, 2U);
    reconcile(after);
}

TEST_F(PublicationMergePair, RepeatedSingleStepMaintenanceAdvancesPastUnchangedClassifications)
{
    detail::ResourceBudget::Limits limits{};
    limits.fill(16 * 1024 * 1024);
    detail::ResourceBudget          budget(limits, 4);
    auto                            account = budget.make_account(limits);
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PageMaintenanceOperationOptions options;
    options.max_steps                             = 1;
    options.publication.pages.merge_trigger_bytes = 256;
    options.publication.pages.merge_target_bytes  = 512;
    options.acquire                               = [&](auto *out) {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, 4 * 1024 * 1024);
        if (!credit) {
            return Status::resource_exhausted();
        }
        *out = std::make_shared<detail::AllocationReservation>(std::move(*credit));
        return Status::Ok();
    };
    auto maintenance = make_page_maintenance(publisher, runtime, lane, options);
    for (size_t attempt = 0; attempt < 8 && publisher.capture()->stats.leaf_pages == 3; ++attempt) {
        std::promise<Status> done;
        auto                 terminal = done.get_future();
        maintenance(publisher.capture(), [&](Status status, uint64_t) { done.set_value(std::move(status)); });
        const auto status = terminal.get();
        ASSERT_TRUE(status.ok()) << status.to_string();
        runtime.drain();
    }
    const auto after = publisher.capture();
    EXPECT_LT(after->stats.leaf_pages, 3U);
    reconcile(after);
}

TEST_F(PublicationMergePair, IndependentBootstrapClearsOnlyItsNewGenerationMembership)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    const auto           before = publisher.capture();
    PublicationBootstrap bootstrap;
    bootstrap.initial = before;
    PublicationCoordinator other(8, 1, {}, std::pmr::get_default_resource(), {}, std::move(bootstrap));
    const auto             after = other.capture();
    EXPECT_EQ(after->stats.merge_candidate_pairs, 0U);
    EXPECT_EQ(after->stats.merge_candidate_pages, 0U);
    EXPECT_EQ(after->mapping.copy_bounds().merge_candidate_pages, 0U);
    EXPECT_EQ(before->stats.merge_candidate_pages, 2U);
    EXPECT_TRUE(before->mapping.get(1)->metrics.merge_candidate);
    EXPECT_TRUE(before->mapping.get(1)->merge_outgoing);
    EXPECT_FALSE(after->mapping.get(1)->merge_outgoing);
    reconcile(before);
    reconcile(after);
}

TEST_F(PublicationMergePair, ClearingOutgoingPairKeepsTheSharedIncomingEndpoint)
{
    ASSERT_TRUE(publish(classification(1)).status.ok());
    ASSERT_TRUE(publish(classification(2)).status.ok());
    auto clear = classification(2);
    if (!clear.merge_pair) {
        FAIL() << "classification missing";
    }
    clear.merge_pair->candidate.reset();
    ASSERT_TRUE(publish(std::move(clear)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.merge_candidate_pairs, 1U);
    EXPECT_EQ(after->stats.merge_candidate_pages, 2U);
    EXPECT_TRUE(after->mapping.get(2)->metrics.merge_candidate);
    EXPECT_FALSE(after->mapping.get(3)->metrics.merge_candidate);
    EXPECT_FALSE(after->mapping.get(2)->merge_outgoing);
    reconcile(after);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST_F(PublicationMergePair, FailureAfterMembershipPreparationLeavesBothLedgersAndRootUnchanged)
{
    ASSERT_TRUE(publish(classification(1, false)).status.ok());
    const auto before = publisher.capture();
    publisher.set_before_root_for_tests([] { throw std::bad_alloc(); });
    EXPECT_EQ(publish(classification(2)).status.code(), Code::kResourceExhausted);
    publisher.set_before_root_for_tests({});
    EXPECT_EQ(publisher.capture(), before);
    ASSERT_TRUE(publish(classification(1, true)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.merge_candidate_pages, 2U);
    EXPECT_EQ(after->stats.merge_candidate_pairs, 1U);
    EXPECT_EQ(after->stats.merge_ready_pairs, 1U);
    EXPECT_EQ(after->merge_candidate_since_ns, before->merge_candidate_since_ns);
    reconcile(after);
}
#endif
} // namespace
} // namespace crowdb::tree
