// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_preparation_test_support.h"

#include <future>

namespace crowdb::tree
{
namespace
{

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<void> *terminal;

    void set_value() const noexcept
    {
        terminal->set_value();
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
        terminal->set_value();
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "flush must return its progress result";
        terminal->set_value();
    }
};

TEST(FlushReservation, AdmittedUnappliedWriterAndMaintenanceFinishWithAllUnusedPartitionsExhausted)
{
    for (const size_t workers : {1U, 4U}) {
        TestPreparedFlushTree   tree(workers);
        SourceAdmission::Writer writer;
        ASSERT_TRUE(tree.sources.try_admit(0, {16, 32, 0, 2}, &writer).ok());
        ASSERT_TRUE(writer.upsert("a0", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
        tree.frontier.store(1);
        std::shared_ptr<FlushOperation> operation;
        const auto                      admitted = tree.controller->prepare(tree.preparation, &operation);
        ASSERT_TRUE(admitted.ok()) << admitted.to_string();
        auto blockers = tree.exhaust_unused();
        // The reservation predates these writes. Only after they have applied
        // may the host advance the completed frontier captured by flush.
        for (uint64_t i = 2; i <= 16; ++i) {
            const auto key = std::string(i % 2 == 0 ? "a" : "z") + static_cast<char>('A' + i);
            ASSERT_TRUE(writer.upsert(key, i, encode_cell_buf(i, OpKind::kPut, "v")).ok());
        }
        tree.frontier.store(16);
        std::promise<void> terminal;
        auto               done      = terminal.get_future();
        auto               connected = stdexec::connect(operation->sender(), Receiver{&terminal});
        stdexec::start(connected);
        tree.runtime->drain();
        EXPECT_TRUE(writer.table()->closed());
        EXPECT_EQ(tree.publisher.capture()->coverage, 0U);
        writer.release();
        done.get();
        tree.runtime->drain();
        EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
        EXPECT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.to_string();
        EXPECT_EQ(operation->result().published_coverage, 16U);
        EXPECT_EQ(tree.publisher.capture()->flush_admission_owner, 0U);
        const auto snapshot           = tree.budget.snapshot();
        const auto lifecycle_reserved = tree.lifecycle_reservations();
        for (size_t category = 0; category < blockers.size(); ++category) {
            const auto &blocker = blockers[category];
            if (!blocker.has_value()) {
                ADD_FAILURE() << "test unused credit blocker missing";
                return;
            }
            EXPECT_EQ(snapshot.reserved[category], blocker->amount() + lifecycle_reserved[category]);
            EXPECT_LE(snapshot.reserved[category] + snapshot.allocated[category],
                      TestPreparedFlushTree::limits()[category]);
        }
    }
}

TEST(FlushReservation, ColdRoutingReserveFollowsCapturedBaseCount)
{
    TestPreparedFlushTree tree;
    auto                  view    = *tree.publisher.capture();
    const auto            sources = tree.sources.catalog();
    const auto            bound   = [&](size_t source_limit) {
        auto preparation                                      = tree.preparation;
        preparation.maintenance.publication.pages.max_sources = source_limit;
        detail::FlushReserveBounds result;
        const auto                 status =
            detail::flush_reserve_bounds(tree.publisher, view, *sources, preparation, 256 * 1024, tree.batch, &result);
        EXPECT_TRUE(status.ok()) << status.to_string();
        return result.amounts[1];
    };
    const auto empty = bound(1);
    EXPECT_EQ(bound(64), empty);
    EXPECT_LT(empty, TestPreparedFlushTree::limits()[static_cast<size_t>(detail::BudgetClass::kRouting)]);

    auto page = std::make_shared<PublishedPage>(*view.mapping.get(0));
    auto leaf = std::make_shared<PageState>(*page->leaf);
    for (uint64_t pid = 0; pid < 4; ++pid) {
        leaf->bases.push_back(test_read::cold(pid));
    }
    page->leaf = std::move(leaf);
    const std::array replacements{
        PageReplacement{.page_id = 0, .expected = view.mapping.get(0), .replacement = std::move(page)}
    };
    MappingView::CopyStats copied;
    view.mapping = view.mapping.replace(replacements, copied);
    EXPECT_GT(bound(4), empty);
    EXPECT_EQ(bound(64), bound(4));
    // Primary maintenance retains overflow references without reading chains.
    const auto primary_only                   = bound(64);
    tree.preparation.loads.max_overflow_pages = SIZE_MAX;
    EXPECT_EQ(bound(64), primary_only);
    EXPECT_FALSE(AsyncStorePageLoader::read_reserve_bound(tree.preparation.loads, 1, 1, 4, 0, 0));
}

TEST(FlushReservation, GrantedCompletePartitionsBindWithoutReacquiringUnderHostPressure)
{
    TestPreparedFlushTree   tree(1, true);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    writer.release();
    tree.frontier.store(1);
    const auto                 view    = tree.publisher.capture();
    const auto                 sources = tree.sources.catalog();
    detail::FlushReserveBounds bounds;
    // This caller supplies a finite control arena for the operation factory.
    constexpr size_t controls = 256 * 1024;
    const auto       before   = tree.budget.snapshot();
    ASSERT_TRUE(
        detail::flush_reserve_bounds(tree.publisher, *view, *sources, tree.preparation, controls, tree.batch, &bounds)
            .ok());
    EXPECT_EQ(tree.budget.snapshot().reserved, before.reserved);
    EXPECT_EQ(tree.budget.snapshot().allocated, before.allocated);
    detail::FlushCredits credits;
    for (size_t index = 0; index < credits.size(); ++index) {
        detail::ResourceBudget::Request request(tree.account, detail::FlushReserveBounds::categories[index],
                                                bounds.amounts[index]);
        ASSERT_TRUE(stdexec::sync_wait(request.sender()));
        auto paid = request.take_credit();
        if (!paid) {
            FAIL() << "missing paid test prerequisite";
        }
        credits[index] = std::move(*paid);
    }
    auto                      blockers = tree.exhaust_unused();
    auto                      batch    = tree.batch;
    FlushBatch::Maintenance   maintenance;
    detail::FlushReservations reserved;
    ASSERT_TRUE(detail::bind_flush_reservations(tree.publisher, tree.preparation, tree.account, tree.lane, bounds,
                                                std::move(credits), &batch, &maintenance, &reserved)
                    .ok());
    FlushController controller(tree.publisher, *tree.runtime, tree.sources, tree.frontier, tree.lane, std::move(batch),
                               std::move(maintenance));
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(reserved.handoff, &operation, view->commit_seq).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->report_sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
    EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
    EXPECT_GT(tree.store->reads.load(), 0U);
    reserved.seal();
}

TEST(FlushReservation, BindingRejectsWrongPartitionBeforeConstructingConsumers)
{
    TestPreparedFlushTree      tree;
    const auto                 view    = tree.publisher.capture();
    const auto                 sources = tree.sources.catalog();
    detail::FlushReserveBounds bounds;
    ASSERT_TRUE(
        detail::flush_reserve_bounds(tree.publisher, *view, *sources, tree.preparation, 256 * 1024, tree.batch, &bounds)
            .ok());
    detail::FlushCredits credits;
    for (size_t index = 0; index < credits.size(); ++index) {
        auto paid = detail::ResourceBudget::try_reserve(
            tree.account, index == 2 ? detail::BudgetClass::kRouting : detail::FlushReserveBounds::categories[index],
            bounds.amounts[index]);
        if (!paid) {
            FAIL() << "missing paid test prerequisite";
        }
        credits[index] = std::move(*paid);
    }
    const auto                before = tree.budget.snapshot();
    auto                      batch  = tree.batch;
    FlushBatch::Maintenance   maintenance;
    detail::FlushReservations reserved;
    EXPECT_EQ(detail::bind_flush_reservations(tree.publisher, tree.preparation, tree.account, tree.lane, bounds,
                                              std::move(credits), &batch, &maintenance, &reserved)
                  .code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(batch.loader);
    EXPECT_FALSE(maintenance);
    EXPECT_FALSE(sources->active->closed());
    EXPECT_EQ(tree.budget.snapshot().allocated, before.allocated);
    EXPECT_EQ(tree.budget.snapshot().reserved, tree.lifecycle_reservations());
}

detail::FlushCredits grant_plan(TestPreparedFlushTree &tree, const FlushReservePlan &plan)
{
    detail::FlushCredits credits;
    for (size_t index = 0; index < credits.size(); ++index) {
        detail::ResourceBudget::Request request(tree.account, detail::FlushReserveBounds::categories[index],
                                                plan.bounds.amounts[index]);
        const auto                      completed = stdexec::sync_wait(request.sender());
        if (!completed) {
            throw std::runtime_error("test flush grant stopped");
        }
        auto paid = request.take_credit();
        if (!paid) {
            throw std::runtime_error("test flush grant is missing");
        }
        credits[index] = std::move(*paid);
    }
    return credits;
}

TEST(FlushReservation, ControllerConsumesPlannedGrantsAndReusesTheExactPendingOperation)
{
    TestPreparedFlushTree   tree(1, true);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    writer.release();
    tree.frontier.store(1);
    const auto                      before = tree.budget.snapshot();
    FlushReservePlan                plan;
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare_or_plan(tree.preparation, &operation, &plan).ok());
    EXPECT_FALSE(operation);
    ASSERT_TRUE(plan.view);
    EXPECT_EQ(tree.budget.snapshot().reserved, before.reserved);
    auto credits  = grant_plan(tree, plan);
    auto blockers = tree.exhaust_unused();
    ASSERT_TRUE(tree.controller->prepare_prepaid(tree.preparation, plan, std::move(credits), &operation).ok());
    FlushReservePlan                unused;
    std::shared_ptr<FlushOperation> same;
    ASSERT_TRUE(tree.controller->prepare_or_plan(tree.preparation, &same, &unused).ok());
    EXPECT_EQ(same, operation);
    EXPECT_FALSE(unused.view);
    auto completed = stdexec::sync_wait(operation->report_sender());
    if (!completed) {
        FAIL() << "missing completed test prerequisite";
    }
    const auto &report = std::get<0>(*completed);
    EXPECT_TRUE(report.handoff_status.ok()) << report.handoff_status.to_string();
    EXPECT_EQ(report.published_coverage, 1U);
    tree.runtime->drain();
}

TEST(FlushReservation, StalePlanReturnsAllGrantsBeforeSourceClosure)
{
    TestPreparedFlushTree tree;
    tree.frontier.store(1);
    FlushReservePlan                plan;
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare_or_plan(tree.preparation, &operation, &plan).ok());
    auto credits                = grant_plan(tree, plan);
    auto request                = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(InstallToken{});
    request->on_done            = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(tree.publisher.try_submit(request));
    tree.runtime->drain();
    EXPECT_EQ(tree.controller->prepare_prepaid(tree.preparation, plan, std::move(credits), &operation).code(),
              Code::kUnavailable);
    EXPECT_FALSE(operation);
    EXPECT_FALSE(plan.sources->active->closed());
    EXPECT_EQ(tree.budget.snapshot().reserved, tree.lifecycle_reservations());
}

TEST(FlushReservation, UncontendedPlanningIsNotAnApplyProgressTransition)
{
    TestPreparedFlushTree tree;
    tree.frontier.store(1);
    auto paid = detail::ResourceBudget::try_reserve(tree.account, detail::BudgetClass::kHandoff, 16384);
    if (!paid) {
        FAIL() << "missing paid test prerequisite";
    }
    auto                           metadata = detail::AllocationReservation::create(std::move(*paid));
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(2, metadata, &progress).ok());
    FlushController controller(tree.publisher, *tree.runtime, tree.sources, tree.frontier, tree.lane, tree.batch, {},
                               {}, progress);
    const auto      sequence = progress->sequence();
    for (size_t attempt = 0; attempt < 4; ++attempt) {
        FlushReservePlan                plan;
        std::shared_ptr<FlushOperation> existing;
        ASSERT_TRUE(controller.prepare_or_plan(tree.preparation, &existing, &plan).ok());
        EXPECT_FALSE(existing);
        EXPECT_EQ(progress->sequence(), sequence);
    }
}

TEST(FlushReservation, EachMissingProgressPartitionRollsBackBeforeSourceClosure)
{
    for (const auto category : detail::FlushReserveBounds::categories) {
        TestPreparedFlushTree tree;
        tree.frontier.store(1);
        const auto before  = tree.publisher.capture();
        const auto active  = tree.sources.catalog()->active;
        auto       blocker = detail::ResourceBudget::try_reserve(
            tree.account, category, TestPreparedFlushTree::limits()[static_cast<size_t>(category)]);
        ASSERT_TRUE(blocker);
        const auto                      budget_before = tree.budget.snapshot();
        std::shared_ptr<FlushOperation> operation;
        EXPECT_EQ(tree.controller->prepare(tree.preparation, &operation).code(), Code::kUnavailable);
        EXPECT_FALSE(operation);
        EXPECT_FALSE(active->closed());
        EXPECT_EQ(tree.publisher.capture(), before);
        EXPECT_EQ(tree.budget.snapshot().reserved, budget_before.reserved);
        EXPECT_EQ(tree.budget.snapshot().allocated, budget_before.allocated);
    }
}

TEST(FlushReservation, ColdRoutingUsesHeldBytesAndIoUnderCompleteHostPressure)
{
    TestPreparedFlushTree tree(1, true);
    tree.preparation.cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 2, .max_waiters = 1});
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {2, 2, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    ASSERT_TRUE(writer.upsert("z", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
    writer.release();
    tree.frontier.store(2);
    std::shared_ptr<FlushOperation> operation;
    auto                            status = tree.controller->prepare(tree.preparation, &operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    auto blockers = tree.exhaust_unused();
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
    EXPECT_EQ(operation->result().published_coverage, 2U);
    EXPECT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.to_string();
    EXPECT_GT(operation->result().deferred_pages, 0U);
    // One root load serves both routing and the parent join.
    EXPECT_EQ(tree.store->reads.load(), 1U);
    EXPECT_EQ(tree.budget.snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kIo)], 0U);
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    EXPECT_EQ(tree.preparation.cache->snapshot().misses, 1U);
}

TEST(FlushReservation, DescendingColdRoutePidsUseHeldCreditForEveryLoadedLevel)
{
    TestPreparedFlushTree tree(1, true);
    tree.preparation.cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 2, .max_waiters = 1});
    const auto before      = tree.publisher.capture();
    auto       root        = std::make_shared<PublishedPage>();
    root->level            = 2;
    root->metrics.leaf     = false;
    root->inner_backing    = test_read::inner_backing(tree.store, 1, *test_read::inner({}, {2}));
    auto request           = std::make_shared<PublicationRequest>();
    request->transaction   = {.record       = std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 2}),
                              .pages        = {{.page_id = 3, .expected = {}, .replacement = std::move(root)}},
                              .root_page_id = 3,
                              .expected_root_page_id = before->root_page_id};
    std::promise<Status> published;
    auto                 installed = published.get_future();
    request->on_done               = [&](PublicationResult result) { published.set_value(std::move(result.status)); };
    ASSERT_TRUE(tree.publisher.try_submit(request));
    ASSERT_TRUE(installed.get().ok());
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {2, 2, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    ASSERT_TRUE(writer.upsert("z", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
    writer.release();
    tree.frontier.store(2);
    std::shared_ptr<FlushOperation> operation;
    const auto                      status = tree.controller->prepare(tree.preparation, &operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto blockers = tree.exhaust_unused();
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
    EXPECT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.to_string();
    EXPECT_EQ(operation->result().published_coverage, 2U);
    // Each descending routing page is loaded once for the complete flow.
    EXPECT_EQ(tree.store->reads.load(), 2U);
    EXPECT_EQ(tree.preparation.cache->snapshot().misses, 2U);
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    EXPECT_EQ(tree.budget.snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kIo)], 0U);
}

TEST(FlushReservation, LiveResidencyAndColdProxyPublicationUseHeldCreditUnderCompleteHostPressure)
{
    for (const size_t workers : {1U, 4U}) {
        TestPreparedFlushTree tree(workers, true, true);
        EXPECT_EQ(tree.resources->snapshot().proxy_pages, 3U);
        SourceAdmission::Writer writer;
        ASSERT_TRUE(tree.sources.try_admit(0, {2, 2, 0}, &writer).ok());
        ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
        ASSERT_TRUE(writer.upsert("z", 2, encode_cell_buf(2, OpKind::kPut, "v")).ok());
        writer.release();
        tree.frontier.store(2);
        std::shared_ptr<FlushOperation> operation;
        const auto                      status = tree.controller->prepare(tree.preparation, &operation);
        ASSERT_TRUE(status.ok()) << status.to_string();
        auto blockers = tree.exhaust_unused();
        ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
        tree.runtime->drain();
        EXPECT_TRUE(operation->result().handoff_status.ok()) << operation->result().handoff_status.to_string();
        EXPECT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.to_string();
        EXPECT_EQ(operation->result().published_coverage, 2U);
        // Both leaves finish using temporary raw loads and retain their cold backing.
        EXPECT_EQ(tree.store->reads.load(), 3U);
        EXPECT_EQ(tree.cache->snapshot().misses, 1U);
        const auto view = tree.publisher.capture();
        EXPECT_FALSE(view->mapping.get(0)->leaf->bases.front().resident);
        EXPECT_FALSE(view->mapping.get(1)->leaf->bases.front().resident);
        EXPECT_TRUE(view->mapping.get(0)->metrics.kv_known);
        EXPECT_TRUE(view->mapping.get(1)->metrics.kv_known);
        EXPECT_FALSE(view->mapping.get(2)->inner);
        EXPECT_EQ(tree.resources->snapshot().proxy_pages, 2U);
        EXPECT_EQ(tree.resources->snapshot().resident_frames, 1U);
        EXPECT_EQ(tree.resources->snapshot().cache_pinned_bytes, 0U);
        EXPECT_EQ(tree.cache->snapshot().reserved_waiters, 0U);
        EXPECT_EQ(tree.budget.snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kIo)], 0U);
        tree.cache->evict();
        tree.runtime->drain();
        EXPECT_EQ(tree.resources->snapshot().proxy_pages, 3U);
        EXPECT_EQ(tree.resources->snapshot().resident_frames, 0U);
        EXPECT_EQ(tree.resources->snapshot().cache_bytes, 0U);
    }
}

TEST(FlushReservation, StaleMappingRejectsAndReleasesAllUnspentReservations)
{
    TestPreparedFlushTree tree;
    tree.frontier.store(1);
    const auto                      active = tree.sources.catalog()->active;
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &operation).ok());
    auto request                = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(InstallToken{});
    request->on_done            = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(tree.publisher.try_submit(request));
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_EQ(operation->result().handoff_status.code(), Code::kUnavailable);
    EXPECT_FALSE(active->closed());
    EXPECT_EQ(tree.budget.snapshot().reserved, tree.lifecycle_reservations());
}

TEST(FlushReservation, BoundsRejectOverflowAndNeverWrapIntoSmallAdmission)
{
    EXPECT_FALSE(detail::AllocationBound(SIZE_MAX).add(1).value());
    EXPECT_FALSE(detail::AllocationBound().add_product(SIZE_MAX, 2).add_product(0, 1).value());
    TestPreparedFlushTree tree;
    const auto            view = tree.publisher.capture();
    EXPECT_FALSE(FlushBatch::metadata_reserve_bound(view->mapping, 1, SIZE_MAX, SIZE_MAX, 1, tree.batch));
    EXPECT_FALSE(SourceAdmission::capture_allocation_bound(SIZE_MAX));
    auto loads           = tree.preparation.loads;
    loads.max_blob_bytes = SIZE_MAX;
    EXPECT_FALSE(AsyncStorePageLoader::routing_reserve_bound(loads, 1, 1));
    auto maintenance           = tree.preparation.maintenance;
    maintenance.max_candidates = SIZE_MAX;
    EXPECT_FALSE(page_maintenance_reserve_bound(view->mapping, 0, maintenance));
}

TEST(FlushReservation, ClosedSourcesUseActualKeyBoundsOnlyAfterAcceptedWritersDrain)
{
    TestPreparedFlushTree   tree;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {2, 2, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "first")).ok());
    const auto                 sources = tree.sources.catalog();
    const auto                 view    = tree.publisher.capture();
    detail::FlushReserveBounds open;
    ASSERT_TRUE(
        detail::flush_reserve_bounds(tree.publisher, *view, *sources, tree.preparation, 0, tree.batch, &open).ok());
    writer.table()->close();
    detail::FlushReserveBounds draining;
    ASSERT_TRUE(
        detail::flush_reserve_bounds(tree.publisher, *view, *sources, tree.preparation, 0, tree.batch, &draining).ok());
    EXPECT_EQ(draining.amounts, open.amounts);
    ASSERT_TRUE(writer.upsert("z", 2, encode_cell_buf(2, OpKind::kPut, "last")).ok());
    writer.release();
    detail::FlushReserveBounds frozen;
    ASSERT_TRUE(
        detail::flush_reserve_bounds(tree.publisher, *view, *sources, tree.preparation, 0, tree.batch, &frozen).ok());
    EXPECT_LT(frozen.amounts[0], open.amounts[0]);
    EXPECT_EQ(sources->tables.front()->source_stats().records, 2U);
}

TEST(FlushReservation, CoveredPreparationFixesItsPrefixWithoutBlockingLaterWrites)
{
    for (const bool planned : {false, true}) {
        TestPreparedFlushTree                                        tree;
        std::array<std::optional<detail::ResourceBudget::Credit>, 3> blockers;
        constexpr std::array categories{detail::BudgetClass::kRouting, detail::BudgetClass::kReclaim,
                                        detail::BudgetClass::kIo};
        for (size_t i = 0; i < categories.size(); ++i) {
            blockers[i] = detail::ResourceBudget::try_reserve(
                tree.account, categories[i], TestPreparedFlushTree::limits()[static_cast<size_t>(categories[i])]);
            ASSERT_TRUE(blockers[i]);
        }
        const auto                      catalog = tree.sources.catalog();
        const auto                      view    = tree.publisher.capture();
        std::shared_ptr<FlushOperation> operation;
        FlushReservePlan                plan;
        const auto prepared = planned ? tree.controller->prepare_or_plan(tree.preparation, &operation, &plan)
                                      : tree.controller->prepare(tree.preparation, &operation);
        ASSERT_TRUE(prepared.ok()) << prepared.to_string();
        ASSERT_TRUE(operation);
        EXPECT_FALSE(plan.view);
        SourceAdmission::Writer writer;
        ASSERT_TRUE(tree.sources.try_admit(0, {1, 1, 0}, &writer).ok());
        ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "new")).ok());
        writer.release();
        tree.frontier.store(1, std::memory_order_release);
        auto completed = stdexec::sync_wait(operation->report_sender());
        if (!completed) {
            FAIL() << "covered operation did not complete";
        }
        const auto &report = std::get<0>(*completed);
        EXPECT_TRUE(report.handoff_status.ok());
        EXPECT_TRUE(report.maintenance_status.ok());
        EXPECT_EQ(report.floor, 0U);
        EXPECT_EQ(report.frontier, 0U);
        EXPECT_EQ(report.published_coverage, 0U);
        EXPECT_FALSE(report.handoff_advanced);
        EXPECT_EQ(tree.sources.catalog(), catalog);
        EXPECT_FALSE(catalog->active->closed());
        EXPECT_EQ(tree.publisher.capture(), view);
        EXPECT_EQ(tree.store->reads.load(), 0U);
        blockers = {};
        std::shared_ptr<FlushOperation> following;
        const auto                      next = tree.controller->prepare(tree.preparation, &following);
        ASSERT_TRUE(next.ok()) << next.to_string();
        ASSERT_NE(following, operation);
        ASSERT_TRUE(stdexec::sync_wait(following->sender()));
        tree.runtime->drain();
        EXPECT_TRUE(following->result().handoff_status.ok()) << following->result().handoff_status.to_string();
        EXPECT_EQ(following->result().frontier, 1U);
        EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
        const auto value = tree.publisher.capture()->mapping.get(0)->leaf->lookup("a");
        ASSERT_EQ(value.state, LogicalLookupState::kValue);
        EXPECT_EQ(value.cell.value.to_string(), "new");
    }
}

TEST(FlushReservation, NoProgressReleasesEveryPartitionWithoutRotatingSources)
{
    TestPreparedFlushTree tree;
    tree.preparation.cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 1, .max_waiters = 1});
    const auto                      catalog = tree.sources.catalog();
    const auto                      view    = tree.publisher.capture();
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &operation).ok());
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok());
    EXPECT_EQ(operation->result().published_coverage, 0U);
    EXPECT_EQ(tree.sources.catalog(), catalog);
    EXPECT_EQ(tree.publisher.capture(), view);
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    EXPECT_EQ(tree.budget.snapshot().reserved, tree.lifecycle_reservations());
}

TEST(FlushReservation, CancelledClosedBatchRetriesWithOriginalCreditsWhileHostIsFull)
{
    TestPreparedFlushTree tree;
    tree.preparation.cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 1, .max_waiters = 1});
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {1, 1, 0}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    tree.frontier.store(1);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &operation).ok());
    auto               blockers = tree.exhaust_unused();
    std::promise<void> terminal;
    auto               done      = terminal.get_future();
    auto               connected = stdexec::connect(operation->sender(), Receiver{&terminal});
    stdexec::start(connected);
    tree.runtime->drain();
    ASSERT_TRUE(writer.table()->closed());
    operation->request_stop();
    writer.release();
    done.get();
    tree.runtime->drain();
    EXPECT_EQ(operation->result().phase, FlushPhase::kRepairRequired);
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
    const auto identity = operation->result().batch_id;
    const auto before   = tree.budget.snapshot();
    const auto owner    = tree.publisher.capture()->flush_admission_owner;
    ASSERT_NE(owner, 0U);
    std::shared_ptr<FlushOperation> retry;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &retry).ok());
    EXPECT_EQ(retry, operation);
    EXPECT_EQ(tree.budget.snapshot().reserved, before.reserved);
    EXPECT_EQ(tree.publisher.capture()->flush_admission_owner, owner);
    tree.frontier.store(10);
    ASSERT_TRUE(stdexec::sync_wait(retry->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(retry->result().handoff_status.ok()) << retry->result().handoff_status.to_string();
    EXPECT_TRUE(retry->result().maintenance_status.ok()) << retry->result().maintenance_status.to_string();
    EXPECT_EQ(retry->result().batch_id, identity);
    EXPECT_EQ(retry->result().frontier, 1U);
    EXPECT_EQ(retry->result().published_coverage, 1U);
    EXPECT_EQ(tree.publisher.capture()->flush_admission_owner, 0U);
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
}

TEST(FlushReservation, OccupiedCacheWaiterDoesNotBlockFlush)
{
    TestPreparedFlushTree tree;
    tree.frontier.store(1);
    tree.preparation.cache = std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 1, .max_waiters = 1});
    auto                        upstream = std::make_shared<test_read::TestLoader>();
    std::shared_ptr<PageLoader> occupied;
    ASSERT_TRUE(tree.preparation.cache->bind_reserved(upstream, {}, &occupied).ok());
    const auto                      catalog = tree.sources.catalog();
    const auto                      view    = tree.publisher.capture();
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &operation).ok());
    EXPECT_FALSE(catalog->active->closed());
    EXPECT_EQ(tree.sources.catalog(), catalog);
    EXPECT_EQ(tree.publisher.capture(), view);
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()));
    tree.runtime->drain();
    EXPECT_TRUE(operation->result().handoff_status.ok());
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 1U);
    occupied.reset();
    EXPECT_EQ(tree.preparation.cache->snapshot().reserved_waiters, 0U);
}
} // namespace
} // namespace crowdb::tree
