// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_operation.h"
#include "btree/flush_reservation_internal.h"
#include "btree/maintenance_operation.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <future>

namespace crowdb::tree
{
namespace
{
struct MaintenanceCredits
{
    MaintenanceCredits()
    {
        account->set_resource_metrics(resources);
    }

    std::shared_ptr<TreeResourceMetrics> resources = std::make_shared<TreeResourceMetrics>();

    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(64 * 1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account  = budget.make_account(limits());
    size_t                                           acquired = 0;

    Status reserve(std::shared_ptr<detail::AllocationReservation> *out,
                   detail::BudgetClass                             category = detail::BudgetClass::kReclaim)
    {
        auto credit = detail::ResourceBudget::try_reserve(account, category, 4 * 1024 * 1024);
        if (!credit) {
            return Status::resource_exhausted();
        }
        *out = std::make_shared<detail::AllocationReservation>(std::move(*credit));
        ++acquired;
        return Status::Ok();
    }

    PageMaintenanceOperationOptions options()
    {
        PageMaintenanceOperationOptions result;
        result.publication.pages.primary_page_bytes  = 512;
        result.publication.pages.target_page_bytes   = 256;
        result.publication.pages.merge_trigger_bytes = 64;
        result.publication.pages.merge_target_bytes  = 128;
        result.publication.pages.max_work_entries    = 4;
        result.acquire                               = [this](auto *out) { return reserve(out); };
        return result;
    }
};

Status publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<Status> completed;
    auto                 terminal = completed.get_future();
    request->on_done              = [&](PublicationResult result) { completed.set_value(std::move(result.status)); };
    if (!publisher.try_submit(request)) {
        return Status::unavailable();
    }
    return terminal.get();
}

std::shared_ptr<InstallRecord> record()
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 0, .range = 0, .sequence = 0});
}

std::shared_ptr<PublishedPage> leaf(size_t count, size_t start = 0)
{
    OverlayBuilder builder({});
    for (size_t i = start; i < start + count; ++i) {
        auto cell = std::make_shared<CellVersion>(encode_cell_buf(1, OpKind::kPut, std::string(16, 'v')), 1, 0);
        EXPECT_TRUE(builder.append(std::to_string(1000 + i), std::move(cell)).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(1, &run).ok());
    auto logical = std::make_shared<PageState>();
    logical->overlays.push_back(std::move(run));
    auto page                     = std::make_shared<PublishedPage>();
    page->leaf                    = std::move(logical);
    page->metrics.dirty           = true;
    page->metrics.overlay_runs    = 1;
    page->metrics.overlay_entries = count;
    return page;
}

std::pair<Status, uint64_t> maintain(FlushBatch::Maintenance &callback, PublicationCoordinator &publisher)
{
    std::promise<std::pair<Status, uint64_t>> completed;
    auto                                      terminal = completed.get_future();
    callback(publisher.capture(),
             [&](Status status, uint64_t deferred) { completed.set_value({std::move(status), deferred}); });
    return terminal.get();
}

std::shared_ptr<const PublishedPage> route(const PublishedTreeState &view, const std::string &key)
{
    auto page = view.mapping.get(view.root_page_id);
    for (size_t depth = 0; depth < 64 && page && page->inner; ++depth) {
        auto child =
            static_cast<size_t>(std::upper_bound(page->inner->separators.begin(), page->inner->separators.end(), key) -
                                page->inner->separators.begin());
        page = view.mapping.get(page->inner->children[child]);
    }
    return page;
}

TEST(PageMaintenanceOperation, FreshFactoriesContinuePastThePreviousBoundedWindow)
{
    PublicationCoordinator publisher;
    auto                   left        = leaf(1);
    auto                   right       = leaf(1, 1);
    auto                   left_state  = std::make_shared<PageState>(*left->leaf);
    auto                   right_state = std::make_shared<PageState>(*right->leaf);
    left_state->range.upper            = OwnedKey("1001");
    left_state->right_sibling          = 1;
    right_state->range.lower           = OwnedKey("1001");
    left->leaf                         = left_state;
    right->leaf                        = right_state;
    auto inner                         = std::make_shared<InnerPageState>();
    inner->children                    = {0, 1};
    inner->separators.emplace_back("1001");
    auto root          = std::make_shared<PublishedPage>();
    root->level        = 1;
    root->inner        = inner;
    root->metrics.leaf = false;
    ASSERT_TRUE(publish(publisher,
                        {
                            .record                = record(),
                            .pages                 = {{.page_id = 0, .expected = {}, .replacement = left},
                                                      {.page_id = 1, .expected = {}, .replacement = right},
                                                      {.page_id = 2, .expected = {}, .replacement = root}},
                            .root_page_id          = 2,
                            .expected_next_page_id = 0,
                            .next_page_id          = 3
    })
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    for (uint64_t pid = 0; pid < 2; ++pid) {
        auto options       = credits.options();
        options.max_visits = options.max_candidates = options.max_steps = 1;
        auto       fresh  = make_page_maintenance(publisher, runtime, lane, std::move(options));
        const auto result = maintain(fresh, publisher);
        ASSERT_TRUE(result.first.ok()) << result.first.to_string();
        runtime.drain();
        EXPECT_TRUE(publisher.capture()->mapping.get(pid)->metrics.page_size_known) << pid;
    }
}

TEST(PageMaintenanceOperation, QueueObservationEndsAtDispatchAndTerminalReleasesAllWork)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record                = record(),
                                    .pages                 = {{.page_id = 0, .expected = {}, .replacement = leaf(1)}},
                                    .root_page_id          = 0,
                                    .expected_next_page_id = 0,
                                    .next_page_id          = 1})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 maintenance = make_page_maintenance(publisher, runtime, lane, credits.options());

    struct Blocker
    {
        std::promise<void> entered;
        std::promise<void> release;

        static void run(void *raw) noexcept
        {
            auto &self = *static_cast<Blocker *>(raw);
            self.entered.set_value();
            self.release.get_future().wait();
        }
    } blocker;

    ASSERT_TRUE(runtime.try_submit(lane, &Blocker::run, &blocker).ok());
    blocker.entered.get_future().wait();
    std::promise<Status> done;
    auto                 finished = done.get_future();
    maintenance(publisher.capture(), [&](Status status, uint64_t) { done.set_value(std::move(status)); });
    const auto queued = credits.resources->snapshot();
    EXPECT_EQ(queued.maintenance_queued, 1U);
    EXPECT_EQ(queued.maintenance_inflight, 0U);
    EXPECT_GT(publisher.oldest_maintenance_since_ns(), 0U);
    EXPECT_GT(publisher.stats(*credits.resources).progress.maintenance_oldest_age_ns, 0U);
    blocker.release.set_value();
    EXPECT_TRUE(finished.get().ok());
    runtime.drain();
    const auto ended = credits.resources->snapshot();
    EXPECT_EQ(ended.maintenance_queued, 0U);
    EXPECT_EQ(ended.maintenance_inflight, 0U);
    EXPECT_EQ(publisher.oldest_maintenance_since_ns(), 0U);
    EXPECT_EQ(publisher.stats(*credits.resources).progress.maintenance_oldest_age_ns, 0U);
}

TEST(PageMaintenanceOperation, SingleStepPublishesBulkRangesWithinAdvertisedCreditWithoutEncoding)
{
    PublicationCoordinator publisher;
    const auto             original = leaf(12);
    ASSERT_TRUE(publish(publisher, {.record                = record(),
                                    .pages                 = {{.page_id = 0, .expected = {}, .replacement = original}},
                                    .root_page_id          = 0,
                                    .expected_next_page_id = 0,
                                    .next_page_id          = 1})
                    .ok());
    const auto           before = publisher.capture();
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options                 = credits.options();
    options.max_steps                            = 1;
    options.publication.pages.max_work_entries   = 16;
    options.publication.pages.max_output_pages   = 4;
    options.publication.pages.max_metadata_bytes = 1024 * 1024;
    const auto bound                             = page_maintenance_reserve_bound(before->mapping, 0, options);
    if (!bound) {
        FAIL() << "maintenance reserve bound must be representable";
    }
    ASSERT_LE(*bound, MaintenanceCredits::limits()[static_cast<size_t>(detail::BudgetClass::kReclaim)]);
    options.acquire = [&](auto *out) {
        auto credit = detail::ResourceBudget::try_reserve(credits.account, detail::BudgetClass::kReclaim, *bound);
        if (!credit) {
            return Status::resource_exhausted();
        }
        *out = detail::AllocationReservation::create(std::move(*credit));
        return Status::Ok();
    };
    auto       maintenance = make_page_maintenance(publisher, runtime, lane, std::move(options));
    const auto result      = maintain(maintenance, publisher);
    ASSERT_TRUE(result.first.ok()) << result.first.to_string();
    runtime.drain();
    const auto after = publisher.capture();
    EXPECT_EQ(after->commit_seq, before->commit_seq + 1);
    EXPECT_EQ(after->stats.leaf_pages, 4U);
    EXPECT_EQ(after->stats.overlay_entries, 12U);
    for (size_t i = 0; i < 12; ++i) {
        const auto key  = std::to_string(1000 + i);
        const auto page = route(*after, key);
        ASSERT_TRUE(page && page->leaf);
        EXPECT_EQ(page->leaf->lookup(key).cell.owner, original->leaf->lookup(key).cell.owner);
    }
    const auto totals = publisher.work_metrics()->totals();
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::PageLayout)].count, 1U);
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::PageSplit)].count, 1U);
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::PageEncode)].count, 0U);
    EXPECT_EQ(before->mapping.get(0), original);
    EXPECT_EQ(before->stats.leaf_pages, 1U);
}

TEST(PageMaintenanceOperation, IdleAdmissionAppliesItsNewWindowBeforeBuildingTheSplit)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record                = record(),
                                    .pages                 = {{.page_id = 0, .expected = {}, .replacement = leaf(64)}},
                                    .root_page_id          = 0,
                                    .expected_next_page_id = 0,
                                    .next_page_id          = 1})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options                 = credits.options();
    options.max_steps                            = 1;
    options.publication.pages.max_work_entries   = 128;
    options.publication.pages.max_output_pages   = 7;
    options.publication.pages.max_metadata_bytes = 1024 * 1024;
    detail::ReclaimAdmission admission{.publisher = &publisher, .account = credits.account, .options = options};
    options.publication.pages.max_output_pages = 2;
    options.acquire_window                     = [&](const auto &mapping, auto *publication, auto *credit) {
        return admission.acquire_window(mapping, publication, credit);
    };
    auto       maintenance = make_page_maintenance(publisher, runtime, lane, std::move(options));
    const auto result      = maintain(maintenance, publisher);
    ASSERT_TRUE(result.first.ok()) << result.first.to_string();
    runtime.drain();
    EXPECT_EQ(publisher.capture()->stats.leaf_pages, 7U);
    EXPECT_EQ(publisher.capture()->stats.overlay_entries, 64U);
    EXPECT_EQ(credits.acquired, 0U);
}

TEST(PageMaintenanceOperation, FullTreeMaintenanceWindowDefersBeforeQueueingAndCanRetry)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record                = record(),
                                    .pages                 = {{.page_id = 0, .expected = {}, .replacement = leaf(1)}},
                                    .root_page_id          = 0,
                                    .expected_next_page_id = 0,
                                    .next_page_id          = 1})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 maintenance = make_page_maintenance(publisher, runtime, lane, credits.options());
    const auto           before      = publisher.capture();
    std::array<size_t, PublicationCoordinator::kMaintenanceSlots> slots{};
    for (auto &slot : slots) {
        slot = publisher.start_maintenance(tree_metrics_now_ns());
        ASSERT_LT(slot, slots.size());
    }
    bool deferred = false;
    maintenance(before, [&](Status status, uint64_t dirty) {
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(dirty, before->stats.dirty_pages);
        deferred = true;
    });
    EXPECT_TRUE(deferred);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_EQ(credits.resources->snapshot().maintenance_queued, 0U);
    for (const auto slot : slots) {
        publisher.finish_maintenance(slot);
    }
    std::promise<Status> done;
    auto                 result = done.get_future();
    maintenance(publisher.capture(), [&](Status status, uint64_t) { done.set_value(std::move(status)); });
    EXPECT_TRUE(result.get().ok());
    runtime.drain();
    EXPECT_EQ(publisher.oldest_maintenance_since_ns(), 0U);
}

TEST(PageMaintenanceOperation, SingleWorkerFlushSplitsClipsRunsAndPreservesOldView)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    auto                   initial = std::make_shared<PublishedPage>();
    initial->leaf                  = std::make_shared<PageState>();
    auto       catalog             = std::make_shared<PublishedSources>();
    const auto source_view         = sources.catalog();
    catalog->tables.assign(source_view->tables.begin(), source_view->tables.end());
    ASSERT_TRUE(publish(publisher, {.record           = record(),
                                    .pages            = {{.page_id = 0, .expected = {}, .replacement = initial}},
                                    .root_page_id     = 0,
                                    .expected_sources = publisher.capture()->sources,
                                    .sources          = catalog})
                    .ok());
    const auto            before = publisher.capture();
    detail::FlushRuntime  runtime(1, 8);
    auto                  lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits    credits;
    auto                  callback = make_page_maintenance(publisher, runtime, lane, credits.options());
    std::atomic<uint64_t> contiguous{1};
    FlushBatchOptions     flush_options;
    flush_options.run_limits.max_entries = 2;
    FlushController         controller(publisher, runtime, sources, contiguous, lane, flush_options, callback);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    for (size_t i = 0; i < 12; ++i) {
        ASSERT_TRUE(writer.table()->upsert(std::to_string(1000 + i), 1,
                                           encode_cell_buf(1, OpKind::kPut, std::string(16, 'v'))));
    }
    writer.release();
    std::shared_ptr<detail::AllocationReservation> metadata;
    ASSERT_TRUE(credits.reserve(&metadata, detail::BudgetClass::kHandoff).ok());
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(metadata, &operation).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    runtime.drain();
    ASSERT_TRUE(operation->result().handoff_status.ok());
    ASSERT_TRUE(operation->result().maintenance_status.ok()) << operation->result().maintenance_status.message();
    const auto after = publisher.capture();
    EXPECT_GT(after->stats.leaf_pages, 1U);
    EXPECT_EQ(operation->result().deferred_pages, after->stats.dirty_pages);
    EXPECT_GT(operation->result().deferred_pages, 0U);
    for (size_t i = 0; i < 12; ++i) {
        const auto key  = std::to_string(1000 + i);
        auto       page = route(*after, key);
        ASSERT_TRUE(page && page->leaf);
        EXPECT_EQ(page->leaf->lookup(key).cell.value.to_string(), std::string(16, 'v'));
        for (const auto &run : page->leaf->overlays) {
            for (const auto &entry : run->entries()) {
                EXPECT_TRUE(page->leaf->range.contains(entry.key()));
            }
        }
    }
    EXPECT_EQ(before->mapping.get(0), initial);
    EXPECT_EQ(before->mapping.get(0)->leaf->lookup("1000").state, LogicalLookupState::kMissing);
}

TEST(PageMaintenanceOperation, IdleMaintenanceMergesExactSiblingsAndKeepsOldRoot)
{
    PublicationCoordinator publisher;
    auto                   left         = leaf(1);
    auto                   right        = leaf(1, 1);
    auto                   left_logical = std::make_shared<PageState>(*left->leaf);
    left_logical->range.upper           = "1001";
    left_logical->right_sibling         = 1;
    left->leaf                          = left_logical;
    auto right_logical                  = std::make_shared<PageState>(*right->leaf);
    right_logical->range.lower          = "1001";
    right->leaf                         = right_logical;
    auto inner                          = std::make_shared<InnerPageState>();
    inner->children                     = {0, 1};
    inner->separators                   = {"1001"};
    auto parent                         = std::make_shared<PublishedPage>();
    parent->level                       = 1;
    parent->inner                       = inner;
    parent->metrics.leaf                = false;
    ASSERT_TRUE(publish(publisher,
                        {
                            .record       = record(),
                            .pages        = {{.page_id = 0, .expected = {}, .replacement = left},
                                             {.page_id = 1, .expected = {}, .replacement = right},
                                             {.page_id = 2, .expected = {}, .replacement = parent}},
                            .root_page_id = 2
    })
                    .ok());
    const auto           before = publisher.capture();
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options                  = credits.options();
    options.publication.pages.target_page_bytes   = 400;
    options.publication.pages.merge_trigger_bytes = 256;
    options.publication.pages.merge_target_bytes  = 350;
    auto callback                                 = make_page_maintenance(publisher, runtime, lane, options);
    EXPECT_TRUE(maintain(callback, publisher).first.ok());
    runtime.drain();
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.leaf_pages, 1U);
    EXPECT_EQ(after->root_page_id, 0U);
    EXPECT_EQ(after->mapping.get(0)->leaf->lookup("1001").cell.owner, right->leaf->lookup("1001").cell.owner);
    EXPECT_EQ(before->mapping.get(2), parent);
    EXPECT_EQ(before->mapping.get(1), right);
    const auto counters = publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Flush)];
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::MergeOperations)], 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::MergeInputPages)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::MergeOutputPages)], 1U);
}

TEST(PageMaintenanceOperation, CommittedCoalesceCountsInputOutputWhileOldReadersKeepPayload)
{
    PublicationCoordinator publisher;
    auto                   original = leaf(2);
    auto                   logical  = std::make_shared<PageState>(*original->leaf);
    logical->overlays.push_back(logical->overlays.front());
    original->leaf                    = logical;
    original->metrics.overlay_runs    = 2;
    original->metrics.overlay_entries = 4;
    ASSERT_TRUE(publish(publisher, {.record       = record(),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = original}},
                                    .root_page_id = 0})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 callback = make_page_maintenance(publisher, runtime, lane, credits.options());
    const auto           status   = maintain(callback, publisher).first;
    ASSERT_TRUE(status.ok()) << status.to_string();
    runtime.drain();
    auto counters = publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Flush)];
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::CoalesceOperations)], 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::CoalesceInputRuns)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::CoalesceOutputRuns)], 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::CoalesceInputEntries)], 4U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::CoalesceOutputEntries)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::PayloadReferencesDropped)], 2U);
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("1000").cell.owner,
              original->leaf->lookup("1000").cell.owner);
    ASSERT_TRUE(maintain(callback, publisher).first.ok());
    runtime.drain();
    EXPECT_EQ(publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Flush)]
                                                           [static_cast<size_t>(TreeWorkCounter::CoalesceOperations)],
              1U);
}

TEST(PageMaintenanceOperation, SmallStepBudgetMakesProgressAcrossIdleInvocations)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record       = record(),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = leaf(16)}},
                                    .root_page_id = 0})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options               = credits.options();
    options.max_steps                          = 1;
    options.publication.pages.max_work_entries = 2;
    auto callback                              = make_page_maintenance(publisher, runtime, lane, options);
    EXPECT_TRUE(maintain(callback, publisher).first.ok());
    runtime.drain();
    EXPECT_GT(publisher.capture()->stats.page_size_unknown_pages, 0U);
    const auto first_pages = publisher.capture()->stats.leaf_pages;
    for (size_t i = 0; i < 40; ++i) {
        ASSERT_TRUE(maintain(callback, publisher).first.ok());
    }
    runtime.drain();
    const auto after = publisher.capture();
    EXPECT_GT(after->stats.leaf_pages, first_pages);
    EXPECT_EQ(after->stats.page_size_unknown_pages, 0U);
    EXPECT_EQ(after->stats.kv_known_count, 16U);
    EXPECT_EQ(credits.acquired, 41U);
    for (size_t i = 0; i < 16; ++i) {
        const auto key = std::to_string(1000 + i);
        EXPECT_EQ(route(*after, key)->leaf->lookup(key).state, LogicalLookupState::kValue);
    }
}

TEST(PageMaintenanceOperation, ColdBaseDefersWithoutChangingTheLogicalPage)
{
    PublicationCoordinator publisher;
    auto                   page    = leaf(1);
    auto                   logical = std::make_shared<PageState>(*page->leaf);
    auto                   backing = std::make_shared<BackingRef>();
    backing->length                = 4096;
    backing->owner                 = std::make_shared<int>(1);
    logical->bases.push_back({.identity = 1, .backing = backing});
    page->leaf = logical;
    ASSERT_TRUE(
        publish(publisher,
                {.record = record(), .pages = {{.page_id = 0, .expected = {}, .replacement = page}}, .root_page_id = 0})
            .ok());
    const auto           before = publisher.capture();
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 callback = make_page_maintenance(publisher, runtime, lane, credits.options());
    auto                 result   = maintain(callback, publisher);
    runtime.drain();
    EXPECT_TRUE(result.first.ok());
    EXPECT_EQ(result.second, 1U);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_FALSE(logical->bases.front().resident);
}

TEST(PageMaintenanceOperation, EmptyTreeAndCorruptRoutingReturnErrors)
{
    PublicationCoordinator publisher;
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits     credits;
    auto                   callback = make_page_maintenance(publisher, runtime, lane, credits.options());
    EXPECT_EQ(maintain(callback, publisher).first.code(), Code::kInvalidArgument);
    auto invalid          = std::make_shared<PublishedPage>();
    invalid->level        = 1;
    invalid->inner        = std::make_shared<InnerPageState>();
    invalid->metrics.leaf = false;
    ASSERT_TRUE(publish(publisher, {.record       = record(),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = invalid}},
                                    .root_page_id = 0})
                    .ok());
    EXPECT_EQ(maintain(callback, publisher).first.code(), Code::kCorruption);
    runtime.drain();
}

TEST(PageMaintenanceOperation, PrepaidCreditRunsWithNoUnreservedCapacityAndLaterInvocationReacquires)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record       = record(),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = leaf(4)}},
                                    .root_page_id = 0})
                    .ok());
    const auto           before = publisher.capture()->commit_seq;
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options = credits.options();
    ASSERT_TRUE(credits.reserve(&options.prepaid).ok());
    size_t acquire_calls = 0;
    options.acquire      = [&](auto *) {
        ++acquire_calls;
        return Status::unavailable();
    };
    auto           callback = make_page_maintenance(publisher, runtime, lane, std::move(options));
    constexpr auto category = static_cast<size_t>(detail::BudgetClass::kReclaim);
    const auto     snapshot = credits.budget.snapshot();
    const auto     remaining =
        MaintenanceCredits::limits()[category] - snapshot.allocated[category] - snapshot.reserved[category];
    auto blocker = detail::ResourceBudget::try_reserve(credits.account, detail::BudgetClass::kReclaim, remaining);
    ASSERT_TRUE(blocker);
    auto result = maintain(callback, publisher);
    runtime.drain();
    EXPECT_TRUE(result.first.ok()) << result.first.to_string();
    EXPECT_GT(publisher.capture()->commit_seq, before);
    EXPECT_EQ(acquire_calls, 0U);
    EXPECT_EQ(credits.budget.snapshot().reserved[category], remaining);
    EXPECT_TRUE(maintain(callback, publisher).first.ok());
    runtime.drain();
    EXPECT_EQ(acquire_calls, 1U);
}

TEST(PageMaintenanceOperation, PrepaidCreditRejectsHandoffPartitionBeforeConsumption)
{
    PublicationCoordinator publisher;
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits     credits;
    auto                   options = credits.options();
    ASSERT_TRUE(credits.reserve(&options.prepaid, detail::BudgetClass::kHandoff).ok());
    const auto available = options.prepaid->available();
    EXPECT_THROW((void)make_page_maintenance(publisher, runtime, lane, options), std::invalid_argument);
    EXPECT_EQ(options.prepaid->available(), available);
}

TEST(PageMaintenanceOperation, FailedAcquisitionReturnsTerminalAndLeavesFactoryAvailableForRetry)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher, {.record       = record(),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = leaf(4)}},
                                    .root_page_id = 0})
                    .ok());
    detail::FlushRuntime runtime(1, 8);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    MaintenanceCredits   credits;
    auto                 options  = credits.options();
    size_t               attempts = 0;
    options.acquire               = [&](auto *out) -> Status {
        ++attempts;
        if (attempts == 1) {
            throw std::bad_alloc();
        }
        if (attempts == 2) {
            throw std::runtime_error("injected admission failure");
        }
        return credits.reserve(out);
    };
    auto       callback = make_page_maintenance(publisher, runtime, lane, std::move(options));
    const auto before   = publisher.capture();
    EXPECT_TRUE(maintain(callback, publisher).first.ok());
    EXPECT_EQ(maintain(callback, publisher).first.code(), Code::kInternal);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_TRUE(maintain(callback, publisher).first.ok());
    runtime.drain();
    EXPECT_EQ(attempts, 3U);
    EXPECT_GT(publisher.capture()->commit_seq, before->commit_seq);
}
} // namespace
} // namespace crowdb::tree
