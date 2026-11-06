// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void initialize_windows(PublicationCoordinator &publisher, size_t entries)
{
    auto table = std::make_shared<MemTable>();
    for (size_t i = 0; i < entries; ++i) {
        const auto key = std::to_string(1000 + i);
        ASSERT_TRUE(table->upsert(key, 1, encode_cell_buf(1, OpKind::kPut, "old")));
        ASSERT_TRUE(table->upsert(key, 2, encode_cell_buf(2, OpKind::kPut, "latest")));
    }
    table->close();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(std::move(table));
    auto page            = std::make_shared<PublishedPage>();
    page->leaf           = std::make_shared<PageState>();
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {.record           = std::make_shared<InstallRecord>(InstallToken{}),
                            .pages            = {{.page_id = 0, .expected = {}, .replacement = page}},
                            .root_page_id     = 0,
                            .expected_sources = publisher.capture()->sources,
                            .sources          = sources};
    request->on_done     = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(publisher.try_submit(request));
}

void verify_window_values(const std::shared_ptr<const PublishedTreeState> &view, size_t count)
{
    EXPECT_EQ(view->stats.overlay_entries, count);
    const auto page = view->mapping.get(0);
    for (size_t i = 0; i < count; ++i) {
        const auto value = page->leaf->lookup(std::to_string(1000 + i));
        ASSERT_EQ(value.state, LogicalLookupState::kValue);
        EXPECT_EQ(value.cell.value.to_string(), "latest");
    }
    ASSERT_EQ(page->leaf->overlays.size(), count);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(page->leaf->overlays[i]->entries().front().key().to_string(), std::to_string(1000 + i));
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(FlushWindow, GeometricInstallationsPublishPrefixesBeforeGlobalCoverage)
{
    for (size_t workers : {1U, 4U}) {
        detail::FlushRuntime   runtime(workers, 8);
        auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher;
        initialize_windows(publisher, 6);
        const auto        old = publisher.capture();
        FlushBatchOptions options;
        options.install_run_window     = 1;
        options.run_limits.max_entries = 1;
        std::vector<size_t> prefixes;
        options.after_run_for_tests = [&](size_t, size_t) {
            const auto view = publisher.capture();
            prefixes.push_back(view->stats.overlay_entries);
            EXPECT_EQ(view->coverage, 0U);
            ASSERT_TRUE(view->pending_handoff);
            EXPECT_FALSE(view->sources->tables.empty());
        };
        bool                        maintained = false;
        std::shared_ptr<FlushBatch> batch;
        ASSERT_TRUE(FlushBatch::prepare(
                        publisher, runtime, lane, 1, 2, options,
                        [&](auto view, auto done) {
                            maintained = true;
                            EXPECT_EQ(view->coverage, 2U);
                            verify_window_values(view, 6);
                            done(Status::Ok(), 0);
                        },
                        &batch)
                        .ok());
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
        runtime.drain();
        ASSERT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
        EXPECT_EQ(prefixes, (std::vector<size_t>{0, 1, 2, 2, 4, 4}));
        EXPECT_TRUE(maintained);
        EXPECT_EQ(publisher.capture()->coverage, 2U);
        EXPECT_TRUE(publisher.capture()->sources->tables.empty());
        EXPECT_EQ(old->stats.overlay_entries, 0U);
        verify_window_values(publisher.capture(), 6);
    }
}

TEST(FlushWindow, CancelAfterCommittedPrefixResumesPreparedSuffixExactlyOnce)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize_windows(publisher, 6);
    FlushBatchOptions options;
    options.install_run_window     = 1;
    options.run_limits.max_entries = 1;
    std::shared_ptr<FlushBatch> batch;
    std::vector<size_t>         prepared;
    options.after_run_for_tests = [&](size_t, size_t count) {
        prepared.push_back(count);
        if (count == 3) {
            batch->request_stop();
        }
    };
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 2, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    verify_window_values(publisher.capture(), 2);
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(prepared, (std::vector<size_t>{1, 2, 3, 4, 5, 6}));
    verify_window_values(publisher.capture(), 6);
    EXPECT_EQ(publisher.capture()->coverage, 2U);
}

TEST(FlushWindow, RejectedContinuationKeepsCommittedWindowAndStableNextRecord)
{
    detail::FlushRuntime   runtime(1, 1);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(1);
    PublicationCoordinator publisher;
    initialize_windows(publisher, 4);
    FlushBatchOptions options;
    options.install_run_window     = 1;
    options.run_limits.max_entries = 1;
    std::vector<size_t> prepared;
    bool                other_ran = false;
    options.after_run_for_tests   = [&](size_t, size_t count) {
        prepared.push_back(count);
        if (count == 1) {
            EXPECT_TRUE(
                runtime.try_submit(
                           lane, [](void *raw) noexcept { *static_cast<bool *>(raw) = true; }, &other_ran)
                    .ok());
        }
    };
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 2, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    EXPECT_TRUE(other_ran);
    EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
    verify_window_values(publisher.capture(), 1);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(prepared, (std::vector<size_t>{1, 2, 3, 4}));
    verify_window_values(publisher.capture(), 4);
}
#endif

TEST(FlushWindow, AllInstallRootsAndArraysFitThePrepaidBoundAtTheHardLimit)
{
    detail::ResourceBudget::Limits limits;
    limits.fill(64 * 1024 * 1024);
    detail::ResourceBudget budget(limits, 4);
    auto                   account  = budget.make_account(limits);
    constexpr auto         category = detail::BudgetClass::kHandoff;
    {
        detail::FlushRuntime   runtime(1, 8);
        auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher;
        initialize_windows(publisher, 64);
        FlushBatchOptions options;
        options.install_run_window     = 2;
        options.run_limits.max_entries = 1;
        const auto bound = FlushBatch::metadata_reserve_bound(publisher.capture()->mapping, 1, 64, 256, 1, options);
        if (!bound) {
            FAIL() << "window footprint exceeds representable allocation bound";
        }
        auto credit = detail::ResourceBudget::try_reserve(account, category, *bound);
        if (!credit) {
            FAIL() << "window footprint exceeds configured handoff quota";
        }
        auto metadata = detail::AllocationReservation::create(std::move(*credit));
        auto remaining =
            detail::ResourceBudget::try_reserve(account, category, limits[static_cast<size_t>(category)] - *bound);
        ASSERT_TRUE(remaining);
        std::shared_ptr<FlushBatch> batch;
        ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 2, options, {}, &batch, {}, metadata).ok());
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
        runtime.drain();
        ASSERT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
        verify_window_values(publisher.capture(), 64);
        metadata->seal();
    }
    EXPECT_EQ(budget.snapshot().allocated[static_cast<size_t>(category)], 0U);
    EXPECT_EQ(budget.snapshot().reserved[static_cast<size_t>(category)], 0U);
}
} // namespace
} // namespace crowdb::tree
