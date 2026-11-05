// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"

#include <gtest/gtest.h>

#include <future>
#include <latch>

namespace crowdb::tree
{
namespace
{
void initialize(PublicationCoordinator &publisher, std::vector<std::shared_ptr<MemTable>> tables)
{
    auto left           = std::make_shared<PageState>();
    left->range.upper   = "m";
    left->right_sibling = 1;
    auto right          = std::make_shared<PageState>();
    right->range.lower  = "m";
    auto first          = std::make_shared<PublishedPage>();
    first->leaf         = left;
    auto second         = std::make_shared<PublishedPage>();
    second->leaf        = right;
    auto index          = std::make_shared<InnerPageState>();
    index->separators   = {"m"};
    index->children     = {0, 1};
    auto root           = std::make_shared<PublishedPage>();
    root->inner         = index;
    root->level         = 1;
    root->metrics.leaf  = false;
    auto catalog        = std::make_shared<PublishedSources>();
    catalog->tables.assign(tables.begin(), tables.end());
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {
        .record = std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 0, .range = 0, .sequence = 0}
          ),
        .pages  = {{.page_id = 0, .expected = {}, .replacement = first},
                                                               {.page_id = 1, .expected = {}, .replacement = second},
                                                               {.page_id = 2, .expected = {}, .replacement = root}},
        .root_page_id     = 2,
        .expected_sources = publisher.capture()->sources,
        .sources          = catalog
    };
    request->on_done = [](const auto &result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

std::shared_ptr<MemTable> table(std::initializer_list<std::pair<std::string, uint64_t>> entries)
{
    auto source = std::make_shared<MemTable>();
    for (const auto &[key, slot] : entries) {
        EXPECT_TRUE(source->upsert(key, slot, encode_cell_buf(slot, OpKind::kPut, key + std::to_string(slot))));
    }
    source->close();
    return source;
}

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<void> *done;

    void set_value() const noexcept
    {
        done->set_value();
    }

    void set_error(Status error) const noexcept
    {
        ADD_FAILURE() << error.to_string();
        done->set_value();
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "unexpected unreported stop";
        done->set_value();
    }
};

TEST(ParallelFlush, AllMemTablesMergeByRangeBeforeMaintenanceWithSingleOrMultipleWorkers)
{
    for (const size_t workers : {1U, 4U}) {
        detail::FlushRuntime   runtime(workers, 16);
        auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(16);
        PublicationCoordinator publisher;
        auto                   older = table({
            {"a", 1},
            {"b", 1},
            {"c", 1},
            {"d", 1},
            {"e", 1},
            {"m", 2},
            {"z", 8}
        });
        auto                   newer = table({
            {"a", 3},
            {"m", 4},
            {"n", 4}
        });
        const void            *payload;
        {
            auto guard = newer->read_guard();
            payload    = newer->find("a")->cell.data() + kCellHeaderSize;
        }
        initialize(publisher, {older, newer});
        auto              before          = publisher.capture();
        bool              maintenance_ran = false;
        FlushBatchOptions options;
        options.run_limits.max_entries = 2;
        std::shared_ptr<FlushBatch> batch;
        auto                        status = FlushBatch::prepare(
            publisher, runtime, lane, 1, 4, options,
            [&](auto view, auto done) {
                EXPECT_EQ(view->coverage, 4U);
                EXPECT_EQ(view->mapping.get(0)->leaf->lookup("a").cell.slot, 3U);
                EXPECT_EQ(view->mapping.get(1)->leaf->lookup("m").cell.slot, 4U);
                maintenance_ran = true;
                done(Status::Ok(), 2);
            },
            &batch);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
        runtime.drain();
        EXPECT_TRUE(maintenance_ran);
        EXPECT_EQ(batch->result().phase, FlushPhase::kComplete);
        EXPECT_EQ(batch->result().published_coverage, 4U);
        const auto after = publisher.capture();
        for (const auto pid : {0U, 1U}) {
            ASSERT_TRUE(after->snapshot_changes.get(pid));
            EXPECT_EQ(after->snapshot_changes.get(pid)->effect,
                      SnapshotChangeEffect::FullContent | SnapshotChangeEffect::AcceptedPrefix);
        }
        EXPECT_EQ(after->stats.leaf_pages, 2U);
        EXPECT_EQ(after->stats.height, 2U);
        EXPECT_EQ(after->stats.dirty_pages, 2U);
        EXPECT_EQ(after->stats.overlay_entries, 7U);
        EXPECT_EQ(after->mapping.get(0)->leaf->lookup("a").cell.value.data(), payload);
        EXPECT_EQ(after->mapping.get(1)->leaf->lookup("z").state, LogicalLookupState::kMissing);
        ASSERT_EQ(after->sources->tables.size(), 1U);
        EXPECT_EQ(after->sources->tables.front(), older);
        EXPECT_EQ(after->sources->floor, 4U);
        EXPECT_EQ(before->coverage, 0U);
        EXPECT_EQ(before->sources->tables.size(), 2U);
        EXPECT_EQ(before->mapping.get(0)->leaf->lookup("a").state, LogicalLookupState::kMissing);
    }
}

TEST(ParallelFlush, PartialHandoffRetainsSourcesAndRetryDoesNotDuplicateInstalledRuns)
{
    detail::FlushRuntime   runtime(1, 1);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(1);
    PublicationCoordinator publisher;
    initialize(publisher, {
                              table({{"a", 1}, {"m", 1}}
                              )
    });
    std::latch entered(1);
    std::latch release(1);

    struct Blocker
    {
        std::latch &entered;
        std::latch &release;
    } blocker{.entered = entered, .release = release};

    ASSERT_TRUE(runtime
                    .try_submit(
                        lane,
                        [](void *context) noexcept {
                            auto *block = static_cast<Blocker *>(context);
                            block->entered.count_down();
                            block->release.wait();
                        },
                        &blocker)
                    .ok());
    entered.wait();
    std::shared_ptr<FlushBatch> batch;
    FlushBatchOptions           options;
    // This case exercises queued-child admission failure.
    options.inline_ranges_per_step = 0;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    std::promise<void> done;
    auto               terminal  = done.get_future();
    auto               operation = stdexec::connect(batch->sender(), Receiver{&done});
    stdexec::start(operation);
    release.count_down();
    terminal.get();
    runtime.drain();
    EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
    EXPECT_FALSE(batch->result().handoff_status.ok());
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_EQ(publisher.capture()->sources->tables.size(), 1U);
    auto installed = publisher.capture()->mapping.get(0);
    ASSERT_EQ(installed->leaf->overlays.size(), 1U);
    ASSERT_TRUE(publisher.capture()->pending_handoff);
    auto checkpoint         = std::make_shared<PublicationRequest>();
    checkpoint->transaction = {
        .record            = std::make_shared<InstallRecord>(InstallToken{.batch = 2}
                     ),
        .expected_coverage = 0,
        .checkpoint        = CheckpointPublication{.action = CheckpointAction::Capture, .token = 1}
    };
    checkpoint->on_done = [](PublicationResult result) { EXPECT_EQ(result.status.code(), Code::kUnavailable); };
    ASSERT_TRUE(publisher.try_submit(checkpoint));
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(batch->result().published_coverage, 1U);
    EXPECT_EQ(publisher.capture()->mapping.get(0), installed);
    EXPECT_EQ(publisher.capture()->stats.overlay_entries, 2U);
    EXPECT_TRUE(publisher.capture()->sources->tables.empty());
    EXPECT_FALSE(publisher.capture()->pending_handoff);
}

TEST(ParallelFlush, MaintenanceFailurePreservesCoverageAndRetriesOnlyMaintenance)
{
    detail::FlushRuntime   runtime(2, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize(publisher, {
                              table({{"a", 1}, {"m", 1}}
                              )
    });
    std::shared_ptr<FlushBatch>           batch;
    size_t                                attempts = 0;
    std::function<void(Status, uint64_t)> late;
    ASSERT_TRUE(FlushBatch::prepare(
                    publisher, runtime, lane, 1, 1, {},
                    [&](auto, auto done) {
                        ++attempts;
                        if (attempts == 1) {
                            late = done;
                        }
                        else {
                            late(Status::corruption("stale maintenance completion"), 999);
                        }
                        done(attempts == 1 ? Status::io_error("controlled writeback failure") : Status::Ok(), 2);
                    },
                    &batch)
                    .ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(batch->result().maintenance_status.code(), Code::kIoError);
    EXPECT_EQ(batch->result().published_coverage, 1U);
    auto covered = publisher.capture();
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().maintenance_status.ok());
    EXPECT_EQ(batch->result().deferred_pages, 2U);
    EXPECT_EQ(attempts, 2U);
    EXPECT_EQ(publisher.capture(), covered);
    const auto work = publisher.work_metrics()->totals();
    EXPECT_EQ(work[static_cast<size_t>(TreeStage::FlushBatch)].count, 2U);
    EXPECT_EQ(work[static_cast<size_t>(TreeStage::FlushHandoff)].count, 1U);
    EXPECT_EQ(work[static_cast<size_t>(TreeStage::MaintenancePlan)].count, 2U);
}

TEST(ParallelFlush, PreStartCancellationRetainsCapturedBatchForRetry)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize(publisher, {
                              table({{"a", 1}, {"m", 1}}
                              )
    });
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, {}, {}, &batch).ok());
    batch->request_stop();
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_EQ(publisher.capture()->stats.overlay_entries, 0U);
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(batch->result().published_coverage, 1U);
    EXPECT_EQ(publisher.capture()->stats.overlay_entries, 2U);
}

TEST(ParallelFlush, ColdLeafAcceptsOverlayWithoutMaterializingBase)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize(publisher, {
                              table({{"a", 1}}
                              )
    });
    auto original   = publisher.capture()->mapping.get(0);
    auto leaf       = std::make_shared<PageState>(*original->leaf);
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 4096;
    backing->owner  = std::make_shared<uint8_t>(0);
    leaf->bases.push_back({.identity = 1, .range = leaf->range, .max_slot = 0, .backing = backing});
    auto proxy           = std::make_shared<PublishedPage>(*original);
    proxy->leaf          = leaf;
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {
        .record = std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 0, .range = 1, .sequence = 0}),
        .pages  = {{.page_id = 0, .expected = original, .replacement = proxy}}};
    ASSERT_TRUE(publisher.try_submit(request));
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, {}, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    auto current = publisher.capture()->mapping.get(0)->leaf;
    EXPECT_EQ(current->bases.front().backing, backing);
    EXPECT_FALSE(current->bases.front().resident);
    EXPECT_EQ(current->lookup("a").cell.value.to_string(), "a1");
    EXPECT_EQ(current->lookup("b").state, LogicalLookupState::kNeedsBase);
}

TEST(ParallelFlush, DetachedSourceIndexCanDieWhileOverlayValueRemainsReadable)
{
    detail::FlushRuntime    runtime(1, 8);
    auto                    lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator  publisher;
    auto                    source = table({
        {"a", 1},
        {"m", 1}
    });
    std::weak_ptr<MemTable> weak   = source;
    initialize(publisher, {source});
    source.reset();
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, {}, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    publisher.reclaim();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "a1");
}

TEST(ParallelFlush, SuccessorRemainsVisibleAndDoesNotExpandCapturedFrontier)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    auto                   source    = table({
        {"a", 1}
    });
    auto                   successor = std::make_shared<MemTable>();
    ASSERT_TRUE(successor->upsert("n", 2, encode_cell_buf(2, OpKind::kPut, "new")));
    initialize(publisher, {source, successor});
    auto inputs    = std::make_shared<PublishedSources>();
    inputs->tables = {source};
    FlushBatchOptions           options;
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch, inputs).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok());
    auto current = publisher.capture();
    EXPECT_EQ(current->coverage, 1U);
    ASSERT_EQ(current->sources->tables.size(), 1U);
    EXPECT_EQ(current->sources->tables.front(), successor);
    EXPECT_EQ(current->mapping.get(1)->leaf->lookup("n").state, LogicalLookupState::kMissing);
    EXPECT_EQ(current->stats.overlay_entries, 1U);
    auto guard = successor->read_guard();
    ASSERT_NE(successor->find("n"), nullptr);
    EXPECT_EQ(successor->find("n")->slot, 2U);
}

TEST(ParallelFlush, EmptyInputAdvancesCoverageWithoutInstallingEmptyRuns)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize(publisher, {table({})});
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 8, {}, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok());
    auto current = publisher.capture();
    EXPECT_EQ(current->coverage, 8U);
    EXPECT_EQ(current->stats.overlay_runs, 0U);
    EXPECT_TRUE(current->sources->tables.empty());
}

TEST(ParallelFlush, CannotDropAnotherClosedTableFromBatchInputs)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    auto                   source = table({
        {"a", 1}
    });
    initialize(publisher, {
                              source, table({{"m", 2}}
                               )
    });
    auto inputs    = std::make_shared<PublishedSources>();
    inputs->tables = {source};
    std::shared_ptr<FlushBatch> batch;
    EXPECT_EQ(FlushBatch::prepare(publisher, runtime, lane, 1, 2, {}, {}, &batch, inputs).code(),
              Code::kInvalidArgument);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
}
} // namespace
} // namespace crowdb::tree
