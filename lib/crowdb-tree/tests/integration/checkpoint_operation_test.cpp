// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_checkpoint_test_support.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
namespace
{
using namespace test_write;

struct TestCheckpointPipeline
{
    TestCheckpoint                            local;
    std::shared_ptr<LocalBackendState>        backend = std::make_shared<LocalBackendState>();
    std::shared_ptr<const PublishedTreeState> initial = [] {
        auto result = captured_leaf(page({
            {.key = "a", .value = "v"}
        }));
        EXPECT_TRUE(apply_page_metrics(result->stats, nullptr, &result->mapping.get(0)->metrics));
        return result;
    }();
    PublicationCoordinator publisher{
        4, 1, {},
          std::pmr::get_default_resource(), {},
          {.metadata = {}, .initial = initial, .source_catalog = {}}
    };
    std::shared_ptr<detail::AllocationReservation> finish;
    std::shared_ptr<CheckpointOperation>           operation;

    TestCheckpointPipeline()
    {
        backend->store       = local.store;
        backend->ledger      = local.source.ledger;
        backend->checkpoints = local.registry;
        auto credit          = local.source.credits.credits.reservation()->take_reserved(
            CheckpointOperation::finish_allocation_bound() + 512);
        EXPECT_TRUE(credit.has_value());
        if (credit) {
            finish = detail::AllocationReservation::create(std::move(*credit));
        }
    }

    Status prepare(CheckpointOperation::PrepareWork work = {}, std::optional<CheckpointOptions> supplied = {})
    {
        CheckpointOptions options;
        options.write.frames.encode.frame_bytes        = 4096;
        options.write.frames.encode.inline_value_bytes = 16;
        options.write.write.max_frame_bytes            = 4096;
        if (supplied) {
            options = *supplied;
        }
        auto loader = std::make_shared<test_read::TestLoader>();
        return CheckpointOperation::create({}, publisher, backend, local.store, local.source.runtime, local.source.lane,
                                           options, local.source.credits.credits, finish, loader, {}, std::move(work),
                                           &operation);
    }

    void replace_live()
    {
        auto before                 = publisher.capture();
        auto leaf                   = std::make_shared<PageState>(*page({
            {.key = "a", .value = "new"}
        }));
        leaf->content_id            = before->mapping.get(0)->leaf->content_id + 1;
        auto replacement            = std::make_shared<PublishedPage>(*before->mapping.get(0));
        replacement->leaf           = std::move(leaf);
        auto request                = std::make_shared<PublicationRequest>();
        request->transaction.record = std::make_shared<InstallRecord>(InstallToken{.sequence = 99});
        request->transaction.pages.push_back(
            {.page_id = 0, .expected = before->mapping.get(0), .replacement = replacement});
        request->on_done = [](PublicationResult result) {
            EXPECT_TRUE(result.status.ok()) << result.status.to_string();
        };
        ASSERT_TRUE(publisher.try_submit(request));
    }
};

TEST(CheckpointOperation, DurableCommitReconcilesOnlyItsCapturedView)
{
    TestCheckpointPipeline test;
    test.local.store->defer_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    ASSERT_TRUE(test.publisher.capture()->checkpoint_inflight);
    const auto waiting_metrics = test.publisher.work_metrics()->totals();
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::SnapshotCapture)].count, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].count, 0U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].inflight, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].inflight, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].count, 0U);
    test.replace_live();
    const auto live = test.publisher.capture()->mapping.get(0);
    test.local.store->finish();
    test.local.source.runtime->drain();
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_TRUE(test.operation->result().durable_committed);
    EXPECT_EQ(test.operation->result().durable_coverage, 10U);
    EXPECT_EQ(test.operation->result().snapshot_sequence, 1U);
    EXPECT_EQ(test.publisher.capture()->mapping.get(0), live);
    EXPECT_EQ(test.publisher.capture()->stats.checkpoint_pending_pages, 1U);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    const auto baseline = test.publisher.capture()->snapshot_baseline;
    ASSERT_TRUE(baseline);
    ASSERT_TRUE(baseline->pages);
    EXPECT_EQ(baseline->coverage, 10U);
    EXPECT_EQ(baseline->snapshot_sequence, 1U);
    EXPECT_EQ(baseline->pages->root_page_id, 0U);
    EXPECT_EQ(baseline->pages->frames.size(), 1U);
    ASSERT_TRUE(test.publisher.capture()->snapshot_changes.get(0));
    EXPECT_EQ(test.publisher.capture()->snapshot_changes.get(0)->after.content, live->leaf->content_id);
    const auto committed_metrics = test.publisher.work_metrics()->totals();
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].count, 1U);
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].inflight, 0U);
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Success)],
              1U);
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].count, 1U);
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].inflight, 0U);
    EXPECT_EQ(committed_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Stale)],
              1U);
    EXPECT_EQ(test.publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(
                  TreeWorkCause::Checkpoint)][static_cast<size_t>(TreeWorkCounter::RebaseMismatches)],
              1U);
    test.local.expect_read("v");
}

TEST(CheckpointOperation, FailedMetadataAbortsPublicationWithoutAdvancingDurability)
{
    TestCheckpointPipeline test;
    test.local.store->fail_at = 2;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(
                  TreeWorkCause::Checkpoint)][static_cast<size_t>(TreeWorkCounter::CheckpointFailures)],
              1U);
    EXPECT_EQ(test.operation->result().status.code(), Code::kIoError);
    EXPECT_FALSE(test.operation->result().durable_committed);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_FALSE(test.local.registry->needs_recovery());
    EXPECT_FALSE(test.publisher.capture()->snapshot_baseline);
    EXPECT_FALSE(test.publisher.capture()->snapshot_changes.empty());
    const auto failed_metrics = test.publisher.work_metrics()->totals();
    EXPECT_EQ(failed_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].count, 1U);
    EXPECT_EQ(failed_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].inflight, 0U);
    EXPECT_EQ(failed_metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Error)],
              1U);
    EXPECT_EQ(failed_metrics[static_cast<size_t>(TreeStage::SnapshotCommit)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Error)],
              1U);
}

TEST(CheckpointOperation, UnknownAnchorKeepsRegistryFencedAfterPublicationAbort)
{
    TestCheckpointPipeline test;
    test.local.store->throw_at = 5;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_TRUE(test.local.registry->needs_recovery());
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
    ASSERT_TRUE(test.local.store->pending);
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(test.prepare().code(), Code::kUnavailable);
}

TEST(CheckpointOperation, CancellationBeforeAnchorWaitsForWriteAndAbortsCapture)
{
    TestCheckpointPipeline test;
    test.local.store->defer_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    test.operation->request_stop();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 0U);
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
}

TEST(CheckpointOperation, CancellationAfterAnchorCompletesCommitEvenAfterPublisherClose)
{
    TestCheckpointPipeline test;
    test.local.store->defer_at = 6;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    ASSERT_TRUE(test.local.store->pending);
    ASSERT_EQ(test.local.store->events.back(), "barrier");
    test.publisher.close();
    test.operation->request_stop();
    test.local.source.runtime->drain();
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_TRUE(test.operation->result().durable_committed);
    EXPECT_EQ(test.local.registry->durable_coverage(), 10U);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
}

TEST(CheckpointOperation, ExhaustedMaterializationStillHasIndependentAbortCredit)
{
    TestCheckpointPipeline test;
    test.local.store->defer_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    auto       credit    = test.local.source.credits.credits.reservation();
    const auto remaining = credit->available();
    ASSERT_TRUE(credit->claim(remaining));
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.operation->result().status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    credit->release(remaining);
}

TEST(CheckpointOperation, ChangedPublicationAfterLayoutIsRejectedBeforeCapture)
{
    TestCheckpointPipeline test;
    ASSERT_TRUE(test.prepare([&test](const PublishedTreeState &, const CapturedWritePlan *plan, CheckpointWork *work) {
                        work->budget = test.local.source.credits.credits;
                        work->loader = std::make_shared<test_read::TestLoader>();
                        if (plan != nullptr) {
                            test.replace_live();
                        }
                        return Status::Ok();
                    })
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kUnavailable);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_TRUE(test.local.store->events.empty());
    EXPECT_EQ(test.backend->checkpoints->durable_coverage(), 0U);
}

TEST(CheckpointOperation, NewL0CatalogBetweenLayoutAndCapturePreservesTheFixedPrefix)
{
    TestCheckpointPipeline test;
    bool                   published = false;
    ASSERT_TRUE(test.prepare([&](const PublishedTreeState &, const CapturedWritePlan *plan, CheckpointWork *work) {
                        work->budget = test.local.source.credits.credits;
                        work->loader = std::make_shared<test_read::TestLoader>();
                        if (plan != nullptr) {
                            const auto before  = test.publisher.capture();
                            auto       sources = std::make_shared<PublishedSources>(*before->sources);
                            sources->tables.push_back(std::make_shared<MemTable>());
                            auto request                = std::make_shared<PublicationRequest>();
                            request->transaction.record = std::make_shared<InstallRecord>(InstallToken{.sequence = 99});
                            request->transaction.expected_sources = before->sources;
                            request->transaction.sources          = std::move(sources);
                            request->on_done                      = [&](PublicationResult result) {
                                EXPECT_TRUE(result.status.ok()) << result.status.to_string();
                                published = result.status.ok();
                            };
                            EXPECT_TRUE(test.publisher.try_submit(request));
                        }
                        return Status::Ok();
                    })
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    EXPECT_TRUE(published);
    ASSERT_EQ(done.values.load(), 1U) << test.operation->result().status.to_string();
    EXPECT_TRUE(test.operation->result().durable_committed);
    EXPECT_EQ(test.operation->result().durable_coverage, 10U);
    EXPECT_EQ(test.publisher.capture()->sources->tables.size(), 1U);
}

TEST(CheckpointOperation, PlannedWorkAdmissionFailureDoesNotCaptureOrWrite)
{
    TestCheckpointPipeline test;
    ASSERT_TRUE(test.prepare([&test](const PublishedTreeState &, const CapturedWritePlan *plan, CheckpointWork *work) {
                        if (plan != nullptr) {
                            return Status::resource_exhausted();
                        }
                        work->budget = test.local.source.credits.credits;
                        work->loader = std::make_shared<test_read::TestLoader>();
                        return Status::Ok();
                    })
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_TRUE(test.local.store->events.empty());
}

TEST(CheckpointOperation, InvalidGeometryIsRejectedBeforeAnyCaptureOrIo)
{
    TestCheckpointPipeline test;
    CheckpointOptions      options;
    options.write.write.iu_size          = 8192;
    options.write.write.buffer_alignment = 8192;
    EXPECT_EQ(test.prepare({}, options).code(), Code::kInvalidArgument);
    EXPECT_FALSE(test.operation);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_TRUE(test.local.store->events.empty());
    options.write.write.iu_size             = 4096;
    options.write.frames.encode.frame_bytes = options.write.write.max_frame_bytes + 1;
    EXPECT_EQ(test.prepare({}, options).code(), Code::kInvalidArgument);
    EXPECT_FALSE(test.operation);
    EXPECT_TRUE(test.local.store->events.empty());
}

} // namespace
} // namespace crowdb::tree
