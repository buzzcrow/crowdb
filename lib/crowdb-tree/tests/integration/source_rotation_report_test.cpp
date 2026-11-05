// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_rotation.h"
#include "flush_preparation_test_support.h"

namespace crowdb::tree
{
namespace
{
struct TestSoftFlush : TestPreparedFlushTree
{
    std::shared_ptr<detail::AllocationReservation> bootstrap;
    std::shared_ptr<ApplyProgress>                 progress;
    std::shared_ptr<ApplyDispatcher>               dispatcher;
    std::shared_ptr<SourceRotation>                rotation;

    explicit TestSoftFlush(bool cold = false) : TestPreparedFlushTree(1, cold)
    {
        auto paid = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 1024 * 1024);
        if (!paid) {
            throw std::bad_alloc();
        }
        bootstrap = detail::AllocationReservation::create(std::move(*paid));
        EXPECT_TRUE(ApplyProgress::create(8, bootstrap, &progress).ok());
        EXPECT_TRUE(ApplyDispatcher::create(runtime, lane, 4, bootstrap, &dispatcher).ok());
        preparation.cpu_progress = dispatcher;
        batch.cpu_progress       = dispatcher;
    }

    ~TestSoftFlush()
    {
        if (rotation) {
            rotation->stop();
            runtime->drain();
            while (!store->pending.empty()) {
                store->finish(Status::io_error());
                runtime->drain();
            }
            rotation->drain();
            runtime->drain();
            rotation.reset();
        }
        controller->release_pending();
        dispatcher->close_and_drain();
    }

    void configure(FlushBatch::Maintenance maintenance = {})
    {
        controller = std::make_unique<FlushController>(publisher, *runtime, sources, frontier, lane, batch,
                                                       std::move(maintenance), bootstrap, progress);
        rotation   = std::make_shared<SourceRotation>(
            sources, publisher, dispatcher, progress, account,
            SourceRotationOptions{.bytes = 1, .entries = 1, .tables = 2, .source_window = 2});
        rotation->set_async_flush(*controller, [this](FlushPreparationOptions *out) {
            *out = preparation;
            return Status::Ok();
        });
    }

    void write(const char *key, uint64_t slot)
    {
        SourceAdmission::Writer writer;
        ASSERT_TRUE(sources.try_admit(slot - 1, {1, 1, 1, 1}, &writer).ok());
        ASSERT_TRUE(writer.upsert(key, slot, encode_cell_buf(slot, OpKind::kPut, "v")).ok());
        writer.release();
        frontier.store(slot);
    }
};

TEST(SourceRotationReport, StaleAdmissionReplansWithoutAnotherWrite)
{
    TestSoftFlush tree;
    tree.configure();
    tree.write("a", 1);
    std::shared_ptr<FlushOperation> stale;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &stale).ok());
    auto request                = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(
        InstallToken{.generation = tree.publisher.capture()->generation, .batch = 998, .range = 0, .sequence = 0});
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(tree.publisher.try_submit(request));
    const auto sequence = tree.rotation->request();
    ASSERT_NE(sequence, 0U);
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    EXPECT_EQ(stale->result().handoff_status.code(), Code::kUnavailable);
    EXPECT_TRUE(stale->result().admission_stale);
    EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
    EXPECT_EQ(tree.publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "v");
}

TEST(SourceRotationReport, CancelledAdmissionDoesNotRestartItself)
{
    TestSoftFlush tree;
    tree.configure();
    tree.write("a", 1);
    std::shared_ptr<FlushOperation> cancelled;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &cancelled).ok());
    cancelled->request_stop();
    const auto sequence = tree.rotation->request();
    ASSERT_NE(sequence, 0U);
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    EXPECT_EQ(cancelled->result().handoff_status.code(), Code::kUnavailable);
    EXPECT_FALSE(cancelled->result().admission_stale);
    EXPECT_EQ(tree.publisher.capture()->coverage, 0U);
    EXPECT_FALSE(tree.sources.catalog()->active->closed());
}

TEST(SourceRotationReport, InflightCheckpointWaitsAndResumesWithoutAnotherWrite)
{
    TestSoftFlush tree;
    tree.configure();
    tree.write("a", 1);
    const auto checkpoint = [&](CheckpointAction action, uint64_t sequence) {
        auto request                    = std::make_shared<PublicationRequest>();
        request->transaction.record     = std::make_shared<InstallRecord>(InstallToken{
            .generation = tree.publisher.capture()->generation, .batch = 998, .range = 0, .sequence = sequence});
        request->transaction.checkpoint = CheckpointPublication{.action = action, .token = 1};
        if (action == CheckpointAction::Capture) {
            request->transaction.expected_coverage = 0;
        }
        request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
        EXPECT_TRUE(tree.publisher.try_submit(request));
    };
    checkpoint(CheckpointAction::Capture, 0);
    ASSERT_TRUE(tree.publisher.capture()->checkpoint_inflight);
    std::shared_ptr<FlushOperation> operation;
    bool                            blocked = false;
    EXPECT_EQ(tree.controller->prepare(tree.preparation, &operation, &blocked).code(), Code::kUnavailable);
    EXPECT_TRUE(blocked);
    EXPECT_FALSE(operation);
    FlushReservePlan plan;
    EXPECT_EQ(tree.controller->prepare_or_plan(tree.preparation, &operation, &plan).code(), Code::kUnavailable);
    EXPECT_FALSE(operation);
    EXPECT_FALSE(plan.view);
    const auto sequence = tree.rotation->request();
    ASSERT_NE(sequence, 0U);
    tree.runtime->drain();
    EXPECT_EQ(tree.publisher.capture()->coverage, 0U);
    EXPECT_FALSE(tree.sources.catalog()->active->closed());
    checkpoint(CheckpointAction::Abort, 1);
    tree.progress->advance();
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
    EXPECT_EQ(tree.publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "v");
}

TEST(SourceRotationReport, ReadFailureRetainsRepairWithoutRetryingItsOwnProgress)
{
    for (const auto code : {Code::kUnavailable, Code::kIoError}) {
        TestSoftFlush tree(true);
        tree.store->deferred = true;
        tree.configure();
        tree.write("a", 1);
        std::shared_ptr<FlushOperation> failed;
        ASSERT_TRUE(tree.controller->prepare(tree.preparation, &failed).ok());
        const auto sequence = tree.rotation->request();
        ASSERT_NE(sequence, 0U);
        tree.runtime->drain();
        ASSERT_EQ(tree.store->pending.size(), 1U);
        tree.store->finish(code == Code::kUnavailable ? Status::unavailable() : Status::io_error());
        tree.runtime->drain();
        tree.rotation->wait(sequence);
        EXPECT_EQ(failed->result().handoff_status.code(), code);
        EXPECT_EQ(failed->result().phase, FlushPhase::kRepairRequired);
        EXPECT_FALSE(failed->result().admission_stale);
        EXPECT_EQ(tree.store->reads.load(), 1U);
        EXPECT_TRUE(tree.store->pending.empty());
        EXPECT_EQ(tree.publisher.capture()->coverage, 0U);
        const auto                      batch = failed->result().batch_id;
        std::shared_ptr<FlushOperation> retry;
        ASSERT_TRUE(tree.controller->prepare(tree.preparation, &retry).ok());
        ASSERT_EQ(retry, failed);
        tree.store->deferred = false;
        ASSERT_TRUE(retry->wait().ok());
        EXPECT_EQ(retry->result().batch_id, batch);
        EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
    }
}

TEST(SourceRotationReport, MaintenanceFailureKeepsCoverageWithoutRepeatingHandoff)
{
    TestSoftFlush tree;
    size_t        calls = 0;
    tree.configure([&](auto, auto done) {
        ++calls;
        done(Status::unavailable(), 1);
    });
    tree.write("a", 1);
    std::shared_ptr<FlushOperation> failed;
    ASSERT_TRUE(tree.controller->prepare(tree.bootstrap, &failed).ok());
    const auto sequence = tree.rotation->request();
    ASSERT_NE(sequence, 0U);
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    EXPECT_EQ(calls, 1U);
    EXPECT_TRUE(failed->result().handoff_status.ok());
    EXPECT_EQ(failed->result().maintenance_status.code(), Code::kUnavailable);
    EXPECT_FALSE(failed->result().admission_stale);
    EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
    EXPECT_EQ(tree.publisher.capture()->stats.overlay_entries, 1U);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(SourceRotationReport, PartialHandoffRetryKeepsInstalledRunsAndBatchIdentity)
{
    TestSoftFlush                   tree;
    std::shared_ptr<FlushOperation> failed;
    bool                            stop = true;
    tree.batch.parallelism               = 1;
    tree.batch.after_run_for_tests       = [&](size_t range, size_t) {
        if (stop && range == 1) {
            failed->request_stop();
        }
    };
    tree.configure();
    tree.write("a", 1);
    tree.write("z", 2);
    ASSERT_TRUE(tree.controller->prepare(tree.bootstrap, &failed).ok());
    const auto sequence = tree.rotation->request();
    ASSERT_NE(sequence, 0U);
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    ASSERT_EQ(failed->result().phase, FlushPhase::kRepairRequired);
    EXPECT_FALSE(failed->result().admission_stale);
    EXPECT_EQ(tree.publisher.capture()->coverage, 0U);
    ASSERT_EQ(tree.publisher.capture()->stats.overlay_entries, 1U);
    const auto                      installed = tree.publisher.capture()->mapping.get(0);
    const auto                      batch     = failed->result().batch_id;
    std::shared_ptr<FlushOperation> retry;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &retry).ok());
    ASSERT_EQ(retry, failed);
    stop = false;
    ASSERT_TRUE(retry->wait().ok());
    EXPECT_EQ(retry->result().batch_id, batch);
    EXPECT_EQ(tree.publisher.capture()->mapping.get(0), installed);
    EXPECT_EQ(tree.publisher.capture()->stats.overlay_entries, 2U);
    EXPECT_EQ(tree.publisher.capture()->coverage, 2U);
}
#endif

struct TestReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::atomic<bool> *done;

    void set_value(FlushResult report) const noexcept
    {
        EXPECT_TRUE(report.handoff_status.ok());
        done->store(true);
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
        done->store(true);
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "accepted handoff was cancelled";
        done->store(true);
    }
};

TEST(SourceRotationReport, CloseCancelsBusyConnectionWaitAndDrainsAcceptedHandoff)
{
    TestSoftFlush tree;
    tree.configure();
    SourceAdmission::Writer writer;
    ASSERT_TRUE(tree.sources.try_admit(0, {1, 1, 1, 1}, &writer).ok());
    ASSERT_TRUE(writer.upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")).ok());
    tree.frontier.store(1);
    std::shared_ptr<FlushOperation> accepted;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &accepted).ok());

    std::atomic<bool> done{false};
    auto              connected = stdexec::connect(accepted->report_sender(), TestReceiver{&done});
    tree.rotation->set_async_flush(*tree.controller, [&](FlushPreparationOptions *out) {
        *out = tree.preparation;
        stdexec::start(connected);
        return Status::Ok();
    });
    const auto sequence = tree.rotation->request();
    EXPECT_NE(sequence, 0U);
    tree.runtime->drain();
    EXPECT_FALSE(done.load());
    EXPECT_TRUE(writer.table()->closed());
    tree.rotation->stop();
    tree.runtime->drain();
    tree.rotation->wait(sequence);
    EXPECT_FALSE(done.load());
    writer.release();
    tree.runtime->drain();
    EXPECT_TRUE(done.load());
    EXPECT_EQ(tree.publisher.capture()->coverage, 1U);
}
} // namespace
} // namespace crowdb::tree
