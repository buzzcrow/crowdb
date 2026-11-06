// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_operation.h"

#include <gtest/gtest.h>

#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct FlushCredits
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(4 * 1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve() const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 1024 * 1024);
        if (!credit.has_value()) {
            ADD_FAILURE() << "test reservation failed";
            return {};
        }
        return std::make_shared<detail::AllocationReservation>(std::move(credit.value()));
    }
};

void initialize_sources(PublicationCoordinator &publisher, SourceAdmission &admission)
{
    auto       sources = std::make_shared<PublishedSources>();
    const auto catalog = admission.catalog();
    sources->floor     = catalog->floor;
    sources->tables.assign(catalog->tables.begin(), catalog->tables.end());
    auto page            = std::make_shared<PublishedPage>();
    page->leaf           = std::make_shared<PageState>();
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {
        .record = std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 0, .range = 0, .sequence = 0}),
        .pages  = {{.page_id = 0, .expected = {}, .replacement = page}},
        .root_page_id     = 0,
        .expected_sources = publisher.capture()->sources,
        .sources          = std::move(sources)};
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

struct OperationReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<void> *done;

    void set_value() const noexcept
    {
        done->set_value();
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
        done->set_value();
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "expected progress-bearing value";
        done->set_value();
    }
};

TEST(FlushOperation, CapturedWriterDrainAndSuccessorWritesCompleteAsOneAsyncFlow)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime    runtime(1, 8);
    auto                    lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>   contiguous{1};
    FlushCredits            credits;
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    EXPECT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "first")));
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &flush).ok());
    std::promise<void> done;
    auto               terminal  = done.get_future();
    auto               operation = stdexec::connect(flush->sender(), OperationReceiver{&done});
    stdexec::start(operation);
    runtime.drain();
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_EQ(publisher.capture()->sources->tables.size(), 2U);
    EXPECT_NE(publisher.capture()->flush_admission_owner, 0U);
    SourceAdmission::Writer successor;
    ASSERT_TRUE(sources.try_admit(1, &successor).ok());
    EXPECT_NE(successor.table(), writer.table());
    EXPECT_TRUE(successor.table()->upsert("b", 2, encode_cell_buf(2, OpKind::kPut, "next")));
    successor.release();
    contiguous.store(2);
    writer.release();
    terminal.get();
    runtime.drain();
    EXPECT_EQ(flush->result().frontier, 1U);
    EXPECT_EQ(flush->result().published_coverage, 1U);
    EXPECT_EQ(flush->result().phase, FlushPhase::kComplete);
    EXPECT_EQ(sources.catalog()->floor, 1U);
    EXPECT_EQ(publisher.capture()->sources->floor, 1U);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "first");
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("b").state, LogicalLookupState::kMissing);
    std::shared_ptr<FlushOperation> next;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &next).ok());
    EXPECT_NE(next, flush);
    ASSERT_TRUE(stdexec::sync_wait(next->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(next->result().published_coverage, 2U);
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("b").cell.value.to_string(), "next");
}

TEST(FlushOperation, IncompatibleSourceKeyLimitRejectsBeforePublicationAdmissionOrSourceClosure)
{
    FlushCredits           credits;
    SourceAdmission        sources(0, 1, {.account = credits.account, .max_key_size = 8});
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime  runtime(1, 8);
    auto                  lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t> contiguous{1};
    FlushBatchOptions     options;
    options.run_limits.max_key_bytes = 4;
    FlushController                 controller(publisher, runtime, sources, contiguous, lane, options);
    const auto                      before    = publisher.capture();
    const auto                      active    = sources.catalog()->active;
    auto                            metadata  = credits.reserve();
    const auto                      available = metadata->available();
    std::shared_ptr<FlushOperation> operation;
    EXPECT_EQ(controller.prepare(metadata, &operation).code(), Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    EXPECT_FALSE(active->closed());
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_EQ(metadata->available(), available);
}

TEST(FlushOperation, NoProgressReturnsCreditWithoutRotatingSourcesOrPublishing)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>           contiguous{0};
    FlushCredits                    credits;
    FlushController                 controller(publisher, runtime, sources, contiguous, lane);
    const auto                      before      = sources.catalog();
    const auto                      tree_before = publisher.capture();
    auto                            metadata    = credits.reserve();
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(metadata, &operation).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    EXPECT_EQ(operation->result().phase, FlushPhase::kComplete);
    EXPECT_EQ(operation->result().published_coverage, 0U);
    EXPECT_EQ(metadata->available(), 0U);
    EXPECT_EQ(sources.catalog(), before);
    EXPECT_FALSE(before->active->closed());
    EXPECT_EQ(publisher.capture(), tree_before);
}

TEST(FlushOperation, CoveredPersistentBatchRetainsClosedSourceTailForTheNextPrefix)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime runtime(1, 8);
    auto                 lane  = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 table = sources.catalog()->active;
    ASSERT_TRUE(table->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "covered")));
    ASSERT_TRUE(table->upsert("b", 2, encode_cell_buf(2, OpKind::kPut, "tail")));
    table->close();
    std::shared_ptr<FlushBatch> first;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, {}, {}, &first).ok());
    ASSERT_TRUE(stdexec::sync_wait(first->sender()).has_value());
    ASSERT_TRUE(first->result().handoff_status.ok()) << first->result().handoff_status.to_string();
    ASSERT_EQ(publisher.capture()->coverage, 1U);
    ASSERT_EQ(publisher.capture()->sources->tables.size(), 1U);
    auto inputs   = std::make_shared<PublishedSources>();
    inputs->floor = 1;
    auto acquire  = std::make_shared<PublicationRequest>();
    acquire->transaction.record =
        std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 2, .range = 0, .sequence = 0});
    acquire->transaction.expected_commit_seq   = publisher.capture()->commit_seq;
    acquire->transaction.flush_admission       = {.action = FlushAdmissionAction::Acquire, .owner = 2};
    acquire->transaction.flush_admission_owner = 2;
    acquire->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(acquire));
    ASSERT_EQ(publisher.capture()->flush_admission_owner, 2U);
    FlushBatchOptions persistent;
    persistent.persistent       = true;
    persistent.checkpoint_token = 1;
    std::shared_ptr<FlushBatch> checkpoint;
    EXPECT_EQ(FlushBatch::prepare(publisher, runtime, lane, 2, 2, persistent, {}, &checkpoint, inputs, {}, 2).code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(checkpoint);
    const auto prepared =
        FlushBatch::prepare(publisher, runtime, lane, 2, 1, persistent, {}, &checkpoint, inputs, {}, 2);
    ASSERT_TRUE(prepared.ok()) << prepared.to_string();
    ASSERT_TRUE(stdexec::sync_wait(checkpoint->sender()).has_value());
    ASSERT_TRUE(checkpoint->result().handoff_status.ok()) << checkpoint->result().handoff_status.to_string();
    EXPECT_EQ(publisher.capture()->coverage, 1U);
    ASSERT_EQ(publisher.capture()->sources->tables.size(), 1U);
    EXPECT_EQ(publisher.capture()->sources->tables.front(), table);
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "covered");
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("b").state, LogicalLookupState::kMissing);
    auto request = std::make_shared<PublicationRequest>();
    request->transaction.record =
        std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 2, .range = 0, .sequence = 1});
    request->transaction.checkpoint = {.action = CheckpointAction::Abort, .token = 1};
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
    std::shared_ptr<FlushBatch> next;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 3, 2, {}, {}, &next).ok());
    ASSERT_TRUE(stdexec::sync_wait(next->sender()).has_value());
    ASSERT_TRUE(next->result().handoff_status.ok()) << next->result().handoff_status.to_string();
    EXPECT_EQ(publisher.capture()->coverage, 2U);
    EXPECT_TRUE(publisher.capture()->sources->tables.empty());
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("b").cell.value.to_string(), "tail");
}

TEST(FlushOperation, MaintenanceRetryReusesBatchAfterCoverageAckAndMetadataSeal)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime    runtime(1, 8);
    auto                    lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>   contiguous{1};
    FlushCredits            credits;
    auto                    metadata = credits.reserve();
    size_t                  attempts = 0;
    FlushController         controller(publisher, runtime, sources, contiguous, lane, {}, [&](auto view, auto done) {
        EXPECT_EQ(sources.catalog()->floor, view->coverage);
        EXPECT_EQ(metadata->available(), 0U);
        ++attempts;
        done(attempts == 1 ? Status::io_error("controlled maintenance error") : Status::Ok(), 1);
    });
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    EXPECT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    std::weak_ptr<MemTable> old_source = writer.table();
    writer.release();
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(metadata, &operation).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(operation->result().handoff_status.ok());
    EXPECT_EQ(operation->result().maintenance_status.code(), Code::kIoError);
    EXPECT_EQ(operation->result().published_coverage, 1U);
    auto      *original = operation.get();
    const auto batch    = operation->result().batch_id;
    auto       covered  = publisher.capture();
    operation.reset();
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation).ok());
    EXPECT_EQ(operation.get(), original);
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(operation->result().batch_id, batch);
    EXPECT_TRUE(operation->result().maintenance_status.ok());
    EXPECT_EQ(attempts, 2U);
    EXPECT_EQ(publisher.capture(), covered);
    publisher.reclaim();
    sources.reclaim();
    EXPECT_TRUE(old_source.expired());
}

TEST(FlushOperation, CancellationDuringDrainKeepsRepairOwnerAndFixedFrontier)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime    runtime(1, 8);
    auto                    lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>   contiguous{1};
    FlushCredits            credits;
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    EXPECT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation).ok());
    std::promise<void> done;
    auto               terminal  = done.get_future();
    auto               connected = stdexec::connect(operation->sender(), OperationReceiver{&done});
    stdexec::start(connected);
    runtime.drain();
    operation->request_stop();
    writer.release();
    terminal.get();
    EXPECT_EQ(operation->result().phase, FlushPhase::kRepairRequired);
    EXPECT_EQ(operation->result().published_coverage, 0U);
    EXPECT_NE(publisher.capture()->flush_admission_owner, 0U);
    const auto batch = operation->result().batch_id;
    contiguous.store(10);
    std::shared_ptr<FlushOperation> retry;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &retry).ok());
    EXPECT_EQ(retry, operation);
    ASSERT_TRUE(stdexec::sync_wait(retry->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(retry->result().batch_id, batch);
    EXPECT_EQ(retry->result().frontier, 1U);
    EXPECT_EQ(retry->result().published_coverage, 1U);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
}

TEST(FlushOperation, StaleReservationFailsBeforeSourceClosureAndCanBeReprepared)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>           contiguous{1};
    FlushCredits                    credits;
    FlushController                 controller(publisher, runtime, sources, contiguous, lane);
    const auto                      before = sources.catalog();
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation, publisher.capture()->commit_seq - 1).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(operation->result().handoff_status.code(), Code::kUnavailable);
    EXPECT_EQ(sources.catalog(), before);
    EXPECT_FALSE(before->active->closed());
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    std::shared_ptr<FlushOperation> next;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &next).ok());
    EXPECT_NE(next, operation);
    ASSERT_TRUE(stdexec::sync_wait(next->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(next->result().handoff_status.ok()) << next->result().handoff_status.to_string();
    EXPECT_EQ(next->result().published_coverage, 1U);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
}

TEST(FlushOperation, CaptureAllocationFailureReleasesGateUsingIndependentCredit)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime           runtime(1, 8);
    auto                           lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>          contiguous{1};
    FlushCredits                   credits;
    auto                           metadata = credits.reserve();
    const auto                     before   = sources.catalog();
    std::optional<AllocationLease> exhausted;
    FlushBatchOptions              options;
    options.before_capture_for_tests = [&] {
        EXPECT_NE(publisher.capture()->flush_admission_owner, 0U);
        EXPECT_FALSE(before->active->closed());
        exhausted.emplace(metadata, metadata->available());
    };
    FlushController                 controller(publisher, runtime, sources, contiguous, lane, options);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(metadata, &operation).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    runtime.drain();
    EXPECT_EQ(operation->result().handoff_status.code(), Code::kResourceExhausted);
    EXPECT_EQ(sources.catalog(), before);
    EXPECT_FALSE(before->active->closed());
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    exhausted.reset();
}

TEST(FlushOperation, MissingPublishedSourceCatalogRejectsBeforeClosingActive)
{
    SourceAdmission                 sources;
    PublicationCoordinator          publisher;
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>           contiguous{1};
    FlushCredits                    credits;
    FlushController                 controller(publisher, runtime, sources, contiguous, lane);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation).ok());
    ASSERT_TRUE(stdexec::sync_wait(operation->sender()).has_value());
    EXPECT_EQ(operation->result().handoff_status.code(), Code::kUnavailable);
    EXPECT_FALSE(sources.catalog()->active->closed());
    EXPECT_EQ(sources.catalog()->tables.size(), 1U);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
}

TEST(FlushOperation, NoProgressCompletionRunsOnTheSharedCpuRuntime)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime            runtime(1, 8);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>           contiguous{0};
    FlushCredits                    credits;
    FlushController                 controller(publisher, runtime, sources, contiguous, lane);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation).ok());
    std::thread::id completion_thread;
    ASSERT_TRUE(stdexec::sync_wait(stdexec::then(operation->sender(), [&] {
                    completion_thread = std::this_thread::get_id();
                })).has_value());
    runtime.drain();
    EXPECT_NE(completion_thread, std::this_thread::get_id());
    EXPECT_GT(runtime.snapshot().submitted, 0U);
}

TEST(FlushOperation, ClosingIngressPreservesAlreadyCapturedWriterDrainAndCoverage)
{
    SourceAdmission        sources;
    PublicationCoordinator publisher;
    initialize_sources(publisher, sources);
    detail::FlushRuntime    runtime(1, 8);
    auto                    lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>   contiguous{1};
    FlushCredits            credits;
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(controller.prepare(credits.reserve(), &operation).ok());
    std::promise<void> done;
    auto               terminal  = done.get_future();
    auto               connected = stdexec::connect(operation->sender(), OperationReceiver{&done});
    stdexec::start(connected);
    runtime.drain();
    ASSERT_EQ(sources.catalog()->tables.size(), 2U);
    controller.close();
    sources.close();
    SourceAdmission::Writer rejected;
    EXPECT_EQ(sources.try_admit(1, &rejected).code(), Code::kUnavailable);
    writer.release();
    terminal.get();
    runtime.drain();
    EXPECT_TRUE(operation->result().handoff_status.ok());
    EXPECT_EQ(operation->result().published_coverage, 1U);
    EXPECT_EQ(sources.catalog()->floor, 1U);
    EXPECT_EQ(publisher.capture()->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), "value");
}
} // namespace
} // namespace crowdb::tree
