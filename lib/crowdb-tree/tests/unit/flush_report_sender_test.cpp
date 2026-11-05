// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_progress.h"
#include "btree/flush_operation.h"

#include <gtest/gtest.h>

#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
class FlushReportTest : public testing::Test
{
  protected:
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(4 * 1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    SourceAdmission                                  sources;
    PublicationCoordinator                           publisher;
    detail::FlushRuntime                             runtime{2, 8};
    std::shared_ptr<detail::FlushRuntime::Lane>      lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::atomic<uint64_t>                            contiguous{1};

    void SetUp() override
    {
        auto       inputs  = std::make_shared<PublishedSources>();
        const auto catalog = sources.catalog();
        inputs->floor      = catalog->floor;
        inputs->tables.assign(catalog->tables.begin(), catalog->tables.end());
        auto page            = std::make_shared<PublishedPage>();
        page->leaf           = std::make_shared<PageState>();
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = {.record           = std::make_shared<InstallRecord>(InstallToken{.generation = 1}),
                                .pages            = {{.page_id = 0, .expected = {}, .replacement = page}},
                                .root_page_id     = 0,
                                .expected_sources = publisher.capture()->sources,
                                .sources          = std::move(inputs)};
        request->on_done     = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
        ASSERT_TRUE(publisher.try_submit(request));
    }

    void TearDown() override
    {
        runtime.drain();
    }

    std::shared_ptr<detail::AllocationReservation> reserve()
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 1024 * 1024);
        if (!credit) {
            ADD_FAILURE() << "test reservation failed";
            return {};
        }
        return std::make_shared<detail::AllocationReservation>(std::move(credit.value()));
    }
};

struct ReportReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<FlushResult> *done;

    void set_value(FlushResult report) const noexcept
    {
        done->set_value(std::move(report));
    }

    void set_error(Status status) const noexcept
    {
        FlushResult report;
        report.handoff_status = std::move(status);
        done->set_value(std::move(report));
    }

    void set_stopped() const noexcept
    {
        set_error(Status::unavailable());
    }
};

TEST_F(FlushReportTest, ConcurrentConnectionRejectsWithoutObservingActiveProgress)
{
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    std::promise<FlushResult> accepted;
    auto                      finished = accepted.get_future();
    auto                      first    = stdexec::connect(flush->report_sender(), ReportReceiver{&accepted});
    stdexec::start(first);
    runtime.drain();
    const auto waiting_metrics = publisher.work_metrics()->totals();
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushCapture)].count, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushWriterDrain)].inflight, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushRange)].attempts, 0U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushBatch)].attempts, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushBatch)].inflight, 1U);
    EXPECT_EQ(waiting_metrics[static_cast<size_t>(TreeStage::FlushBatch)].count, 0U);
    std::promise<FlushResult> rejected;
    auto                      rejected_result = rejected.get_future();
    auto                      second          = stdexec::connect(flush->report_sender(), ReportReceiver{&rejected});
    stdexec::start(second);
    const auto report = rejected_result.get();
    EXPECT_EQ(report.handoff_status.code(), Code::kUnavailable);
    EXPECT_EQ(report.frontier, 0U);
    EXPECT_FALSE(report.handoff_advanced);
    writer.release();
    const auto completed = finished.get();
    EXPECT_EQ(completed.frontier, 1U);
    EXPECT_TRUE(completed.handoff_status.ok());
    EXPECT_TRUE(completed.handoff_advanced);
    const auto completed_metrics = publisher.work_metrics()->totals();
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushBatch)].attempts, 1U);
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushBatch)].count, 1U);
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushBatch)].inflight, 0U);
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushWriterDrain)].count, 1U);
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushWriterDrain)].inflight, 0U);
    EXPECT_EQ(completed_metrics[static_cast<size_t>(TreeStage::FlushRange)].count, 1U);
}

struct PausedReportReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::atomic<bool>         *entered;
    std::atomic<bool>         *release;
    std::promise<FlushResult> *done;

    void set_value(FlushResult report) const noexcept
    {
        entered->store(true, std::memory_order_release);
        entered->notify_one();
        while (!release->load(std::memory_order_acquire)) {
            release->wait(false);
        }
        done->set_value(std::move(report));
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
        FlushResult rejected;
        rejected.handoff_status = std::move(status);
        set_value(std::move(rejected));
    }

    void set_stopped() const noexcept
    {
        set_error(Status::unavailable());
    }
};

TEST_F(FlushReportTest, ReconnectCannotOverwriteAnUndeliveredTerminalValue)
{
    size_t                  attempts = 0;
    FlushController         controller(publisher, runtime, sources, contiguous, lane, {}, [&](auto, auto complete) {
        ++attempts;
        complete(attempts == 1 ? Status::io_error() : Status::Ok(), attempts == 1 ? 7 : 0);
    });
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    writer.release();
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    std::promise<FlushResult> accepted;
    auto                      finished = accepted.get_future();
    std::atomic<bool>         entered{false};
    std::atomic<bool>         release{false};
    auto first = stdexec::connect(flush->report_sender(),
                                  PausedReportReceiver{.entered = &entered, .release = &release, .done = &accepted});
    stdexec::start(first);
    while (!entered.load(std::memory_order_acquire)) {
        entered.wait(false);
    }

    struct ReleaseOnExit
    {
        std::atomic<bool> &gate;

        ~ReleaseOnExit()
        {
            gate.store(true, std::memory_order_release);
            gate.notify_one();
        }
    } release_on_exit{release};

    auto retry = stdexec::sync_wait(flush->report_sender());
    release.store(true, std::memory_order_release);
    release.notify_one();
    const auto original = finished.get();
    runtime.drain();
    if (!retry.has_value()) {
        FAIL() << "retry stopped without a progress report";
    }
    const auto &retried = std::get<0>(retry.value());
    EXPECT_TRUE(retried.maintenance_status.ok());
    EXPECT_EQ(retried.deferred_pages, 0U);
    EXPECT_FALSE(retried.handoff_advanced);
    EXPECT_EQ(original.maintenance_status.code(), Code::kIoError);
    EXPECT_EQ(original.deferred_pages, 7U);
    EXPECT_TRUE(original.handoff_advanced);
    EXPECT_EQ(original.batch_id, retried.batch_id);
    EXPECT_EQ(original.frontier, retried.frontier);
}

TEST_F(FlushReportTest, NoProgressDoesNotCountAHandoff)
{
    contiguous.store(0);
    FlushController                 controller(publisher, runtime, sources, contiguous, lane);
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    auto completed = stdexec::sync_wait(flush->report_sender());
    runtime.drain();
    if (!completed.has_value()) {
        FAIL() << "flush stopped without a progress report";
    }
    const auto &report = std::get<0>(completed.value());
    EXPECT_EQ(report.phase, FlushPhase::kComplete);
    EXPECT_FALSE(report.handoff_advanced);
    EXPECT_EQ(report.frontier, report.floor);
}

TEST_F(FlushReportTest, CaptureDrainsWritersWithoutCoverageAndNormalSenderResumesTheSamePrefix)
{
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "inherited")));
    const auto                      original = writer.table();
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    std::promise<FlushResult> terminal;
    auto                      future  = terminal.get_future();
    auto                      capture = stdexec::connect(flush->capture_sender(), ReportReceiver{&terminal});
    stdexec::start(capture);
    runtime.drain();
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    writer.release();
    const auto report = future.get();
    ASSERT_TRUE(report.handoff_status.ok());
    EXPECT_EQ(report.phase, FlushPhase::kCaptured);
    EXPECT_EQ(report.frontier, 1U);
    EXPECT_EQ(report.published_coverage, 0U);
    EXPECT_FALSE(report.handoff_advanced);
    const auto frozen = flush->frozen_sources();
    ASSERT_TRUE(frozen.catalog);
    ASSERT_TRUE(frozen.base);
    EXPECT_EQ(frozen.base->coverage, frozen.catalog->floor);
    ASSERT_EQ(frozen.catalog->tables.size(), 1U);
    EXPECT_EQ(frozen.catalog->tables.front(), original);
    EXPECT_TRUE(original->closed());
    EXPECT_EQ(original->writers(), 0U);

    SourceAdmission::Writer successor;
    ASSERT_TRUE(sources.try_admit(1, &successor).ok());
    EXPECT_NE(successor.table(), original);
    ASSERT_TRUE(successor.table()->upsert("a", 2, encode_cell_buf(2, OpKind::kPut, "parent-later")));
    successor.release();
    contiguous.store(2);
    const auto resumed = stdexec::sync_wait(flush->report_sender());
    if (!resumed) {
        FAIL() << "flush sender stopped";
    }
    EXPECT_TRUE(std::get<0>(*resumed).handoff_status.ok());
    EXPECT_TRUE(std::get<0>(*resumed).handoff_advanced);
    EXPECT_EQ(std::get<0>(*resumed).published_coverage, 1U);
    EXPECT_EQ(publisher.capture()->coverage, 1U);
    EXPECT_EQ(flush->frozen_sources().catalog, frozen.catalog);
    EXPECT_EQ(flush->frozen_sources().base, frozen.base);
    auto guard  = original->read_guard();
    auto cursor = original->prefix_cursor(1);
    ASSERT_TRUE(cursor.valid());
    EXPECT_EQ(CellView(cursor.cell_version()->cell.slice()).value().to_string(), "inherited");
}

TEST_F(FlushReportTest, RepeatedCaptureHasTheSameFrozenIdentityAndDoesNotPublish)
{
    FlushController         controller(publisher, runtime, sources, contiguous, lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "value")));
    writer.release();
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    auto first = stdexec::sync_wait(flush->capture_sender());
    if (!first) {
        FAIL() << "flush sender stopped";
    }
    ASSERT_TRUE(std::get<0>(*first).handoff_status.ok());
    const auto frozen  = flush->frozen_sources();
    const auto version = publisher.capture()->commit_seq;
    auto       second  = stdexec::sync_wait(flush->capture_sender());
    if (!second) {
        FAIL() << "flush sender stopped";
    }
    EXPECT_TRUE(std::get<0>(*second).handoff_status.ok());
    EXPECT_EQ(publisher.capture()->commit_seq, version);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_EQ(flush->frozen_sources().catalog, frozen.catalog);
    EXPECT_EQ(flush->frozen_sources().base, frozen.base);
    EXPECT_EQ(flush->frozen_sources().frontier, 1U);
    EXPECT_EQ(flush->frozen_sources().batch_id, frozen.batch_id);
}

struct ProgressReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::atomic<size_t> *count;

    void set_value() const noexcept
    {
        count->fetch_add(1, std::memory_order_release);
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "unexpected stopped progress waiter";
    }
};

TEST_F(FlushReportTest, ProgressWakeupsDistinguishSuccessorCaptureAndHandoff)
{
    std::shared_ptr<ApplyProgress> progress;
    ASSERT_TRUE(ApplyProgress::create(1, reserve(), &progress).ok());
    FlushController         controller(publisher, runtime, sources, contiguous, lane, {}, {}, {}, progress);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(sources.try_admit(0, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "v")));
    std::shared_ptr<FlushOperation> flush;
    ASSERT_TRUE(controller.prepare(reserve(), &flush).ok());
    std::atomic<size_t>    successor_ready{0};
    ApplyProgress::Request successor(*progress, progress->sequence());
    auto                   successor_wait = stdexec::connect(successor.sender(), ProgressReceiver{&successor_ready});
    stdexec::start(successor_wait);
    std::promise<FlushResult> terminal;
    auto                      future  = terminal.get_future();
    auto                      capture = stdexec::connect(flush->capture_sender(), ReportReceiver{&terminal});
    stdexec::start(capture);
    runtime.drain();
    EXPECT_EQ(successor_ready.load(std::memory_order_acquire), 1U);
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    std::atomic<size_t>    capture_ready{0};
    ApplyProgress::Request capture_done(*progress, progress->sequence());
    auto                   capture_wait = stdexec::connect(capture_done.sender(), ProgressReceiver{&capture_ready});
    stdexec::start(capture_wait);
    writer.release();
    const auto captured = future.get();
    EXPECT_EQ(captured.phase, FlushPhase::kCaptured);
    EXPECT_FALSE(captured.handoff_advanced);
    EXPECT_EQ(capture_ready.load(std::memory_order_acquire), 1U);
    std::atomic<size_t>    handoff_ready{0};
    ApplyProgress::Request handoff_done(*progress, progress->sequence());
    auto                   handoff_wait = stdexec::connect(handoff_done.sender(), ProgressReceiver{&handoff_ready});
    stdexec::start(handoff_wait);
    const auto completed = stdexec::sync_wait(flush->report_sender());
    if (!completed) {
        FAIL() << "flush sender stopped";
    }
    EXPECT_TRUE(std::get<0>(completed.value()).handoff_advanced);
    EXPECT_EQ(handoff_ready.load(std::memory_order_acquire), 1U);
    EXPECT_EQ(publisher.capture()->coverage, 1U);
}
} // namespace
} // namespace crowdb::tree
