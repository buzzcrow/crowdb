// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_admission.h"

#include <gtest/gtest.h>

#include <thread>

namespace crowdb::tree
{
namespace
{
struct Credits
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(1024 * 1024);
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 2};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 65536) const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, bytes);
        if (!credit.has_value()) {
            ADD_FAILURE() << "test metadata reservation failed";
            return {};
        }
        return std::make_shared<detail::AllocationReservation>(std::move(credit.value()));
    }
};

struct DrainResult
{
    std::atomic<size_t> values{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> stopped{0};
};

struct DrainReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    DrainResult *result;

    void set_value() const noexcept
    {
        result->values.fetch_add(1);
    }

    void set_error(Status /*unused*/) const noexcept
    {
        result->errors.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        result->stopped.fetch_add(1);
    }
};

void count_notification(void *context) noexcept
{
    static_cast<std::atomic<size_t> *>(context)->fetch_add(1);
}

TEST(MemTableFrozenNotification, CloseAndLastWriterNotifyEachSubscriberOnce)
{
    MemTable table;
    ASSERT_TRUE(table.try_enter());
    ASSERT_TRUE(table.try_enter());
    std::atomic<size_t>    notifications{0};
    MemTable::FrozenWaiter first(&count_notification, &notifications);
    MemTable::FrozenWaiter second(&count_notification, &notifications);
    ASSERT_TRUE(table.notify_when_frozen(first));
    table.close();
    ASSERT_TRUE(table.notify_when_frozen(second));
    EXPECT_EQ(notifications.load(), 0U);
    table.leave();
    EXPECT_EQ(notifications.load(), 0U);
    table.leave();
    EXPECT_EQ(notifications.load(), 2U);
    EXPECT_FALSE(table.notify_when_frozen(first));
    table.close();
    EXPECT_EQ(notifications.load(), 2U);
    MemTable::FrozenWaiter late(&count_notification, &notifications);
    EXPECT_TRUE(table.notify_when_frozen(late));
    EXPECT_EQ(notifications.load(), 3U);
}

TEST(MemTableFrozenNotification, ConcurrentRegistrationAndLastWriterCannotLoseNotification)
{
    for (size_t attempt = 0; attempt < 100; ++attempt) {
        MemTable table;
        ASSERT_TRUE(table.try_enter());
        table.close();
        std::atomic<size_t>    notifications{0};
        MemTable::FrozenWaiter waiter(&count_notification, &notifications);
        std::thread            subscriber([&] { EXPECT_TRUE(table.notify_when_frozen(waiter)); });
        table.leave();
        subscriber.join();
        EXPECT_EQ(notifications.load(), 1U);
    }
}

TEST(SourceAdmission, NoProgressOrMissingCreditLeavesActiveOpen)
{
    SourceAdmission                admission(5);
    Credits                        credits;
    auto                           initial = admission.catalog();
    std::atomic<uint64_t>          contiguous{5};
    std::shared_ptr<SourceCapture> capture;
    EXPECT_EQ(admission.try_capture(contiguous, {}, &capture).code(), Code::kInvalidArgument);
    EXPECT_EQ(admission.try_capture(contiguous, credits.reserve(), &capture).code(), Code::kUnavailable);
    contiguous.store(4);
    EXPECT_EQ(admission.try_capture(contiguous, credits.reserve(), &capture).code(), Code::kUnavailable);
    EXPECT_EQ(admission.catalog(), initial);
    EXPECT_FALSE(initial->active->closed());
    EXPECT_FALSE(capture);
    SourceAdmission::Writer writer;
    EXPECT_TRUE(admission.try_admit(5, &writer).ok());
    EXPECT_EQ(writer.table(), initial->active);
}

TEST(SourceAdmission, CapacityPauseSurvivesSuccessorPublicationAndKeepsGapAdmission)
{
    Credits                credits;
    SourceWriteOptions     options{.account = credits.account, .max_entries = 8};
    SourceAdmission        admission(0, 1, options);
    SourceWriteReservation prepaid;
    ASSERT_TRUE(
        SourceWriteReservation::create(options, {.entries = 1}, detail::BudgetClass::kAdmittedWriter, &prepaid).ok());
    admission.pause_capacity();
    SourceAdmission::Writer writer;
    EXPECT_EQ(admission.try_admit(0, &writer).code(), Code::kUnavailable);
    EXPECT_EQ(admission.try_admit_reserved(0, &prepaid, &writer).code(), Code::kUnavailable);
    EXPECT_TRUE(prepaid.valid());
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &capture).ok());
    ASSERT_TRUE(admission.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    EXPECT_EQ(admission.try_admit(1, &writer).code(), Code::kUnavailable);
    ASSERT_TRUE(admission.try_admit(1, WriteFootprint{.entries = 1}, &writer, detail::BudgetClass::kGapRepair).ok());
    writer.release();
    ASSERT_TRUE(admission.finish_covered(capture, capture->batch_id(), 1).ok());
    EXPECT_EQ(admission.try_admit(1, &writer).code(), Code::kUnavailable);
    admission.resume_capacity();
    EXPECT_TRUE(admission.try_admit_reserved(1, &prepaid, &writer).ok());
    EXPECT_FALSE(prepaid.valid());
}

TEST(SourceAdmission, MetadataFailurePrecedesClosure)
{
    SourceAdmission                admission;
    Credits                        credits;
    auto                           initial = admission.catalog();
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    EXPECT_EQ(admission.try_capture(contiguous, credits.reserve(1), &capture).code(), Code::kResourceExhausted);
    EXPECT_FALSE(capture);
    EXPECT_EQ(admission.catalog(), initial);
    EXPECT_FALSE(initial->active->closed());
}

TEST(SourceAdmission, SuccessorNeedsPublicationAndCannotWidenCapturedFrontier)
{
    SourceAdmission         admission;
    Credits                 credits;
    SourceAdmission::Writer old_writer;
    ASSERT_TRUE(admission.try_admit(1, &old_writer).ok());
    EXPECT_TRUE(old_writer.table()->upsert("a", 1, encode_cell_buf(1, OpKind::kPut, "old")));
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &capture).ok());
    EXPECT_EQ(capture->floor(), 0U);
    EXPECT_EQ(capture->frontier(), 1U);
    EXPECT_EQ(capture->expected_catalog()->active, old_writer.table());
    ASSERT_EQ(capture->next_catalog()->tables.size(), 2U);
    SourceAdmission::Writer successor_writer;
    EXPECT_EQ(admission.try_admit(1, &successor_writer).code(), Code::kUnavailable);
    EXPECT_FALSE(admission.activate_successor(capture, capture->batch_id() + 1).ok());
    ASSERT_TRUE(admission.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(admission.try_admit(1, &successor_writer).ok());
    EXPECT_EQ(successor_writer.table(), capture->successor());
    EXPECT_TRUE(successor_writer.table()->upsert("b", 2, encode_cell_buf(2, OpKind::kPut, "successor")));
    successor_writer.release();
    contiguous.store(2);
    DrainResult result;
    auto        operation = stdexec::connect(capture->drain_sender(), DrainReceiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(result.values.load(), 0U);
    EXPECT_FALSE(admission.finish_covered(capture, capture->batch_id(), 1).ok());
    EXPECT_TRUE(old_writer.table()->upsert("future", 7, encode_cell_buf(7, OpKind::kPut, "future")));
    old_writer.release();
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_TRUE(capture->drained());
    EXPECT_EQ(capture->frontier(), 1U);
    ASSERT_TRUE(admission.finish_covered(capture, capture->batch_id(), 1).ok());
    EXPECT_EQ(admission.catalog()->floor, 1U);
    EXPECT_EQ(admission.catalog()->tables.size(), 2U);
    EXPECT_EQ(capture->phase(), SourceCapturePhase::kCovered);
}

TEST(SourceAdmission, RepairReturnsSameCaptureAndRejectsUnrelatedCoverage)
{
    SourceAdmission                admission;
    Credits                        credits;
    std::atomic<uint64_t>          contiguous{3};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &capture).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    ASSERT_TRUE(admission.activate_successor(capture, capture->batch_id()).ok());
    ASSERT_TRUE(admission.mark_repair(capture, capture->batch_id(), 0).ok());
    contiguous.store(99);
    std::shared_ptr<SourceCapture> retried;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &retried).ok());
    EXPECT_EQ(retried, capture);
    EXPECT_EQ(retried->frontier(), 3U);
    EXPECT_FALSE(admission.mark_repair(capture, capture->batch_id() + 1, 0).ok());
    EXPECT_FALSE(admission.mark_repair(capture, capture->batch_id(), 1).ok());
    EXPECT_FALSE(admission.finish_covered(capture, capture->batch_id(), 99).ok());
    EXPECT_EQ(admission.catalog()->floor, 0U);
    ASSERT_TRUE(admission.finish_covered(capture, capture->batch_id(), 3).ok());
    EXPECT_FALSE(admission.finish_covered(capture, capture->batch_id(), 3).ok());
    EXPECT_FALSE(admission.mark_repair(capture, capture->batch_id(), 0).ok());
}

TEST(SourceAdmission, CoveredAcknowledgementUsesPreallocatedStateAfterCreditSeals)
{
    SourceAdmission                admission;
    Credits                        credits;
    auto                           reservation = credits.reserve();
    auto                           before      = admission.catalog();
    std::atomic<uint64_t>          contiguous{2};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, reservation, &capture).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    ASSERT_TRUE(admission.activate_successor(capture, capture->batch_id()).ok());
    reservation->seal();
    EXPECT_EQ(reservation->available(), 0U);
    ASSERT_TRUE(admission.finish_covered(capture, capture->batch_id(), 2).ok());
    EXPECT_EQ(admission.catalog()->floor, 2U);
    ASSERT_EQ(admission.catalog()->tables.size(), 1U);
    EXPECT_EQ(admission.catalog()->active->durable_floor(), 2U);
    EXPECT_EQ(before->floor, 0U);
    EXPECT_EQ(before->tables.size(), 1U);
    EXPECT_TRUE(before->active->closed());
}

TEST(SourceAdmission, CancellationDrainsWritersAndRetryKeepsSources)
{
    SourceAdmission         admission;
    Credits                 credits;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(admission.try_admit(1, &writer).ok());
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &capture).ok());
    capture->request_stop();
    DrainResult result;
    auto        operation = stdexec::connect(capture->drain_sender(), DrainReceiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(result.stopped.load(), 0U);
    writer.release();
    EXPECT_EQ(result.stopped.load(), 1U);
    EXPECT_EQ(admission.catalog()->floor, 0U);
    EXPECT_EQ(admission.catalog()->tables.size(), 1U);
    ASSERT_TRUE(admission.mark_repair(capture, capture->batch_id(), 0).ok());
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    EXPECT_EQ(capture->phase(), SourceCapturePhase::kRepairRequired);
}

TEST(SourceAdmission, AcceptedCaptureActivatesAndAcknowledgesAfterIngressCloses)
{
    SourceAdmission         admission;
    Credits                 credits;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(admission.try_admit(0, &writer).ok());
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission.try_capture(contiguous, credits.reserve(), &capture).ok());
    admission.close();
    SourceAdmission::Writer rejected;
    EXPECT_EQ(admission.try_admit(1, &rejected).code(), Code::kUnavailable);
    ASSERT_TRUE(admission.activate_successor(capture, capture->batch_id()).ok());
    writer.release();
    ASSERT_TRUE(stdexec::sync_wait(capture->drain_sender()).has_value());
    ASSERT_TRUE(admission.finish_covered(capture, capture->batch_id(), 1).ok());
    EXPECT_EQ(admission.catalog()->floor, 1U);
    EXPECT_EQ(admission.try_admit(1, &rejected).code(), Code::kUnavailable);
}

TEST(SourceAdmission, AsyncDrainOwnsSourcesAfterManagerAndCaptureAreReleased)
{
    auto                    admission = std::make_unique<SourceAdmission>();
    Credits                 credits;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(admission->try_admit(1, &writer).ok());
    std::atomic<uint64_t>          contiguous{1};
    std::shared_ptr<SourceCapture> capture;
    ASSERT_TRUE(admission->try_capture(contiguous, credits.reserve(), &capture).ok());
    DrainResult result;
    auto        operation = stdexec::connect(capture->drain_sender(), DrainReceiver{&result});
    stdexec::start(operation);
    std::weak_ptr<MemTable> source = writer.table();
    capture.reset();
    admission.reset();
    EXPECT_FALSE(source.expired());
    writer.release();
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_TRUE(source.expired());
}
} // namespace
} // namespace crowdb::tree
