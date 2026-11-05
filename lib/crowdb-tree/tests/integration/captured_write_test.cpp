// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "captured_write_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

TEST(CapturedWrite, OverflowAndPrimaryUseOneWindowAndKeepProtectedReferencesAfterInputsEnd)
{
    TestCapturedWrite test;
    const std::string value(16000, 'v');
    auto              captured = captured_leaf(page({
        {.key = "a", .value = value}
    }));
    ASSERT_TRUE(test.prepare(captured).ok());
    test.store->deferred = true;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    size_t completed = 0;
    size_t peak      = 0;
    while (done.values.load() + done.errors.load() == 0 && completed < 100) {
        ASSERT_TRUE(test.store->pending);
        EXPECT_EQ(test.store->writes, completed + 1);
        peak = std::max(peak, test.credits.budget.snapshot().allocated[LoadCredits::kBytes]);
        test.store->finish();
        ++completed;
        test.runtime->drain();
    }
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    auto result = test.operation->result();
    ASSERT_TRUE(result.pages);
    EXPECT_EQ(result.pages->coverage, 10U);
    EXPECT_EQ(result.written_frames, result.pages->frames.size());
    EXPECT_GT(result.written_frames, 40U);
    EXPECT_EQ(result.written_bytes, result.written_frames * 4096);
    EXPECT_LT(peak, result.written_bytes);
    RecordProperty("allocated_peak", peak);
    RecordProperty("written_bytes", result.written_bytes);
    test.operation.reset();
    captured.reset();
    const auto     raw = test.read(result.pages->frames.at(0));
    LeafFrameView  leaf(raw.data(), raw.size());
    const CellView cell(leaf.cell(0));
    std::string    restored;
    uint64_t       pid = cell.overflow_head();
    while (pid != kInvalidPageId) {
        const auto        bytes = test.read(result.pages->frames.at(pid));
        OverflowFrameView chunk(bytes.data(), bytes.size());
        restored += chunk.payload().to_string();
        pid = chunk.next_page_id();
    }
    EXPECT_EQ(restored, value);
    EXPECT_EQ(test.ledger->stats().protected_ranges, result.written_frames);
    result = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(CapturedWrite, CancellationWaitsForPendingWriteAndDoesNotPublishPartialMapping)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = std::string(2000, 'v')}
    })))
                    .ok());
    test.store->deferred = true;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    test.operation->request_stop();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 0U);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 1U);
    test.store->finish();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.operation->result().pages);
    EXPECT_EQ(test.store->writes, 1U);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
}

TEST(CapturedWrite, ClosedRuntimeDrainsPendingWriteBeforeReportingDispatchFailure)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = "v"}
    })))
                    .ok());
    test.store->deferred = true;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    test.runtime->close();
    test.store->finish();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(test.operation->result().pages);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
}

TEST(CapturedWrite, UnknownWriteSubmissionKeepsPhysicalProtectionUntilLateCompletion)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = "v"}
    })))
                    .ok());
    test.store->throw_after_capture = true;
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(test.operation->result().pages);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 1U);
    test.store->finish();
    test.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
}

TEST(CapturedWrite, FrameLimitFailsAndReleasesAllCompletedOrphanWrites)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = std::string(2000, 'v')}
    })),
                             {},
                             {.frames     = {.encode = {.frame_bytes = 512, .inline_value_bytes = 16}},
                              .write      = {.max_frame_bytes = 512},
                              .max_frames = 2})
                    .ok());
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.operation->result().pages);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
}

TEST(CapturedWrite, ReusesOnlyTheCapturedOverflowChainAndReleasesTheOldPrimary)
{
    TestCapturedWrite test;
    const std::string value(3000, 'r');
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = value}
    })))
                    .ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto old = test.operation->result().pages;
    ASSERT_TRUE(old);
    const auto raw  = test.read(old->frames.at(0));
    auto       leaf = std::make_shared<PageState>();
    leaf->bases.push_back(
        {.identity   = 1,
         .generation = 1,
         .max_slot   = 10,
         .resident   = std::shared_ptr<const LeafBase>(LeafBase::from_frame_copy(raw.data(), raw.size())),
         .backing    = old->frames.at(0).backing});
    auto next          = captured_leaf(leaf);
    next->next_page_id = old->next_page_id;
    test.operation.reset();
    ASSERT_TRUE(test.prepare(next, old).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U) << second.status.to_string();
    auto result = test.operation->result();
    ASSERT_TRUE(result.pages);
    EXPECT_EQ(result.written_frames, 1U);
    EXPECT_EQ(result.reused_frames, old->frames.size() - 1);
    for (const auto &[pid, frame] : old->frames) {
        if (pid != 0) {
            const auto &adopted = result.pages->frames.at(pid).backing;
            EXPECT_NE(frame.backing, adopted);
            EXPECT_EQ(frame.backing->physical_owner, adopted->physical_owner);
            EXPECT_EQ(frame.backing->address, adopted->address);
            EXPECT_EQ(frame.backing->length, adopted->length);
        }
    }
    const auto expected = result.pages->frames.size();
    test.operation.reset();
    old.reset();
    next.reset();
    leaf.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, expected);
    const CellView cell(LeafFrameView(raw.data(), raw.size()).cell(0));
    std::string    restored;
    for (auto pid = cell.overflow_head(); pid != kInvalidPageId;) {
        const auto        bytes = test.read(result.pages->frames.at(pid));
        OverflowFrameView chunk(bytes.data(), bytes.size());
        restored += chunk.payload().to_string();
        pid = chunk.next_page_id();
    }
    EXPECT_EQ(restored, value);
    result = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(CapturedWrite, MissingOrCyclicInheritedOverflowCannotBecomeSuccessfulOutput)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = std::string(1000, 'r')}
    })))
                    .ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto           old = test.operation->result().pages;
    const auto     raw = test.read(old->frames.at(0));
    const CellView cell(LeafFrameView(raw.data(), raw.size()).cell(0));
    for (bool typed : {false, true}) {
        for (bool cycle : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "typed=" << typed << " cycle=" << cycle);
            auto bad = std::make_shared<CapturedDurablePages>(nullptr);
            bad->frames.insert(old->frames.begin(), old->frames.end());
            auto backing = std::make_shared<BackingRef>(*old->frames.at(0).backing);
            backing->overflow.reset();
            if (typed) {
                auto dependencies   = std::make_shared<OverflowBackingSet>(nullptr);
                dependencies->pages = old->frames.at(0).backing->overflow->pages;
                if (cycle) {
                    dependencies->pages.at(cell.overflow_head()).next = cell.overflow_head();
                }
                else {
                    dependencies->pages.erase(cell.overflow_head());
                }
                backing->overflow = std::move(dependencies);
            }
            else if (cycle) {
                bad->frames.at(cell.overflow_head()).next_overflow = cell.overflow_head();
            }
            else {
                bad->frames.erase(cell.overflow_head());
            }
            auto leaf = std::make_shared<PageState>();
            leaf->bases.push_back(
                {.identity = 1,
                 .max_slot = 10,
                 .resident = std::shared_ptr<const LeafBase>(LeafBase::from_frame_copy(raw.data(), raw.size())),
                 .backing  = std::move(backing)});
            auto next          = captured_leaf(leaf);
            next->next_page_id = old->next_page_id;
            test.operation.reset();
            const auto writes = test.store->writes;
            ASSERT_TRUE(test.prepare(next, bad).ok());
            Completion done;
            test.run(&done);
            EXPECT_EQ(done.errors.load(), 1U);
            EXPECT_EQ(done.status.code(), Code::kCorruption);
            EXPECT_FALSE(test.operation->result().pages);
            EXPECT_EQ(test.store->writes, writes);
        }
    }
}

TEST(CapturedWrite, EmptyCaptureCompletesWithoutReservingAnExtent)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(std::make_shared<PublishedTreeState>()).ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U);
    ASSERT_TRUE(test.operation->result().pages);
    EXPECT_TRUE(test.operation->result().pages->frames.empty());
    EXPECT_EQ(test.store->writes, 0U);
}

TEST(CapturedWrite, MetadataExhaustionAfterAcceptedIoReleasesThePartialImage)
{
    TestCapturedWrite test;
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = std::string(2000, 'v')}
    })))
                    .ok());
    test.store->deferred = true;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    test.credits.credits.bytes->seal();
    test.store->finish();
    test.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.operation->result().pages);
    EXPECT_EQ(test.store->writes, 1U);
    test.operation.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}
} // namespace crowdb::tree
