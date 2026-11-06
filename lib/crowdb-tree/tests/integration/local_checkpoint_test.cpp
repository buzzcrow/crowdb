// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_checkpoint_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

TEST(LocalCheckpoint, PersistsExistingFormatAndAdvancesOnlyAfterSecondBarrier)
{
    TestCheckpoint test;
    auto           pages = test.materialize(std::string(3000, 'v'));
    ASSERT_TRUE(test.prepare(pages).ok());
    test.store->defer_at = 5;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.source.runtime->drain();
    EXPECT_EQ(test.store->events, (std::vector<std::string>{"metadata", "metadata", "barrier", "anchor", "barrier"}));
    EXPECT_EQ(done.values.load(), 0U);
    EXPECT_EQ(test.registry->durable_coverage(), 0U);
    test.store->finish();
    test.source.runtime->drain();
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(test.registry->durable_coverage(), 10U);
    EXPECT_EQ(test.registry->snapshot_sequence(), 1U);
    EXPECT_FALSE(test.registry->needs_recovery());
    test.expect_read(std::string(3000, 'v'));
}

TEST(LocalCheckpoint, UnchangedCheckpointDoesNotWriteOrAdvanceSequence)
{
    TestCheckpoint test;
    auto           pages = test.materialize();
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto       original   = test.operation->result().checkpoint;
    const auto events     = test.store->events.size();
    const auto high_water = test.source.ledger->stats().address_high_water;
    ASSERT_TRUE(test.prepare(std::make_shared<CapturedDurablePages>(*pages)).ok());
    Completion unchanged;
    test.run(&unchanged);
    ASSERT_EQ(unchanged.values.load(), 1U) << unchanged.status.to_string();
    EXPECT_EQ(test.operation->result().checkpoint, original);
    EXPECT_EQ(test.store->events.size(), events);
    EXPECT_EQ(test.registry->snapshot_sequence(), 1U);
    EXPECT_EQ(test.source.ledger->stats().address_high_water, high_water);
    auto advanced = std::make_shared<CapturedDurablePages>(*pages);
    ++advanced->coverage;
    ASSERT_TRUE(test.prepare(advanced).ok());
    Completion coverage;
    test.run(&coverage);
    ASSERT_EQ(coverage.values.load(), 1U) << coverage.status.to_string();
    EXPECT_EQ(test.registry->snapshot_sequence(), 2U);
    EXPECT_EQ(test.registry->durable_coverage(), advanced->coverage);
    EXPECT_EQ(test.store->events.size(), events + 3);
    EXPECT_EQ(test.operation->result().checkpoint->metadata, original->metadata);
    EXPECT_EQ(test.source.ledger->stats().address_high_water, high_water);
}

TEST(LocalCheckpoint, GcRetiresTheObsoleteAnchorWithoutRewritingUnchangedSegments)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto                      reader      = test.operation->result().checkpoint;
    std::weak_ptr<const void> old_segment = reader->metadata->segments.front().second.owner;
    auto                      next        = test.materialize("next");
    ASSERT_TRUE(test.prepare(next).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U);
    ASSERT_TRUE(test.registry->needs_anchor_retirement());
    auto       latest = test.operation->result().checkpoint;
    const auto before = test.store->events.size();
    ASSERT_TRUE(test.prepare(next).ok());
    Completion unchanged;
    test.run(&unchanged);
    ASSERT_EQ(unchanged.values.load(), 1U);
    EXPECT_EQ(test.store->events.size(), before);
    EXPECT_TRUE(test.registry->needs_anchor_retirement());
    ASSERT_TRUE(test.prepare(next, {.retire_previous_anchor = true}).ok());
    Completion retired;
    test.run(&retired);
    ASSERT_EQ(retired.values.load(), 1U) << retired.status.to_string();
    EXPECT_EQ(test.store->events.size(), before + 3);
    EXPECT_EQ(test.operation->result().checkpoint->metadata, latest->metadata);
    EXPECT_EQ(test.registry->snapshot_sequence(), 3U);
    EXPECT_FALSE(test.registry->needs_anchor_retirement());
    EXPECT_FALSE(old_segment.expired());
    reader.reset();
    EXPECT_TRUE(old_segment.expired());
    test.expect_read("next");
}

TEST(LocalCheckpoint, ExcludedMetadataSegmentIsRelocatedDespiteUnchangedPageSlots)
{
    TestCheckpoint test;
    auto           pages = test.materialize();
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto        original = test.operation->result().checkpoint;
    const auto &segment  = original->metadata->segments.front().second;
    auto        fence    = await_ledger([&](auto done) {
        return test.source.ledger->exclude_allocations(segment.address, segment.length,
                                                       test.source.credits.credits.reservation(), std::move(done));
    });
    ASSERT_TRUE(fence.status.ok());
    const BackingExclusion exclusion{.address = segment.address, .length = segment.length};
    const auto             before = test.store->events.size();
    ASSERT_TRUE(test.prepare(pages, {.exclusion = exclusion}).ok());
    Completion relocated;
    test.run(&relocated);
    ASSERT_EQ(relocated.values.load(), 1U) << relocated.status.to_string();
    EXPECT_EQ(test.store->events.size(), before + 5);
    const auto &current = test.operation->result().checkpoint->metadata->segments.front();
    EXPECT_NE(current.first.image_addr, segment.address);
    EXPECT_FALSE(exclusion.overlaps(current.second.address, current.second.length));
    EXPECT_EQ(test.operation->result().checkpoint->pages, pages);
    EXPECT_EQ(test.registry->snapshot_sequence(), 2U);
    test.expect_read("v");
}

TEST(LocalCheckpoint, RotationRetainsBothAnchorsAndReaderOwners)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto                      reader       = test.operation->result().checkpoint;
    std::weak_ptr<const void> first_extent = reader->metadata->extents[0].owner;
    ASSERT_TRUE(test.prepare(test.materialize("second")).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U);
    EXPECT_FALSE(first_extent.expired());
    ASSERT_TRUE(test.prepare(test.materialize("third")).ok());
    Completion third;
    test.run(&third);
    ASSERT_EQ(third.values.load(), 1U);
    EXPECT_FALSE(first_extent.expired());
    reader.reset();
    EXPECT_TRUE(first_extent.expired());
    EXPECT_EQ(test.registry->snapshot_sequence(), 3U);
    std::vector<snapshot::CommitAnchor> anchors;
    ASSERT_TRUE(snapshot::read_valid_anchors(*test.store->device, 4096, &anchors).ok());
    ASSERT_EQ(anchors.size(), 2U);
    EXPECT_EQ(anchors[0].snapshot_seq, 2U);
    EXPECT_EQ(anchors[1].snapshot_seq, 3U);
    test.expect_read("third");
}

TEST(LocalCheckpoint, PreAnchorFailureKeepsPreviousSnapshotAndPermitsRetry)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto pages = test.materialize("next");
    ASSERT_TRUE(test.prepare(pages).ok());
    test.store->fail_at = test.store->events.size() + 3;
    Completion failed;
    test.run(&failed);
    EXPECT_EQ(failed.errors.load(), 1U);
    EXPECT_EQ(test.registry->snapshot_sequence(), 1U);
    EXPECT_FALSE(test.registry->needs_recovery());
    test.expect_read("v");
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion retry;
    test.run(&retry);
    EXPECT_EQ(retry.values.load(), 1U) << retry.status.to_string();
    test.expect_read("next");
}

TEST(LocalCheckpoint, FinalBarrierFailureProtectsCandidateAndRequiresRecovery)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    test.store->fail_at = 5;
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_TRUE(test.registry->needs_recovery());
    EXPECT_EQ(test.registry->durable_coverage(), 0U);
    EXPECT_EQ(test.registry->snapshot_sequence(), 0U);
    test.operation.reset();
    test.source.operation.reset();
    EXPECT_EQ(test.source.ledger->stats().protected_ranges, 3U);
    ASSERT_TRUE(test.prepare().ok());
    Completion rejected;
    test.run(&rejected);
    EXPECT_EQ(rejected.errors.load(), 1U);
    EXPECT_EQ(test.store->events.size(), 5U);
}

TEST(LocalCheckpoint, CoverageMayResetOnlyWhenTheLogicalGenerationAdvances)
{
    TestCheckpoint test;
    auto           first_pages = test.materialize();
    ASSERT_TRUE(test.prepare(first_pages).ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto empty        = detail::reserved_shared<CapturedDurablePages>(test.source.credits.credits.reservation(),
                                                                      test.source.credits.credits.reservation());
    empty->generation = first_pages->generation;
    ASSERT_TRUE(test.prepare(empty).ok());
    const auto before = test.store->events.size();
    Completion same_generation;
    test.run(&same_generation);
    EXPECT_EQ(same_generation.status.code(), Code::kUnavailable);
    EXPECT_EQ(test.store->events.size(), before);
    ++empty->generation;
    ASSERT_TRUE(test.prepare(empty).ok());
    Completion next_generation;
    test.run(&next_generation);
    ASSERT_EQ(next_generation.values.load(), 1U) << next_generation.status.to_string();
    EXPECT_EQ(test.registry->snapshot_sequence(), 2U);
    EXPECT_EQ(test.registry->durable_coverage(), 0U);
    ASSERT_TRUE(test.prepare(first_pages).ok());
    Completion old_generation;
    test.run(&old_generation);
    EXPECT_EQ(old_generation.status.code(), Code::kUnavailable);
    EXPECT_EQ(test.registry->snapshot_sequence(), 2U);
    EXPECT_EQ(test.registry->durable_coverage(), 0U);
}

TEST(LocalCheckpoint, EmptyCapturedViewCommitsAnEmptyDirectory)
{
    TestCheckpoint test;
    auto           pages = std::make_shared<CapturedDurablePages>(test.source.credits.credits.reservation());
    pages->coverage      = 12;
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(test.registry->durable_coverage(), 12U);
    EXPECT_EQ(test.operation->result().checkpoint->anchor.segdir_len, 20U);
    EXPECT_EQ(test.source.ledger->stats().protected_ranges, 1U);
}
} // namespace crowdb::tree
