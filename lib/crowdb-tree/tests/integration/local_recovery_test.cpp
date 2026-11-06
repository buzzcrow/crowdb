// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_persist.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "local_recovery_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

namespace
{
Status recover(TestRecovery &test, LocalRecoveryLimits limits = {})
{
    auto status = test.prepare(limits);
    if (!status.ok()) {
        return status;
    }
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.values.load() + done.errors.load(), 1U);
    return test.operation->result().status;
}

std::vector<DirEntry> read_directory(TestRecovery &test)
{
    std::vector<uint8_t> bytes(round_up_to_iu(test.anchor.segdir_len, 4096));
    EXPECT_TRUE(test.store->device->read_at(test.anchor.segdir_addr, bytes.data(), bytes.size()).ok());
    std::vector<DirEntry> entries;
    EXPECT_TRUE(decode_segment_directory(bytes.data(), bytes.size(), &entries).ok());
    return entries;
}

void write_directory(TestRecovery &test, const std::vector<DirEntry> &entries)
{
    std::vector<uint8_t> bytes;
    encode_segment_directory(entries, &bytes);
    test.anchor.segdir_len = static_cast<uint32_t>(bytes.size());
    test.anchor.segdir_crc = common::crc32c(bytes.data(), bytes.size());
    bytes.resize(round_up_to_iu(bytes.size(), 4096));
    ASSERT_TRUE(test.store->device->write_at(test.anchor.segdir_addr, bytes.data(), bytes.size()).ok());
    bytes.clear();
    snapshot::encode_anchor(test.anchor, 4096, &bytes);
    ASSERT_TRUE(
        test.store->device->write_at(test.anchor.snapshot_seq % 2 == 0 ? 4096 : 0, bytes.data(), bytes.size()).ok());
}

std::vector<uint64_t> read_segment(TestRecovery &test, const DirEntry &entry, SegmentImageHeader *header)
{
    std::vector<uint8_t> bytes(round_up_to_iu(entry.image_len, 4096));
    EXPECT_TRUE(test.store->device->read_at(entry.image_addr, bytes.data(), bytes.size()).ok());
    std::vector<uint64_t> words;
    EXPECT_TRUE(decode_segment_image(bytes.data(), bytes.size(), header, &words).ok());
    return words;
}

void write_segment(TestRecovery &test, DirEntry &entry, const SegmentImageHeader &header,
                   const std::vector<uint64_t> &words)
{
    std::vector<uint8_t> bytes;
    encode_segment_image(header, words, &bytes, &entry.image_crc);
    entry.image_len = static_cast<uint32_t>(bytes.size());
    bytes.resize(round_up_to_iu(bytes.size(), 4096));
    ASSERT_TRUE(test.store->device->write_at(entry.image_addr, bytes.data(), bytes.size()).ok());
}

void expect_corruption(TestRecovery &test)
{
    EXPECT_EQ(recover(test).code(), Code::kCorruption);
    ASSERT_TRUE(test.operation);
    EXPECT_FALSE(test.operation->result().latest);
    EXPECT_FALSE(test.operation->result().registry);
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace

TEST(LocalRecovery, FreshDomainActivatesAnEmptyRegistryBeforeItsFirstAllocation)
{
    TestRecovery test;
    ASSERT_TRUE(recover(test).ok());
    const auto &result = test.operation->result();
    ASSERT_TRUE(result.ledger);
    ASSERT_TRUE(result.registry);
    EXPECT_FALSE(result.latest);
    EXPECT_FALSE(result.anchors[0]);
    EXPECT_FALSE(result.anchors[1]);
    EXPECT_EQ(result.registry->snapshot_sequence(), 0U);
    EXPECT_EQ(result.ledger->stats().protected_ranges, 0U);
    auto allocated = await_ledger(
        [&](auto done) { return result.ledger->reserve(4096, test.credits.credits.reservation(), std::move(done)); });
    ASSERT_TRUE(allocated.status.ok());
    EXPECT_EQ(allocated.extent.address, 8192U);
    EXPECT_EQ(allocated.extent.generation, 2U);
    EXPECT_EQ(result.ledger->stats().appended_allocations, 1U);
}

TEST(LocalRecovery, ChoosesTheNewestAnchorAcrossParityRotationAndReleasesUnrelatedOwners)
{
    TestRecovery test;
    test.checkpoint("first");
    test.checkpoint("second");
    test.checkpoint("third");
    ASSERT_TRUE(recover(test).ok());
    auto        ledger = test.operation->result().ledger;
    const auto &result = test.operation->result();
    ASSERT_TRUE(result.anchors[0]);
    ASSERT_TRUE(result.anchors[1]);
    EXPECT_EQ(result.anchors[0]->anchor.snapshot_seq, 3U);
    EXPECT_EQ(result.anchors[1]->anchor.snapshot_seq, 2U);
    EXPECT_EQ(result.latest, result.anchors[0]->pages);
    EXPECT_EQ(result.registry->snapshot_sequence(), 3U);
    EXPECT_EQ(ledger->stats().protected_ranges, 6U);
    auto leaf = result.latest->frames.at(0).backing;
    EXPECT_EQ(leaf->generation, 2U);
    test.original.expect_read("third");
    test.operation.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 1U);
    leaf.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 0U);
}

TEST(LocalRecovery, NewestSequenceWinsAfterAChangedGenerationResetsCoverage)
{
    TestRecovery test;
    test.checkpoint("old generation");
    auto empty = detail::reserved_shared<CapturedDurablePages>(test.original.source.credits.credits.reservation(),
                                                               test.original.source.credits.credits.reservation());
    empty->generation = test.original.operation->result().checkpoint->pages->generation + 1;
    ASSERT_TRUE(test.original.prepare(empty).ok());
    Completion cleared;
    test.original.run(&cleared);
    ASSERT_EQ(cleared.values.load(), 1U) << cleared.status.to_string();
    empty.reset();
    const auto status = recover(test);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto &result = test.operation->result();
    ASSERT_TRUE(result.anchors[0]);
    ASSERT_TRUE(result.anchors[1]);
    EXPECT_EQ(result.anchors[0]->anchor.last_applied_slot, 10U);
    EXPECT_EQ(result.anchors[1]->anchor.last_applied_slot, 0U);
    EXPECT_EQ(result.latest, result.anchors[1]->pages);
    EXPECT_EQ(result.registry->snapshot_sequence(), 2U);
    EXPECT_EQ(result.registry->durable_coverage(), 0U);
    EXPECT_EQ(result.latest->root_page_id, kInvalidPageId);
    EXPECT_TRUE(result.latest->frames.empty());
}

TEST(LocalRecovery, RecoveredRegistryCanCommitTheNextCheckpointAndAdvanceDurableCoverage)
{
    TestRecovery test;
    test.checkpoint("reopened");
    ASSERT_TRUE(recover(test).ok());
    const auto &result = test.operation->result();
    auto pages = detail::reserved_shared<CapturedDurablePages>(test.credits.credits.reservation(), *result.latest);
    ++pages->coverage;
    ++pages->generation;
    std::shared_ptr<LocalCheckpointOperation> checkpoint;
    ASSERT_TRUE(LocalCheckpointOperation::create(pages, result.registry, test.store, test.original.source.runtime,
                                                 test.original.source.lane, {}, test.credits.credits, &checkpoint)
                    .ok());
    Completion done;
    auto       connected = stdexec::connect(checkpoint->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.original.source.runtime->drain();
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(result.registry->snapshot_sequence(), 2U);
    EXPECT_EQ(result.registry->durable_coverage(), 11U);
    EXPECT_EQ(checkpoint->result().checkpoint->anchor.last_applied_slot, 11U);
    EXPECT_EQ(checkpoint->result().checkpoint->pages->frames.at(0).backing, result.latest->frames.at(0).backing);
    test.original.expect_read("reopened");
}

TEST(LocalRecovery, SegmentGenerationIsIndependentOfTheAnchorSequence)
{
    for (const uint64_t generation : {uint64_t{0}, uint64_t{77}}) {
        TestRecovery test;
        test.checkpoint();
        auto entries = read_directory(test);
        ASSERT_EQ(entries.size(), 1U);
        SegmentImageHeader header;
        const auto         words = read_segment(test, entries[0], &header);
        header.generation        = generation;
        entries[0].generation    = generation;
        write_segment(test, entries[0], header, words);
        write_directory(test, entries);
        ASSERT_TRUE(recover(test).ok());
        EXPECT_EQ(test.operation->result().registry->snapshot_sequence(), 1U);
        test.original.expect_read("v");
    }
}

TEST(LocalRecovery, ValidAnchorWithCorruptDirectoryOrSegmentNeverFallsBackSilently)
{
    for (const bool older : {false, true}) {
        for (const bool segment : {false, true}) {
            TestRecovery test;
            test.checkpoint("first");
            const auto first = test.anchor;
            test.checkpoint("second");
            if (older) {
                test.anchor = first;
            }
            const auto entries = read_directory(test);
            ASSERT_EQ(entries.size(), 1U);
            const auto                address = segment ? entries[0].image_addr : test.anchor.segdir_addr;
            std::array<uint8_t, 4096> bytes{};
            ASSERT_TRUE(test.store->device->read_at(address, bytes.data(), bytes.size()).ok());
            bytes[0] ^= 1;
            ASSERT_TRUE(test.store->device->write_at(address, bytes.data(), bytes.size()).ok());
            expect_corruption(test);
        }
    }
}

TEST(LocalRecovery, ChecksSegmentIdentityGenerationBodyCrcAndLiveCountAgainstDirectory)
{
    for (size_t mode = 0; mode < 4; ++mode) {
        TestRecovery test;
        test.checkpoint();
        auto               entries = read_directory(test);
        SegmentImageHeader header;
        const auto         words = read_segment(test, entries.at(0), &header);
        if (mode == 0) {
            ++header.seg_idx;
        }
        else if (mode == 1) {
            ++header.generation;
        }
        else if (mode == 3) {
            ++header.live_count;
        }
        write_segment(test, entries[0], header, words);
        if (mode == 2) {
            entries[0].image_crc ^= 1;
        }
        write_directory(test, entries);
        expect_corruption(test);
    }
}

TEST(LocalRecovery, RejectsMappingExtentsThatOverlapProtectedMetadata)
{
    TestRecovery test;
    test.checkpoint();
    auto               entries = read_directory(test);
    SegmentImageHeader header;
    auto               words = read_segment(test, entries.at(0), &header);
    words[0]                 = slot_word::pack_unloaded((entries[0].image_addr + 4096) / 4096, 1);
    write_segment(test, entries[0], header, words);
    write_directory(test, entries);
    expect_corruption(test);
}

TEST(LocalRecovery, DeclaredSegmentAndFrameLimitsCannotSelectAnOlderSnapshot)
{
    for (const bool segments : {false, true}) {
        TestRecovery test;
        test.checkpoint();
        test.checkpoint(std::string(8000, 'v'));
        LocalRecoveryLimits limits;
        if (segments) {
            auto     entries = read_directory(test);
            DirEntry extra{.seg_idx = 1, .generation = 1, .image_addr = test.store->device->size()};
            write_segment(test, extra, {.seg_idx = 1, .generation = 1, .slot_count = 1024},
                          std::vector<uint64_t>(1024));
            entries.push_back(extra);
            write_directory(test, entries);
            limits.max_segments = 1;
        }
        else {
            limits.max_frames = 1;
        }
        EXPECT_FALSE(recover(test, limits).ok());
        EXPECT_FALSE(test.operation->result().latest);
        EXPECT_FALSE(test.operation->result().registry);
    }
}

TEST(LocalRecovery, UnreferencedCrashTailBecomesReusableAfterAllAnchorsAreProtected)
{
    TestRecovery test;
    test.checkpoint();
    const auto                      orphan = test.store->device->size();
    const std::array<uint8_t, 4096> bytes{};
    ASSERT_TRUE(test.store->device->write_at(orphan, bytes.data(), bytes.size()).ok());
    ASSERT_TRUE(recover(test).ok());
    auto ledger = test.operation->result().ledger;
    auto reused = await_ledger(
        [&](auto done) { return ledger->reserve(4096, test.credits.credits.reservation(), std::move(done)); });
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, orphan);
    EXPECT_EQ(ledger->stats().reused_allocations, 1U);
    EXPECT_EQ(ledger->stats().appended_allocations, 0U);
    test.original.expect_read("v");
}
} // namespace crowdb::tree
