// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_recovery_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

namespace
{
std::vector<uint8_t> read_frame(TestRecovery &test, uint64_t pid)
{
    const auto &[address, length] = test.locations.at(pid);
    std::vector<uint8_t> blob(length);
    EXPECT_TRUE(test.store->device->read_at(address, blob.data(), blob.size()).ok());
    std::vector<uint8_t> raw(durable_blob_raw_len(blob.data(), blob.size()));
    EXPECT_TRUE(decode_durable_page(blob.data(), blob.size(), raw.data(), raw.size()).ok());
    return raw;
}

void write_frame(TestRecovery &test, uint64_t pid, std::vector<uint8_t> raw, bool restamp = true)
{
    if (restamp) {
        frame_restamp_crc(raw.data(), raw.size());
    }
    std::vector<uint8_t> blob;
    encode_durable_page(raw.data(), raw.size(), compress_algo::kLz4, &blob);
    const auto &[address, length] = test.locations.at(pid);
    ASSERT_LE(blob.size(), length);
    blob.resize(length);
    ASSERT_TRUE(test.store->device->write_at(address, blob.data(), blob.size()).ok());
}

size_t first_cell(std::span<const uint8_t> raw)
{
    const LeafFrameView leaf(raw.data(), raw.size());
    return static_cast<size_t>(leaf.cell(0).bytes() - raw.data());
}

void expect_failure(TestRecovery &test, Code code = Code::kCorruption, LocalRecoveryLimits limits = {})
{
    ASSERT_TRUE(test.prepare(limits).ok());
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), code);
    EXPECT_FALSE(test.operation->result().latest);
    EXPECT_FALSE(test.operation->result().registry);
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace

TEST(LocalRecoveryFrames, RejectsBlobCrcFrameCrcSelfPidAndPageTypeCorruption)
{
    for (size_t mode = 0; mode < 4; ++mode) {
        TestRecovery test;
        test.checkpoint();
        if (mode == 0) {
            const auto &[address, length] = test.locations.at(0);
            std::vector<uint8_t> blob(length);
            ASSERT_TRUE(test.store->device->read_at(address, blob.data(), blob.size()).ok());
            blob[kDurableBlobHeader] ^= 1;
            ASSERT_TRUE(test.store->device->write_at(address, blob.data(), blob.size()).ok());
        }
        else {
            auto raw = read_frame(test, 0);
            if (mode == 3) {
                raw[fh::kType] = 0xFF;
            }
            else {
                frame_put_u64(raw.data(), fh::kSelfpage_id, 9);
            }
            write_frame(test, 0, std::move(raw), mode != 1);
        }
        expect_failure(test);
    }
}

TEST(LocalRecoveryFrames, RejectsInvalidLeafFlagsSlotsAndOverflowCells)
{
    for (size_t mode = 0; mode < 6; ++mode) {
        TestRecovery test;
        test.checkpoint(std::string(2000, 'v'));
        auto  raw  = read_frame(test, 0);
        auto *cell = raw.data() + first_cell(raw);
        if (mode == 0) {
            frame_put_u64(cell, 0, test.anchor.last_applied_slot + 1);
        }
        else if (mode == 1) {
            cell[8] = 0x80;
        }
        else if (mode == 2) {
            cell[8] = kFlagTombstone;
        }
        else if (mode == 3) {
            cell[8] = kFlagTombstone | kFlagOverflow;
        }
        else if (mode == 4) {
            frame_put_u64(cell, kCellHeaderSize, kInvalidPageId);
        }
        else {
            frame_put_u64(cell, kCellHeaderSize + sizeof(uint64_t), 0);
        }
        write_frame(test, 0, std::move(raw));
        expect_failure(test);
    }
}

TEST(LocalRecoveryFrames, RejectsMissingShortLongCyclicAndEmptyOverflowChains)
{
    for (size_t mode = 0; mode < 5; ++mode) {
        TestRecovery test;
        test.checkpoint(std::string(8000, 'v'));
        auto           raw = read_frame(test, 0);
        const CellView cell(LeafFrameView(raw.data(), raw.size()).cell(0));
        const uint64_t head = cell.overflow_head();
        if (mode == 0 || mode == 1) {
            auto *encoded = raw.data() + first_cell(raw);
            if (mode == 0) {
                frame_put_u64(encoded, kCellHeaderSize, test.anchor.next_page_id);
            }
            else {
                frame_put_u64(encoded, kCellHeaderSize + sizeof(uint64_t), cell.overflow_len() - 1);
            }
            write_frame(test, 0, std::move(raw));
        }
        else {
            raw = read_frame(test, head);
            if (mode == 2) {
                frame_put_u64(raw.data(), fh::kRightSibling, kInvalidPageId);
            }
            else if (mode == 3) {
                frame_put_u64(raw.data(), fh::kRightSibling, head);
            }
            else {
                frame_put_u32(raw.data(), fh::kSlotCount, 0);
            }
            write_frame(test, head, std::move(raw));
        }
        expect_failure(test);
    }
}

TEST(LocalRecoveryFrames, DependencyStepsAndReferenceCountAreBounded)
{
    for (const bool limit_references : {false, true}) {
        TestRecovery test;
        test.checkpoint(std::string(8000, 'v'));
        LocalRecoveryLimits limits;
        if (limit_references) {
            const auto           raw  = read_frame(test, 0);
            const auto           cell = LeafFrameView(raw.data(), raw.size()).cell(0);
            std::vector<uint8_t> rewritten(raw.size());
            LeafFrameBuilder     builder(rewritten.data(), rewritten.size());
            ASSERT_TRUE(builder.try_append_sorted("a", cell));
            ASSERT_TRUE(builder.try_append_sorted("b", cell));
            builder.finish(0, kInvalidPageId);
            write_frame(test, 0, std::move(rewritten));
            limits.max_overflow_references = 1;
        }
        else {
            limits.max_dependency_steps = 1;
        }
        expect_failure(test, Code::kResourceExhausted, limits);
    }
}

TEST(LocalRecoveryFrames, LeafBackingAloneKeepsItsPrimaryAndActualOverflowChainAfterRegistryEnds)
{
    TestRecovery test;
    test.checkpoint(std::string(6000, 'o'));
    test.checkpoint(std::string(12000, 'v'));
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    auto ledger = test.operation->result().ledger;
    auto pages  = test.operation->result().latest;
    ASSERT_TRUE(pages);
    const auto                             count = pages->frames.size();
    auto                                   leaf  = pages->frames.at(0).backing;
    std::vector<std::weak_ptr<const void>> overflow;
    for (const auto &[pid, frame] : pages->frames) {
        if (pid != 0) {
            overflow.push_back(frame.backing->owner);
        }
    }
    pages.reset();
    test.operation.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, count);
    for (const auto &owner : overflow) {
        EXPECT_FALSE(owner.expired());
    }
    leaf.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 0U);
    for (const auto &owner : overflow) {
        EXPECT_TRUE(owner.expired());
    }
    overflow.clear();
    ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(LocalRecoveryFrames, RecoveredOverflowCanBeReusedAndDoesNotRetainItsOldPrimary)
{
    TestRecovery test;
    test.checkpoint(std::string(12000, 'r'));
    const auto raw = read_frame(test, 0);
    ASSERT_TRUE(test.prepare().ok());
    Completion recovered;
    test.run(&recovered);
    ASSERT_EQ(recovered.values.load(), 1U) << recovered.status.to_string();
    auto                      inherited   = test.operation->result().latest;
    auto                      ledger      = test.operation->result().ledger;
    std::weak_ptr<const void> old_primary = inherited->frames.at(0).backing->owner;
    auto                      leaf        = std::make_shared<PageState>();
    leaf->bases.push_back(
        {.identity = 1,
         .max_slot = inherited->coverage,
         .resident = std::shared_ptr<const LeafBase>(LeafBase::from_frame_copy(raw.data(), raw.size())),
         .backing  = inherited->frames.at(0).backing});
    auto captured          = captured_leaf(leaf);
    captured->next_page_id = inherited->next_page_id;
    std::shared_ptr<CapturedWriteOperation> writer;
    ASSERT_TRUE(
        CapturedWriteOperation::create(
            captured, {}, test.store, test.original.source.runtime, test.original.source.lane,
            {.frames = {.encode = {.frame_bytes = 4096, .inline_value_bytes = 16}}, .write = {.max_frame_bytes = 4096}},
            test.credits.credits, [](uint64_t, ReservedOverflowIds *) { return Status::not_supported(); },
            extent_write_reservation(ledger, test.credits.credits.reservation()), inherited, &writer)
            .ok());
    Completion written;
    auto       connected = stdexec::connect(writer->sender(), Receiver{.done = &written});
    stdexec::start(connected);
    test.original.source.runtime->drain();
    ASSERT_EQ(written.values.load(), 1U) << written.status.to_string();
    auto pages = writer->result().pages;
    EXPECT_EQ(writer->result().written_frames, 1U);
    EXPECT_EQ(writer->result().reused_frames, inherited->frames.size() - 1);
    for (const auto &[pid, frame] : inherited->frames) {
        if (pid != 0) {
            const auto &adopted = pages->frames.at(pid).backing;
            EXPECT_NE(adopted, frame.backing);
            EXPECT_EQ(adopted->physical_owner, frame.backing->physical_owner);
            EXPECT_EQ(adopted->address, frame.backing->address);
            EXPECT_EQ(adopted->length, frame.backing->length);
        }
    }
    writer.reset();
    inherited.reset();
    captured.reset();
    leaf.reset();
    test.operation.reset();
    EXPECT_TRUE(old_primary.expired());
    EXPECT_EQ(ledger->stats().protected_ranges, pages->frames.size());
    std::string    restored;
    const CellView cell(LeafFrameView(raw.data(), raw.size()).cell(0));
    for (auto pid = cell.overflow_head(); pid != kInvalidPageId;) {
        const auto              bytes = read_frame(test, pid);
        const OverflowFrameView chunk(bytes.data(), bytes.size());
        restored += chunk.payload().to_string();
        pid = chunk.next_page_id();
    }
    EXPECT_EQ(restored, std::string(12000, 'r'));
    pages.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 0U);
}
} // namespace crowdb::tree
