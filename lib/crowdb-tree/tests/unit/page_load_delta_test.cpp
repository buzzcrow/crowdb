// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/btree/leaf_cursor.h"
#include "load_budget_test_support.h"
#include "page_load_delta_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(PageLoadDelta, NormalizesUsingFoldWinnersAndKeepsAllTombstones)
{
    TestDeltaLeaf                   fixture(true);
    auto                            loader = fixture.loader();
    std::shared_ptr<const LeafBase> result;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(leaf);
    });
    fixture.runtime->drain();
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->view().delta_count(), 0U);
    EXPECT_EQ(result->view().self_page_id(), 17U);
    EXPECT_EQ(result->right_sibling(), 23U);
    EXPECT_LE(result->page_bytes(), fixture.frame.size());
    EXPECT_TRUE(frame_validate(result->frame(), result->page_bytes()));
    const auto raw = std::unique_ptr<LeafBase>(LeafBase::from_frame_copy(fixture.frame.data(), fixture.frame.size()));
    LeafChainCursor expected(raw.get(), 0);
    size_t          index = 0;
    while (expected.valid()) {
        ASSERT_LT(index, result->count());
        EXPECT_EQ(result->key(index), expected.key());
        EXPECT_EQ(result->cell(index), expected.cell());
        ++index;
        expected.next();
    }
    ASSERT_EQ(result->count(), index + 1);
    EXPECT_EQ(result->key(index), Slice("zero"));
    EXPECT_TRUE(CellView(result->cell(index)).is_tombstone());
    EXPECT_EQ(CellView(result->cell(index)).slot(), 0U);
    EXPECT_EQ(CellView(result->cell(result->find("a"))).value(), Slice("delta"));
    EXPECT_EQ(CellView(result->cell(result->find("b"))).value(), Slice("base-newer"));
    EXPECT_EQ(CellView(result->cell(result->find("new"))).value(), Slice("first"));
    EXPECT_EQ(CellView(result->cell(result->find("tie"))).value(), Slice("base-tie"));
}

TEST(PageLoadDelta, SlotBoundValidatesMainAndDeltaInsteadOfFilteringHistory)
{
    for (const auto bound : {0U, 5U, 6U}) {
        TestDeltaLeaf fixture;
        fixture.reference.max_slot = bound;
        auto   loader              = fixture.loader();
        size_t calls               = 0;
        loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_FALSE(leaf);
            ++calls;
        });
        fixture.runtime->drain();
        EXPECT_EQ(calls, 1U);
    }
    TestDeltaLeaf fixture;
    auto *raw = const_cast<uint8_t *>(LeafFrameView(fixture.frame.data(), fixture.frame.size()).delta_cell(1).bytes());
    frame_put_u64(raw, 0, 8);
    frame_restamp_crc(fixture.frame.data(), fixture.frame.size());
    fixture.encode();
    auto loader = fixture.loader();
    loader->load_leaf(fixture.reference, [](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(leaf);
    });
    fixture.runtime->drain();
}

TEST(PageLoadDelta, RejectsMalformedCellsEvenWhenTheyLoseTheFold)
{
    for (bool delta : {false, true}) {
        TestDeltaLeaf       fixture;
        const LeafFrameView view(fixture.frame.data(), fixture.frame.size());
        const auto          cell = delta ? view.delta_cell(4) : view.cell(0);
        fixture.frame[static_cast<size_t>(cell.bytes() - fixture.frame.data()) + 8] = 0x80;
        frame_restamp_crc(fixture.frame.data(), fixture.frame.size());
        fixture.encode();
        auto   loader = fixture.loader();
        size_t calls  = 0;
        loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_FALSE(leaf);
            ++calls;
        });
        fixture.runtime->drain();
        EXPECT_EQ(calls, 1U);
    }
}

TEST(PageLoadDelta, EntryAndKeyWindowsCountTheEntireEncodedFrame)
{
    for (bool entries : {false, true}) {
        TestDeltaLeaf  fixture;
        PageLoadLimits limits;
        if (entries) {
            limits.max_leaf_entries = 11;
        }
        else {
            limits.max_leaf_key_bytes = 16;
        }
        auto   loader = fixture.loader(limits);
        size_t calls  = 0;
        loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kResourceExhausted);
            EXPECT_FALSE(leaf);
            ++calls;
        });
        fixture.runtime->drain();
        EXPECT_EQ(calls, 1U);
    }
}

TEST(PageLoadDelta, LeafRetainsOnlyItsBackingProtectionAndActualFrameCharge)
{
    TestDeltaLeaf                   fixture(true);
    LoadCredits                     credits;
    auto                            loader     = fixture.loader({}, credits.credits);
    std::weak_ptr<const void>       protection = fixture.reference.backing->owner;
    std::weak_ptr<TestDeltaStore>   store      = fixture.store;
    std::shared_ptr<const LeafBase> result;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(leaf);
    });
    fixture.runtime->drain();
    ASSERT_NE(result, nullptr);
    loader.reset();
    fixture.reference = {};
    fixture.store.reset();
    credits.seal();
    EXPECT_TRUE(store.expired());
    EXPECT_FALSE(protection.expired());
    EXPECT_EQ(credits.budget.snapshot().reserved[LoadCredits::kBytes], 0U);
    EXPECT_GE(credits.budget.snapshot().allocated[LoadCredits::kBytes], result->page_bytes() + sizeof(LeafBase));
    const CellView overflow(result->cell(result->find("overflow")));
    EXPECT_EQ(overflow.overflow_head(), 41U);
    EXPECT_EQ(overflow.overflow_len(), 16384U);
    result.reset();
    EXPECT_TRUE(protection.expired());
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageLoadDelta, IndexAllocationExhaustionReleasesDecodeAndIoCredit)
{
    TestDeltaLeaf fixture;
    fixture.store->deferred = true;
    LoadCredits credits;
    auto        loader = fixture.loader({}, credits.credits);
    size_t      calls  = 0;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(leaf);
        ++calls;
    });
    const auto delta_bytes = LeafFrameView(fixture.frame.data(), fixture.frame.size()).delta_count() * sizeof(uint32_t);
    const auto held        = credits.credits.reservation()->available() - fixture.frame.size() - delta_bytes + 1;
    ASSERT_TRUE(credits.credits.reservation()->claim(held));
    fixture.store->pending.complete(Status::Ok());
    fixture.store->pending = {};
    fixture.runtime->drain();
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    loader.reset();
    credits.credits.reservation()->release(held);
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace crowdb::tree
